// Copyright 2026 The gVisor Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgalloc

import (
	"bytes"
	"hash/fnv"

	"gvisor.dev/gvisor/pkg/hostarch"
	"gvisor.dev/gvisor/pkg/sentry/checkpoint"
	"gvisor.dev/gvisor/pkg/sentry/memmap"
	"gvisor.dev/gvisor/pkg/sync"
)

// Pre-copy live migration (see kernel.Kernel.Precopy) sends MemoryFile pages
// to the destination while the sandbox runs, possibly several times, and only
// sends MemoryFile metadata and the last dirty pages while it is stopped.
// Pages are sent by MemoryFile offset rather than in pages file order, so the
// destination stores them in a pages file in which every MemoryFile occupies
// its own region and each page is stored at its MemoryFile offset. Restoring
// from such a pages file requires LoadOpts.PagesFileAtFileOffsets.

// PrecopyPagesFileOffset returns the offset at which the page at MemoryFile
// offset 0 of the MemoryFile with the given resource ID (the zero ResourceID
// for the main MemoryFile) is stored in a pre-copy pages file.
//
// Each MemoryFile gets a 16 TiB region; regions for private MemoryFiles are
// chosen by hashing their resource ID, since the set of private MemoryFiles
// can change while pre-copy is in progress.
func PrecopyPagesFileOffset(id checkpoint.ResourceID) uint64 {
	const (
		regionShift = 44
		regions     = 1 << (63 - regionShift) // keep offsets below 1<<63
	)
	if !id.Ok() {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(id.ContainerName))
	h.Write([]byte{0})
	h.Write([]byte(id.Path))
	return (1 + h.Sum64()%(regions-1)) << regionShift
}

// PrecopySink receives MemoryFile contents during pre-copy. Offsets are
// MemoryFile offsets.
type PrecopySink interface {
	// WritePages sends the contents of pages starting at off. The slices
	// collectively span a page-aligned length, and are only valid for the
	// duration of the call.
	WritePages(off uint64, data [][]byte) error

	// WriteZero sends that the given page-aligned range contains only zeroes.
	WriteZero(off, length uint64) error
}

// PrecopySinks provides the PrecopySink for each MemoryFile.
type PrecopySinks interface {
	// MemoryFileSink returns the sink for pages of the MemoryFile with the
	// given resource ID (the zero ResourceID for the main MemoryFile).
	MemoryFileSink(id checkpoint.ResourceID) PrecopySink
}

// PrecopyStats counts bytes sent by MemoryFile.SendRanges.
type PrecopyStats struct {
	// DataBytes is the number of bytes of page contents sent.
	DataBytes uint64

	// ZeroBytes is the number of bytes sent as zero ranges.
	ZeroBytes uint64
}

// Add adds o to s.
func (s *PrecopyStats) Add(o PrecopyStats) {
	s.DataBytes += o.DataBytes
	s.ZeroBytes += o.ZeroBytes
}

// AllocatedRanges returns the ranges of f that are not known to be uncommitted
// (used, waste and releasing pages), in increasing order.
func (f *MemoryFile) AllocatedRanges() []memmap.FileRange {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.memAcctRangesLocked(false /* committedOnly */)
}

// memAcctRangesLocked returns the ranges spanned by f.memAcct, or only its
// known-committed ranges if committedOnly is true.
//
// Preconditions: f.mu must be locked.
func (f *MemoryFile) memAcctRangesLocked(committedOnly bool) []memmap.FileRange {
	var frs []memmap.FileRange
	for seg := f.memAcct.FirstSegment(); seg.Ok(); seg = seg.NextSegment() {
		if committedOnly && !seg.ValuePtr().knownCommitted {
			continue
		}
		fr := seg.Range()
		if n := len(frs); n != 0 && frs[n-1].End == fr.Start {
			frs[n-1].End = fr.End
		} else {
			frs = append(frs, fr)
		}
	}
	return frs
}

// IntersectRanges returns the intersection of two sets of ranges, each in
// increasing order and non-overlapping, in increasing order.
func IntersectRanges(a, b []memmap.FileRange) []memmap.FileRange {
	var out []memmap.FileRange
	for len(a) != 0 && len(b) != 0 {
		if r := a[0].Intersect(b[0]); r.Length() != 0 {
			out = append(out, r)
		}
		if a[0].End < b[0].End {
			a = a[1:]
		} else {
			b = b[1:]
		}
	}
	return out
}

// SubtractRanges returns the ranges in a that are not in b, where a and b are
// each in increasing order and non-overlapping, in increasing order.
func SubtractRanges(a, b []memmap.FileRange) []memmap.FileRange {
	var out []memmap.FileRange
	for _, r := range a {
		for len(b) != 0 && b[0].End <= r.Start {
			b = b[1:]
		}
		for i := 0; i < len(b) && b[i].Start < r.End; i++ {
			if b[i].Start > r.Start {
				out = append(out, memmap.FileRange{Start: r.Start, End: b[i].Start})
			}
			r.Start = max(r.Start, b[i].End)
			if r.Start >= r.End {
				break
			}
		}
		if r.Start < r.End {
			out = append(out, r)
		}
	}
	return out
}

// UnionRanges returns the union of two sets of ranges, each in increasing order
// and non-overlapping, in increasing order.
func UnionRanges(a, b []memmap.FileRange) []memmap.FileRange {
	out := make([]memmap.FileRange, 0, len(a)+len(b))
	add := func(r memmap.FileRange) {
		if n := len(out); n != 0 && out[n-1].End >= r.Start {
			out[n-1].End = max(out[n-1].End, r.End)
			return
		}
		out = append(out, r)
	}
	for len(a) != 0 || len(b) != 0 {
		if len(b) == 0 || (len(a) != 0 && a[0].Start <= b[0].Start) {
			add(a[0])
			a = a[1:]
		} else {
			add(b[0])
			b = b[1:]
		}
	}
	return out
}

// precopyMaxWrite is the maximum number of bytes passed to one call to
// PrecopySink.WritePages.
const precopyMaxWrite = 1 << 20

// SendRanges sends the contents of the pages in frs to sink: non-zero pages as
// data, and zero pages and holes in the backing file as zero ranges. frs must
// be page-aligned and in increasing order, and must only contain pages that
// are not known to be uncommitted (see AllocatedRanges), since reading other
// pages could commit them.
//
// SendRanges may be called while f is in use. Pages that are written
// concurrently may be sent with any mix of old and new contents; dirty tracking
// is responsible for sending them again.
func (f *MemoryFile) SendRanges(frs []memmap.FileRange, sink PrecopySink) (PrecopyStats, error) {
	var stats PrecopyStats
	if len(frs) == 0 {
		return stats, nil
	}
	zeroPage := make([]byte, hostarch.PageSize)

	// Pending output: a run of zero pages or a run of data pages.
	var (
		pendZero      memmap.FileRange
		pendData      memmap.FileRange
		pendDataSlice [][]byte
	)
	flushZero := func() error {
		if pendZero.Length() == 0 {
			return nil
		}
		err := sink.WriteZero(pendZero.Start, pendZero.Length())
		stats.ZeroBytes += pendZero.Length()
		pendZero = memmap.FileRange{}
		return err
	}
	flushData := func() error {
		if pendData.Length() == 0 {
			return nil
		}
		err := sink.WritePages(pendData.Start, pendDataSlice)
		stats.DataBytes += pendData.Length()
		pendData = memmap.FileRange{}
		pendDataSlice = pendDataSlice[:0]
		return err
	}
	addZero := func(fr memmap.FileRange) error {
		if err := flushData(); err != nil {
			return err
		}
		if pendZero.Length() != 0 && pendZero.End == fr.Start {
			pendZero.End = fr.End
			return nil
		}
		if err := flushZero(); err != nil {
			return err
		}
		pendZero = fr
		return nil
	}
	addData := func(fr memmap.FileRange, bs []byte) error {
		if err := flushZero(); err != nil {
			return err
		}
		for len(bs) != 0 {
			if pendData.Length() != 0 && (pendData.End != fr.Start || pendData.Length() >= precopyMaxWrite) {
				if err := flushData(); err != nil {
					return err
				}
			}
			n := min(uint64(len(bs)), precopyMaxWrite-pendData.Length())
			if pendData.Length() == 0 {
				pendData = memmap.FileRange{Start: fr.Start, End: fr.Start}
			}
			pendData.End += n
			pendDataSlice = append(pendDataSlice, bs[:n])
			bs = bs[n:]
			fr.Start += n
		}
		return nil
	}
	// scanData sends pages in fr, finding runs of zero and non-zero pages.
	scanData := func(fr memmap.FileRange) error {
		var err error
		f.forEachChunk(fr, func(chunk *chunkInfo, chunkFR memmap.FileRange) bool {
			bs := chunk.sliceAt(chunkFR)
			runStart, runZero := uint64(0), false
			flushRun := func(end uint64) {
				if end == runStart {
					return
				}
				runFR := memmap.FileRange{Start: chunkFR.Start + runStart, End: chunkFR.Start + end}
				if runZero {
					err = addZero(runFR)
				} else {
					err = addData(runFR, bs[runStart:end])
				}
				runStart = end
			}
			for pgoff := uint64(0); pgoff < uint64(len(bs)) && err == nil; pgoff += hostarch.PageSize {
				isZero := bytes.Equal(bs[pgoff:pgoff+hostarch.PageSize], zeroPage)
				if pgoff == runStart {
					runZero = isZero
				} else if isZero != runZero {
					flushRun(pgoff)
					runZero = isZero
				}
			}
			if err == nil {
				flushRun(uint64(len(bs)))
			}
			return err == nil
		})
		return err
	}

	size := f.TotalSize()
	dataSeeker := f.newHostFileDataSeeker()
	for _, fr := range frs {
		if fr.End > size {
			// Can't happen for pages from AllocatedRanges(), since the file
			// never shrinks.
			fr.End = size
		}
		if dataSeeker == nil {
			if err := scanData(fr); err != nil {
				return stats, err
			}
			continue
		}
		for fr.Start < fr.End {
			data, err := dataSeeker.dataAtOrAfter(fr.Start)
			if err != nil {
				return stats, err
			}
			if holeEnd := min(max(data.Start, fr.Start), fr.End); holeEnd > fr.Start {
				if err := addZero(memmap.FileRange{Start: fr.Start, End: holeEnd}); err != nil {
					return stats, err
				}
				fr.Start = holeEnd
			}
			if dataEnd := min(data.End, fr.End); dataEnd > fr.Start {
				if err := scanData(memmap.FileRange{Start: fr.Start, End: dataEnd}); err != nil {
					return stats, err
				}
				fr.Start = dataEnd
			}
		}
	}
	if err := flushZero(); err != nil {
		return stats, err
	}
	return stats, flushData()
}

// privateMemoryFiles holds MemoryFiles that have a resource ID and haven't
// been destroyed, so that pre-copy migration can find them while the kernel
// runs.
var privateMemoryFiles struct {
	mu  sync.Mutex
	mfs map[*MemoryFile]struct{}
}

func registerPrivateMemoryFile(f *MemoryFile) {
	privateMemoryFiles.mu.Lock()
	defer privateMemoryFiles.mu.Unlock()
	if privateMemoryFiles.mfs == nil {
		privateMemoryFiles.mfs = make(map[*MemoryFile]struct{})
	}
	privateMemoryFiles.mfs[f] = struct{}{}
}

func unregisterPrivateMemoryFile(f *MemoryFile) {
	privateMemoryFiles.mu.Lock()
	defer privateMemoryFiles.mu.Unlock()
	delete(privateMemoryFiles.mfs, f)
}

// ForEachPrivateMemoryFile calls fn for every MemoryFile that was created with
// a resource ID and hasn't been destroyed.
func ForEachPrivateMemoryFile(fn func(*MemoryFile)) {
	privateMemoryFiles.mu.Lock()
	mfs := make([]*MemoryFile, 0, len(privateMemoryFiles.mfs))
	for mf := range privateMemoryFiles.mfs {
		mfs = append(mfs, mf)
	}
	privateMemoryFiles.mu.Unlock()
	for _, mf := range mfs {
		fn(mf)
	}
}
