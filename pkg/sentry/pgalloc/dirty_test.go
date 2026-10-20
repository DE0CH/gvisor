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
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/hostarch"
	"gvisor.dev/gvisor/pkg/safemem"
	"gvisor.dev/gvisor/pkg/sentry/checkpoint"
	"gvisor.dev/gvisor/pkg/sentry/memmap"
	"gvisor.dev/gvisor/pkg/sentry/state/stateio"
	"gvisor.dev/gvisor/pkg/sentry/usage"
)

const pg = hostarch.PageSize

func fr(start, end uint64) memmap.FileRange {
	return memmap.FileRange{Start: start * pg, End: end * pg}
}

func TestDirtyBitmap(t *testing.T) {
	for _, tc := range []struct {
		name string
		mark []memmap.FileRange
		want []memmap.FileRange
	}{
		{name: "empty"},
		{name: "one page", mark: []memmap.FileRange{fr(3, 4)}, want: []memmap.FileRange{fr(3, 4)}},
		{name: "unaligned is rounded out", mark: []memmap.FileRange{{Start: 3*pg + 1, End: 4*pg + 1}}, want: []memmap.FileRange{fr(3, 5)}},
		{name: "merge adjacent", mark: []memmap.FileRange{fr(0, 2), fr(2, 3), fr(10, 11)}, want: []memmap.FileRange{fr(0, 3), fr(10, 11)}},
		{name: "across words", mark: []memmap.FileRange{fr(60, 70)}, want: []memmap.FileRange{fr(60, 70)}},
		{name: "full words", mark: []memmap.FileRange{fr(64, 256)}, want: []memmap.FileRange{fr(64, 256)}},
		{name: "across chunks", mark: []memmap.FileRange{{Start: chunkSize - pg, End: chunkSize + pg}}, want: []memmap.FileRange{{Start: chunkSize - pg, End: chunkSize + pg}}},
		{name: "sparse chunks", mark: []memmap.FileRange{fr(1, 2), {Start: 3 * chunkSize, End: 3*chunkSize + pg}}, want: []memmap.FileRange{fr(1, 2), {Start: 3 * chunkSize, End: 3*chunkSize + pg}}},
		{name: "overlapping", mark: []memmap.FileRange{fr(5, 9), fr(7, 12)}, want: []memmap.FileRange{fr(5, 12)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newDirtyBitmap()
			for _, r := range tc.mark {
				b.mark(r)
			}
			if got := b.ranges(nil); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ranges: got %v, want %v", got, tc.want)
			}
			var wantPages uint64
			for _, r := range tc.want {
				wantPages += r.Length() / pg
			}
			if got := b.pages(); got != wantPages {
				t.Errorf("pages: got %d, want %d", got, wantPages)
			}
		})
	}
}

func TestRangeSetOps(t *testing.T) {
	a := []memmap.FileRange{fr(0, 4), fr(6, 10), fr(12, 13)}
	b := []memmap.FileRange{fr(2, 7), fr(9, 20)}
	if got, want := IntersectRanges(a, b), []memmap.FileRange{fr(2, 4), fr(6, 7), fr(9, 10), fr(12, 13)}; !reflect.DeepEqual(got, want) {
		t.Errorf("IntersectRanges: got %v, want %v", got, want)
	}
	if got, want := SubtractRanges(a, b), []memmap.FileRange{fr(0, 2), fr(7, 9)}; !reflect.DeepEqual(got, want) {
		t.Errorf("SubtractRanges(a, b): got %v, want %v", got, want)
	}
	if got, want := SubtractRanges(b, a), []memmap.FileRange{fr(4, 6), fr(10, 12), fr(13, 20)}; !reflect.DeepEqual(got, want) {
		t.Errorf("SubtractRanges(b, a): got %v, want %v", got, want)
	}
	if got, want := UnionRanges(a, b), []memmap.FileRange{fr(0, 20)}; !reflect.DeepEqual(got, want) {
		t.Errorf("UnionRanges: got %v, want %v", got, want)
	}
	if got := SubtractRanges(a, nil); !reflect.DeepEqual(got, a) {
		t.Errorf("SubtractRanges(a, nil): got %v, want %v", got, a)
	}
}

// recordingSink records what SendRanges sends.
type recordingSink struct {
	data  map[uint64][]byte // by page offset
	zeros []memmap.FileRange
}

func (s *recordingSink) WritePages(off uint64, data [][]byte) error {
	for _, d := range data {
		for i := 0; i < len(d); i += int(pg) {
			s.data[off] = bytes.Clone(d[i : i+int(pg)])
			off += pg
		}
	}
	return nil
}

func (s *recordingSink) WriteZero(off, length uint64) error {
	s.zeros = append(s.zeros, memmap.FileRange{Start: off, End: off + length})
	return nil
}

func allocate(t *testing.T, f *MemoryFile, pages uint64) memmap.FileRange {
	t.Helper()
	r, err := f.Allocate(pages*pg, AllocOpts{Kind: usage.Anonymous, Mode: AllocateAndCommit})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	return r
}

func writePage(t *testing.T, f *MemoryFile, off uint64, b byte, tracked bool) {
	t.Helper()
	r := memmap.FileRange{Start: off, End: off + pg}
	var ims safemem.BlockSeq
	var err error
	if tracked {
		ims, err = f.MapInternalUntracked(r, hostarch.Write)
	} else {
		ims, err = f.MapInternal(r, hostarch.Write)
	}
	if err != nil {
		t.Fatalf("MapInternal: %v", err)
	}
	if _, err := safemem.CopySeq(ims, safemem.BlockSeqOf(safemem.BlockFromSafeSlice(bytes.Repeat([]byte{b}, int(pg))))); err != nil {
		t.Fatalf("CopySeq: %v", err)
	}
	if tracked {
		f.MarkDirty(r)
	}
}

func TestDirtyTracking(t *testing.T) {
	for _, diskBacked := range []bool{false, true} {
		t.Run(fmt.Sprintf("diskBacked=%t", diskBacked), func(t *testing.T) {
			f := newSaveTestMemoryFile(t, diskBacked)
			r := allocate(t, f, 8)
			writePage(t, f, r.Start, 1, true)
			writePage(t, f, r.Start+pg, 2, true)
			// Pages 2-7 stay zero.

			f.StartDirtyTracking()
			defer f.StopDirtyTracking()
			if got := f.TakeDirty(); len(got) != 0 {
				t.Errorf("TakeDirty right after StartDirtyTracking: got %v, want nothing", got)
			}

			// A full copy, as in pre-copy round 0.
			sink := &recordingSink{data: make(map[uint64][]byte)}
			stats, err := f.SendRanges(f.AllocatedRanges(), sink)
			if err != nil {
				t.Fatalf("SendRanges: %v", err)
			}
			if stats.DataBytes != 2*pg || len(sink.data) != 2 || sink.data[r.Start][0] != 1 || sink.data[r.Start+pg][0] != 2 {
				t.Errorf("round 0 sent %d data bytes (%d pages), want the 2 non-zero pages", stats.DataBytes, len(sink.data))
			}
			if want := []memmap.FileRange{{Start: r.Start + 2*pg, End: r.End}}; !reflect.DeepEqual(sink.zeros, want) {
				t.Errorf("round 0 zero ranges: got %v, want %v", sink.zeros, want)
			}

			// Precisely tracked write, untracked (sticky) write, decommit.
			writePage(t, f, r.Start+3*pg, 3, true)
			writePage(t, f, r.Start+5*pg, 5, false)
			f.Decommit(memmap.FileRange{Start: r.Start, End: r.Start + pg})

			wantDirty := []memmap.FileRange{{Start: r.Start, End: r.Start + pg}, {Start: r.Start + 3*pg, End: r.Start + 4*pg}}
			if got := f.TakeDirty(); !reflect.DeepEqual(got, wantDirty) {
				t.Errorf("TakeDirty: got %v, want %v", got, wantDirty)
			}
			if got := f.TakeDirty(); len(got) != 0 {
				t.Errorf("second TakeDirty: got %v, want nothing", got)
			}
			// Sticky pages stay dirty until tracking stops.
			wantSticky := []memmap.FileRange{{Start: r.Start + 5*pg, End: r.Start + 6*pg}}
			for i := 0; i < 2; i++ {
				if got := f.StickyDirty(); !reflect.DeepEqual(got, wantSticky) {
					t.Errorf("StickyDirty: got %v, want %v", got, wantSticky)
				}
			}
			if got, want := f.DirtyBytes(), uint64(pg); got != want {
				t.Errorf("DirtyBytes: got %d, want %d", got, want)
			}

			// Freeing pages decommits them eventually, which marks them.
			f.DecRef(r)
			f.StopDirtyTracking()
			if f.IsDirtyTracking() || DirtyTrackingActive() {
				t.Errorf("dirty tracking still enabled after StopDirtyTracking")
			}
		})
	}
}

func TestPrecopyPagesFileOffset(t *testing.T) {
	if got := PrecopyPagesFileOffset(checkpoint.ResourceID{}); got != 0 {
		t.Errorf("main MemoryFile: got %#x, want 0", got)
	}
	a := PrecopyPagesFileOffset(checkpoint.ResourceID{ContainerName: "c", Path: "/a"})
	b := PrecopyPagesFileOffset(checkpoint.ResourceID{ContainerName: "c", Path: "/b"})
	for _, off := range []uint64{a, b} {
		if off == 0 || off%(1<<44) != 0 || off >= 1<<63 {
			t.Errorf("private MemoryFile offset %#x is not a non-zero, 16 TiB-aligned offset below 2^63", off)
		}
	}
	if a == b {
		t.Errorf("different resource IDs map to the same offset %#x", a)
	}
}

// fileSinks implements PrecopySinks by writing to a pre-copy pages file.
type fileSinks struct {
	f *os.File
}

func (s fileSinks) MemoryFileSink(id checkpoint.ResourceID) PrecopySink {
	return fileSink{f: s.f, base: PrecopyPagesFileOffset(id)}
}

type fileSink struct {
	f    *os.File
	base uint64
}

func (s fileSink) WritePages(off uint64, data [][]byte) error {
	for _, d := range data {
		if _, err := s.f.WriteAt(d, int64(s.base+off)); err != nil {
			return err
		}
		off += uint64(len(d))
	}
	return nil
}

func (s fileSink) WriteZero(off, length uint64) error {
	return unix.Fallocate(int(s.f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, int64(s.base+off), int64(length))
}

// TestPrecopySaveLoad sends a MemoryFile's pages while it changes, then saves
// it with the final pre-copy save, and checks that loading from the resulting
// pages file reproduces the MemoryFile's final contents.
func TestPrecopySaveLoad(t *testing.T) {
	for _, diskBacked := range []bool{false, true} {
		t.Run(fmt.Sprintf("diskBacked=%t", diskBacked), func(t *testing.T) {
			f := newSaveTestMemoryFile(t, diskBacked)
			r := allocate(t, f, 16)
			for i := uint64(0); i < 8; i++ {
				writePage(t, f, r.Start+i*pg, byte(10+i), true)
			}
			pagesFile, err := os.CreateTemp("/dev/shm", "pgalloc-precopy-test")
			if err != nil {
				t.Skipf("no tmpfs at /dev/shm: %v", err)
			}
			defer os.Remove(pagesFile.Name())
			defer pagesFile.Close()
			sinks := fileSinks{f: pagesFile}

			f.StartDirtyTracking()
			defer f.StopDirtyTracking()
			if _, err := f.SendRanges(f.AllocatedRanges(), sinks.MemoryFileSink(f.ResourceID())); err != nil {
				t.Fatalf("SendRanges: %v", err)
			}
			// Changes after round 0: tracked writes, an untracked write,
			// decommitment, and a write to a page that was zero.
			writePage(t, f, r.Start+1*pg, 21, true)
			writePage(t, f, r.Start+2*pg, 22, false)
			f.Decommit(memmap.FileRange{Start: r.Start + 3*pg, End: r.Start + 4*pg})
			writePage(t, f, r.Start+12*pg, 32, true)
			// A second round sends some of them.
			if _, err := f.SendRanges(IntersectRanges(f.TakeDirty(), f.AllocatedRanges()), sinks.MemoryFileSink(f.ResourceID())); err != nil {
				t.Fatalf("SendRanges: %v", err)
			}
			writePage(t, f, r.Start+5*pg, 25, true)

			var meta bytes.Buffer
			popts := &PrecopySaveOpts{Sinks: sinks}
			if err := f.SaveTo(context.Background(), &meta, &SaveOpts{Precopy: popts}); err != nil {
				t.Fatalf("SaveTo: %v", err)
			}
			// Only the page written after the second round and the sticky
			// page should have been sent by the final save.
			if got, want := popts.Stats.DataBytes, 2*uint64(pg); got != want {
				t.Errorf("final save sent %d bytes, want %d", got, want)
			}
			want := make([]byte, r.Length())
			f.forEachMappingSlice(r, func(bs []byte) { copy(want, bs) })

			restored := newSaveTestMemoryFile(t, diskBacked)
			done := make(chan error, 1)
			readFD, err := unix.Dup(int(pagesFile.Fd()))
			if err != nil {
				t.Fatalf("dup: %v", err)
			}
			apfl, err := StartAsyncPagesFileLoad(stateio.NewPagesFileFDReaderDefault(int32(readFD)), func(err error) { done <- err }, nil) // takes ownership of readFD
			if err != nil {
				t.Fatalf("StartAsyncPagesFileLoad: %v", err)
			}
			if err := restored.LoadFrom(context.Background(), &meta, &LoadOpts{PagesFile: apfl, PagesFileAtFileOffsets: true}); err != nil {
				t.Fatalf("LoadFrom: %v", err)
			}
			apfl.MemoryFilesDone()
			if err := <-done; err != nil {
				t.Fatalf("async page loading: %v", err)
			}
			got := make([]byte, r.Length())
			restored.forEachMappingSlice(r, func(bs []byte) { copy(got, bs) })
			for i := uint64(0); i < r.Length()/pg; i++ {
				if g, w := got[i*pg:(i+1)*pg], want[i*pg:(i+1)*pg]; !bytes.Equal(g, w) {
					t.Errorf("page %d: restored first byte %d, want %d", i, g[0], w[0])
				}
			}
		})
	}
}

// TestAsyncLoadHoldBulk checks that while bulk loading is held, only awaited
// pages are loaded, and that releasing it loads the rest.
func TestAsyncLoadHoldBulk(t *testing.T) {
	f := newSaveTestMemoryFile(t, false)
	r := allocate(t, f, 64)
	for i := uint64(0); i < 64; i++ {
		writePage(t, f, r.Start+i*pg, byte(i+1), true)
	}
	pagesFile, err := os.CreateTemp("/dev/shm", "pgalloc-hold-test")
	if err != nil {
		t.Skipf("no tmpfs at /dev/shm: %v", err)
	}
	defer os.Remove(pagesFile.Name())
	defer pagesFile.Close()
	sinks := fileSinks{f: pagesFile}
	var meta bytes.Buffer
	if err := f.SaveTo(context.Background(), &meta, &SaveOpts{Precopy: &PrecopySaveOpts{Sinks: sinks}}); err != nil {
		t.Fatalf("SaveTo: %v", err)
	}

	restored := newSaveTestMemoryFile(t, false)
	readFD, err := unix.Dup(int(pagesFile.Fd()))
	if err != nil {
		t.Fatalf("dup: %v", err)
	}
	done := make(chan error, 1)
	apfl, err := StartAsyncPagesFileLoad(stateio.NewPagesFileFDReaderDefault(int32(readFD)), func(err error) { done <- err }, nil)
	if err != nil {
		t.Fatalf("StartAsyncPagesFileLoad: %v", err)
	}
	apfl.HoldBulkLoading()
	if err := restored.LoadFrom(context.Background(), &meta, &LoadOpts{PagesFile: apfl, PagesFileAtFileOffsets: true}); err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	apfl.MemoryFilesDone()

	// An awaited page is loaded while bulk loading is held.
	page := memmap.FileRange{Start: r.Start + 40*pg, End: r.Start + 41*pg}
	ims, err := restored.MapInternal(page, hostarch.Read)
	if err != nil {
		t.Fatalf("MapInternal: %v", err)
	}
	if b := ims.Head().ToSlice()[0]; b != 41 {
		t.Errorf("awaited page: got first byte %d, want 41", b)
	}
	select {
	case err := <-done:
		t.Fatalf("loading completed (%v) while bulk loading was held", err)
	default:
	}
	if !restored.IsAsyncLoading() {
		t.Errorf("all pages loaded while bulk loading was held")
	}

	apfl.ReleaseBulkLoading()
	if err := <-done; err != nil {
		t.Fatalf("async page loading: %v", err)
	}
	restored.forEachMappingSlice(r, func(bs []byte) {
		for i := uint64(0); i < 64; i++ {
			if got, want := bs[i*pg], byte(i+1); got != want {
				t.Errorf("page %d: got first byte %d, want %d", i, got, want)
			}
		}
	})
}
