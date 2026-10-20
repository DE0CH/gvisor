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
	"fmt"
	"math/bits"
	"sync/atomic"

	"gvisor.dev/gvisor/pkg/hostarch"
	"gvisor.dev/gvisor/pkg/safemem"
	"gvisor.dev/gvisor/pkg/sentry/memmap"
	"gvisor.dev/gvisor/pkg/sync"
)

// Dirty tracking records which pages of a MemoryFile may have changed, so that
// pre-copy live migration can copy a MemoryFile while the sandbox runs and then
// re-copy only the pages written since (see kernel.Kernel.Precopy).
//
// While tracking is enabled, every write to the MemoryFile's pages must be
// recorded by a call to MarkDirty, either:
//
//   - after the write completes; or
//
//   - before the write, while holding a lock that is also held across the
//     start of every pre-copy round (mm.MemoryManager does this for application
//     memory, using MemoryManager.activeMu).
//
// MemoryFile records its own writes (allocation, zeroing, decommitment) this
// way. Callers of MapInternal with write access can't be relied upon to do so,
// since the returned mappings may be written to at any later time. Such writes
// are therefore recorded in a "sticky" set that is only cleared when tracking
// stops, so that their pages are copied again in the final, stop-and-copy round
// when no writes are in progress. Callers that write through internal mappings
// and can record their writes precisely use MapInternalUntracked instead.

// dirtyWordsPerChunk is the number of bitmap words that cover one chunk.
const dirtyWordsPerChunk = chunkSize / hostarch.PageSize / 64

// dirtyChunk is a bitmap of the pages in a chunk; bit i of word j is set if page
// 64*j+i of the chunk is dirty.
type dirtyChunk [dirtyWordsPerChunk]atomic.Uint64

// dirtyBitmap is a set of pages in a MemoryFile. Pages may be added to it
// concurrently.
type dirtyBitmap struct {
	// growMu serializes growth of chunks.
	growMu sync.Mutex

	// chunks is indexed by chunk number. Elements may be nil. chunks slices
	// are immutable; growth replaces the slice.
	chunks atomic.Pointer[[]*dirtyChunk]
}

func newDirtyBitmap() *dirtyBitmap {
	b := &dirtyBitmap{}
	var chunks []*dirtyChunk
	b.chunks.Store(&chunks)
	return b
}

// chunk returns the dirtyChunk for chunk index i, allocating it if necessary.
func (b *dirtyBitmap) chunk(i uint64) *dirtyChunk {
	if chunks := *b.chunks.Load(); i < uint64(len(chunks)) && chunks[i] != nil {
		return chunks[i]
	}
	b.growMu.Lock()
	defer b.growMu.Unlock()
	old := *b.chunks.Load()
	if i < uint64(len(old)) && old[i] != nil {
		return old[i]
	}
	n := len(old)
	if i >= uint64(n) {
		n = int(i) + 1
	}
	chunks := make([]*dirtyChunk, n)
	copy(chunks, old)
	chunks[i] = new(dirtyChunk)
	b.chunks.Store(&chunks)
	return chunks[i]
}

// mark adds all pages overlapping fr to b.
func (b *dirtyBitmap) mark(fr memmap.FileRange) {
	start := fr.Start >> hostarch.PageShift
	end := (fr.End + hostarch.PageSize - 1) >> hostarch.PageShift
	const pagesPerChunk = chunkSize / hostarch.PageSize
	for pg := start; pg < end; {
		c := b.chunk(pg / pagesPerChunk)
		chunkEnd := min(end, (pg/pagesPerChunk+1)*pagesPerChunk)
		for pg < chunkEnd {
			bit := pg % pagesPerChunk
			word := bit / 64
			n := min(64-bit%64, chunkEnd-pg)
			mask := ^uint64(0)
			if n < 64 {
				mask = (uint64(1)<<n - 1)
			}
			mask <<= bit % 64
			if c[word].Load()&mask != mask {
				c[word].Or(mask)
			}
			pg += n
		}
	}
}

// pages returns the number of pages in b.
func (b *dirtyBitmap) pages() uint64 {
	var n uint64
	for _, c := range *b.chunks.Load() {
		if c == nil {
			continue
		}
		for i := range c {
			n += uint64(bits.OnesCount64(c[i].Load()))
		}
	}
	return n
}

// ranges appends the maximal ranges of pages in b to frs, in increasing order,
// and returns the result.
func (b *dirtyBitmap) ranges(frs []memmap.FileRange) []memmap.FileRange {
	for ci, c := range *b.chunks.Load() {
		if c == nil {
			continue
		}
		base := uint64(ci) * chunkSize
		for wi := range c {
			w := c[wi].Load()
			for w != 0 {
				lo := uint64(bits.TrailingZeros64(w))
				run := uint64(bits.TrailingZeros64(^(w >> lo)))
				start := base + (uint64(wi)*64+lo)*hostarch.PageSize
				end := start + run*hostarch.PageSize
				if n := len(frs); n != 0 && frs[n-1].End == start {
					frs[n-1].End = end
				} else {
					frs = append(frs, memmap.FileRange{Start: start, End: end})
				}
				if lo+run == 64 {
					break
				}
				w &^= (uint64(1)<<run - 1) << lo
			}
		}
	}
	return frs
}

// dirtyTracker holds the dirty tracking state of a MemoryFile.
type dirtyTracker struct {
	// mu is locked for reading while marking pages in cur, and for writing
	// while replacing cur, so that no mark can be lost between reading cur and
	// replacing it.
	mu sync.RWMutex

	// cur holds pages marked since the last call to TakeDirty.
	cur *dirtyBitmap

	// sticky holds pages that may have been written through untracked internal
	// mappings since tracking started. See the comment at the top of this file.
	sticky *dirtyBitmap
}

// dirtyTrackingFiles is the number of MemoryFiles with dirty tracking enabled.
// It allows callers to cheaply skip dirty tracking work when no MemoryFile is
// tracked.
var dirtyTrackingFiles atomic.Int32

// DirtyTrackingActive returns true if dirty tracking is enabled for any
// MemoryFile.
func DirtyTrackingActive() bool {
	return dirtyTrackingFiles.Load() != 0
}

// StartDirtyTracking enables dirty tracking for f. Pages written after
// StartDirtyTracking returns are reported by TakeDirty or StickyDirty.
//
// Precondition: Dirty tracking is not enabled for f.
func (f *MemoryFile) StartDirtyTracking() {
	t := &dirtyTracker{
		cur:    newDirtyBitmap(),
		sticky: newDirtyBitmap(),
	}
	if !f.dirty.CompareAndSwap(nil, t) {
		panic(fmt.Sprintf("MemoryFile(%p): dirty tracking already enabled", f))
	}
	dirtyTrackingFiles.Add(1)
}

// StopDirtyTracking disables dirty tracking for f. It has no effect if dirty
// tracking is not enabled.
func (f *MemoryFile) StopDirtyTracking() {
	if f.dirty.Swap(nil) != nil {
		dirtyTrackingFiles.Add(-1)
	}
}

// IsDirtyTracking returns true if dirty tracking is enabled for f.
func (f *MemoryFile) IsDirtyTracking() bool {
	return f.dirty.Load() != nil
}

// MarkDirty records that pages in fr have been or are being written. See the
// comment at the top of this file for when it must be called.
func (f *MemoryFile) MarkDirty(fr memmap.FileRange) {
	t := f.dirty.Load()
	if t == nil || fr.Length() == 0 {
		return
	}
	t.mu.RLock()
	t.cur.mark(fr)
	t.mu.RUnlock()
}

// markDirtySticky records that pages in fr may be written through internal
// mappings at any time.
func (f *MemoryFile) markDirtySticky(fr memmap.FileRange) {
	if t := f.dirty.Load(); t != nil {
		t.sticky.mark(fr)
	}
}

// TakeDirty returns the pages marked dirty since dirty tracking started or
// since the previous call to TakeDirty, whichever is later, and clears them.
// Pages in the sticky set are not returned; see StickyDirty. The returned
// ranges are page-aligned and in increasing order.
//
// Precondition: Dirty tracking is enabled for f.
func (f *MemoryFile) TakeDirty() []memmap.FileRange {
	t := f.dirty.Load()
	t.mu.Lock()
	cur := t.cur
	t.cur = newDirtyBitmap()
	t.mu.Unlock()
	return cur.ranges(nil)
}

// StickyDirty returns pages that may have been written through untracked
// internal mappings since dirty tracking started. These pages must be copied
// again once no writes can be in progress. The returned ranges are page-aligned
// and in increasing order.
//
// Precondition: Dirty tracking is enabled for f.
func (f *MemoryFile) StickyDirty() []memmap.FileRange {
	return f.dirty.Load().sticky.ranges(nil)
}

// DirtyBytes returns the number of bytes currently marked dirty in f,
// including sticky pages. It returns 0 if dirty tracking is not enabled.
func (f *MemoryFile) DirtyBytes() uint64 {
	t := f.dirty.Load()
	if t == nil {
		return 0
	}
	t.mu.RLock()
	n := t.cur.pages()
	t.mu.RUnlock()
	return (n + t.sticky.pages()) * hostarch.PageSize
}

// MapInternalUntracked is equivalent to MapInternal, except that writes through
// the returned mappings are not recorded for dirty tracking. Callers that write
// through the returned mappings must call MarkDirty as described at the top of
// this file.
func (f *MemoryFile) MapInternalUntracked(fr memmap.FileRange, at hostarch.AccessType) (safemem.BlockSeq, error) {
	return f.mapInternal(fr, at)
}

// MapInternalUntracked calls file.MapInternalUntracked if file is a
// *MemoryFile, and file.MapInternal otherwise.
func MapInternalUntracked(file memmap.File, fr memmap.FileRange, at hostarch.AccessType) (safemem.BlockSeq, error) {
	if mf, ok := file.(*MemoryFile); ok {
		return mf.mapInternal(fr, at)
	}
	return file.MapInternal(fr, at)
}

// MarkDirty calls file.MarkDirty if file is a *MemoryFile with dirty tracking
// enabled.
func MarkDirty(file memmap.File, fr memmap.FileRange) {
	if mf, ok := file.(*MemoryFile); ok {
		mf.MarkDirty(fr)
	}
}
