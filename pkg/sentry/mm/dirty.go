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

package mm

import (
	"gvisor.dev/gvisor/pkg/context"
	"gvisor.dev/gvisor/pkg/hostarch"
	"gvisor.dev/gvisor/pkg/sentry/memmap"
	"gvisor.dev/gvisor/pkg/sentry/pgalloc"
	"gvisor.dev/gvisor/pkg/sync"
)

// Dirty tracking of application memory for pre-copy migration (see
// pgalloc/dirty.go).
//
// While a MemoryFile is dirty-tracked, pmas backed by it are mapped into the
// AddressSpace without write permission, except for pages that were written by
// the application since the last call to WriteProtectForDirtyTracking. The
// first write to any other page faults, and HandleUserFault marks that page
// dirty and maps that page, and only that page, writable. Writes by the sentry
// through internal mappings are marked dirty by withInternalMappings and
// withVecInternalMappings. Both happen with mm.activeMu locked, before the write
// is possible; WriteProtectForDirtyTracking locks mm.activeMu for writing, so
// once it returns, every write that was marked before it was called has
// completed.

// maxDirtyTrackedWritablePages is the number of pages that may be made
// writable individually in an AddressSpace for dirty tracking before the
// AddressSpace is write-protected again. Each such page may split a host
// mapping, and the host limits the number of mappings (vm.max_map_count,
// 65530 by default).
const maxDirtyTrackedWritablePages = 16384

// isDirtyTracked returns the pgalloc.MemoryFile that backs file if dirty
// tracking is enabled for it.
func isDirtyTracked(file memmap.File) (*pgalloc.MemoryFile, bool) {
	if !pgalloc.DirtyTrackingActive() {
		return nil, false
	}
	mf, ok := file.(*pgalloc.MemoryFile)
	if !ok || !mf.IsDirtyTracking() {
		return nil, false
	}
	return mf, true
}

// WriteProtectForDirtyTracking removes write permission from all AddressSpace
// mappings of dirty-tracked MemoryFiles, so that the next application write to
// each page is recorded. It also waits for writes through internal mappings
// that were marked dirty before it was called to complete.
//
// +checklocksexclude:mm.activeMu
func (mm *MemoryManager) WriteProtectForDirtyTracking() {
	mm.activeMu.Lock()
	defer mm.activeMu.Unlock()
	mm.writeProtectForDirtyTrackingLocked()
}

// +checklocks:mm.activeMu
func (mm *MemoryManager) writeProtectForDirtyTrackingLocked() {
	mm.dirtyTrackedWritablePages.Store(0)
	if mm.as == nil {
		return
	}
	for pseg := mm.pmas.FirstSegment(); pseg.Ok(); pseg = pseg.NextSegment() {
		pma := pseg.ValuePtr()
		if !pma.effectivePerms.Write || pma.needCOW {
			continue
		}
		if _, ok := isDirtyTracked(pma.file); !ok {
			continue
		}
		perms := pma.effectivePerms
		perms.Write = false
		// Remap read-only rather than unmapping, so that reads don't fault.
		// Limit the size of each call as mapASLocked does.
		const singleMapThreshold = 1 << 30
		pmaAR := pseg.Range()
		for start := pmaAR.Start; start < pmaAR.End; {
			end := min(pmaAR.End, (start+singleMapThreshold)&^(singleMapThreshold-1))
			ar := hostarch.AddrRange{Start: start, End: end}
			if err := mm.as.MapFile(ar.Start, pma.file, pseg.fileRangeOf(ar), perms, false /* precommit */); err != nil {
				// Unmap instead, which can't fail; the next access faults.
				mm.as.Unmap(ar.Start, uint64(ar.Length()))
			}
			start = end
		}
	}
}

// markDirtyLocked marks the pages backing ar dirty in dirty-tracked
// MemoryFiles, before a write to ar through internal mappings.
//
// Preconditions:
//   - mm.activeMu must be locked.
//   - pmas must exist for all addresses in ar.
func (mm *MemoryManager) markDirtyLocked(ar hostarch.AddrRange) {
	if !pgalloc.DirtyTrackingActive() || ar.Length() == 0 {
		return
	}
	ar.Start = ar.Start.RoundDown()
	ar.End, _ = ar.End.RoundUp()
	for pseg := mm.pmas.FindSegment(ar.Start); pseg.Ok() && pseg.Start() < ar.End; pseg = pseg.NextSegment() {
		if mf, ok := isDirtyTracked(pseg.ValuePtr().file); ok {
			mf.MarkDirty(pseg.fileRangeOf(pseg.Range().Intersect(ar)))
		}
	}
}

// markDirtyVecLocked is the vector version of markDirtyLocked.
//
// Preconditions: As for markDirtyLocked, for every range in ars.
func (mm *MemoryManager) markDirtyVecLocked(ars hostarch.AddrRangeSeq) {
	if !pgalloc.DirtyTrackingActive() {
		return
	}
	for ; !ars.IsEmpty(); ars = ars.Tail() {
		mm.markDirtyLocked(ars.Head())
	}
}

// memoryManagers holds every MemoryManager with a non-zero user count, so that
// WriteProtectAllForDirtyTracking can find MemoryManagers that are not (or not
// yet) referenced by a task, such as the new image of a task in execve.
var memoryManagers struct {
	mu  sync.Mutex
	mms map[*MemoryManager]struct{}
}

func registerMemoryManager(mm *MemoryManager) {
	memoryManagers.mu.Lock()
	defer memoryManagers.mu.Unlock()
	if memoryManagers.mms == nil {
		memoryManagers.mms = make(map[*MemoryManager]struct{})
	}
	memoryManagers.mms[mm] = struct{}{}
}

func unregisterMemoryManager(mm *MemoryManager) {
	memoryManagers.mu.Lock()
	defer memoryManagers.mu.Unlock()
	delete(memoryManagers.mms, mm)
}

// WriteProtectAllForDirtyTracking calls WriteProtectForDirtyTracking for every
// MemoryManager in use.
func WriteProtectAllForDirtyTracking(ctx context.Context) {
	memoryManagers.mu.Lock()
	mms := make([]*MemoryManager, 0, len(memoryManagers.mms))
	for mm := range memoryManagers.mms {
		if mm.IncUsers() {
			mms = append(mms, mm)
		}
	}
	memoryManagers.mu.Unlock()
	for _, mm := range mms {
		mm.WriteProtectForDirtyTracking()
		mm.DecUsers(ctx)
	}
}
