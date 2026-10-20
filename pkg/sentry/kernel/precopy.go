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

package kernel

import (
	"fmt"
	"time"

	"gvisor.dev/gvisor/pkg/context"
	"gvisor.dev/gvisor/pkg/log"
	"gvisor.dev/gvisor/pkg/sentry/memmap"
	"gvisor.dev/gvisor/pkg/sentry/mm"
	"gvisor.dev/gvisor/pkg/sentry/pgalloc"
)

// PrecopyStream is the destination of pre-copy migration.
type PrecopyStream interface {
	pgalloc.PrecopySinks

	// Round reports that a pre-copy round has ended.
	Round(round uint32, bytes uint64) error

	// Finish reports that the final save succeeded and the image is
	// complete.
	Finish() error
}

// PrecopyOpts configures pre-copy migration.
type PrecopyOpts struct {
	// Stream receives memory contents.
	Stream PrecopyStream

	// Downtime is the target duration for sending the remaining dirty pages
	// while the sandbox is stopped. Pre-copy rounds continue while the
	// remaining dirty pages are estimated to take longer than Downtime to
	// send, up to MaxRounds.
	Downtime time.Duration

	// MaxRounds is the maximum number of rounds after the first full copy.
	MaxRounds int
}

// PrecopyStats describes a completed pre-copy migration.
type PrecopyStats struct {
	// Rounds is the number of rounds run while the sandbox was running,
	// including the first full copy.
	Rounds int

	// LiveBytes is the number of bytes of page contents sent while the
	// sandbox was running; LiveDuration is the time that took.
	LiveBytes    uint64
	LiveDuration time.Duration

	// FinalBytes is the number of bytes of page contents sent while the
	// sandbox was stopped.
	FinalBytes uint64
}

// precopySession is the state of a pre-copy migration between the live
// rounds and the final save.
type precopySession struct {
	opts  PrecopyOpts
	stats PrecopyStats
	// mfs are the MemoryFiles with dirty tracking enabled.
	mfs []*pgalloc.MemoryFile
}

// Precopy runs the live phase of pre-copy migration: it enables dirty tracking
// for the kernel's MemoryFiles, sends all of their pages to opts.Stream while
// the kernel keeps running, then sends pages dirtied in the meantime in
// further rounds, until the remaining dirty pages are estimated to be sendable
// within opts.Downtime or opts.MaxRounds is reached. The remaining dirty pages
// are sent by the next SaveTo, which must follow (successful or not) unless
// Precopy returns an error; PrecopyDone ends dirty tracking after it.
//
// Precondition: The kernel is running.
func (k *Kernel) Precopy(ctx context.Context, opts PrecopyOpts) error {
	if k.precopy != nil {
		return fmt.Errorf("pre-copy migration already in progress")
	}
	s := &precopySession{opts: opts}

	// Dirty-track the main MemoryFile and the private MemoryFiles that exist
	// now. Private MemoryFiles that are created later are sent in full by
	// SaveTo.
	mfs := []*pgalloc.MemoryFile{k.mf}
	pgalloc.ForEachPrivateMemoryFile(func(mf *pgalloc.MemoryFile) {
		mfs = append(mfs, mf)
	})
	for _, mf := range mfs {
		if err := mf.AwaitLoadAll(); err != nil {
			return fmt.Errorf("previous async page loading failed: %w", err)
		}
	}
	for _, mf := range mfs {
		mf.StartDirtyTracking()
	}
	s.mfs = mfs
	k.precopy = s
	ok := false
	defer func() {
		if !ok {
			k.PrecopyDone()
		}
	}()

	// Every write that was possible without being recorded completes before
	// this returns.
	mm.WriteProtectAllForDirtyTracking(ctx)

	start := time.Now()
	// Round 0: every allocated page.
	var sent uint64
	for _, mf := range mfs {
		stats, err := mf.SendRanges(mf.AllocatedRanges(), opts.Stream.MemoryFileSink(mf.ResourceID()))
		if err != nil {
			return err
		}
		sent += stats.DataBytes
	}
	if err := opts.Stream.Round(0, sent); err != nil {
		return err
	}
	s.stats.Rounds = 1
	s.stats.LiveBytes = sent
	// Bandwidth estimate in bytes per second, from the previous round.
	roundDur := time.Since(start)
	bw := float64(sent) / max(roundDur.Seconds(), 1e-3)
	log.Infof("Pre-copy round 0: sent %d bytes in %s (%.1f MB/s)", sent, roundDur, bw/1e6)

	for round := 1; ; round++ {
		var dirty uint64
		for _, mf := range mfs {
			dirty += mf.DirtyBytes()
		}
		est := time.Duration(float64(dirty) / bw * float64(time.Second))
		if est <= opts.Downtime || round > opts.MaxRounds {
			log.Infof("Pre-copy: %d bytes dirty after %d rounds, estimated %s to send (target %s); stopping the sandbox", dirty, round, est, opts.Downtime)
			break
		}

		// Collect the dirty pages, then write-protect again so that writes
		// after the collection are recorded for the next round; this also
		// waits for marked writes that were in progress to complete.
		dirtyFRs := make([][]memmap.FileRange, len(mfs))
		for i, mf := range mfs {
			dirtyFRs[i] = mf.TakeDirty()
		}
		mm.WriteProtectAllForDirtyTracking(ctx)

		roundStart := time.Now()
		sent = 0
		for i, mf := range mfs {
			stats, err := sendDirty(mf, dirtyFRs[i], opts.Stream.MemoryFileSink(mf.ResourceID()))
			if err != nil {
				return err
			}
			sent += stats.DataBytes
		}
		roundDur = time.Since(roundStart)
		if sent != 0 && roundDur > time.Millisecond {
			bw = float64(sent) / roundDur.Seconds()
		}
		if err := opts.Stream.Round(uint32(round), sent); err != nil {
			return err
		}
		s.stats.Rounds++
		s.stats.LiveBytes += sent
		log.Infof("Pre-copy round %d: sent %d bytes in %s (%.1f MB/s)", round, sent, roundDur, bw/1e6)
	}
	s.stats.LiveDuration = time.Since(start)
	ok = true
	return nil
}

// sendDirty sends the pages in frs, which were dirtied while mf was in use:
// allocated pages by contents, and free pages as zeroes, since free pages are
// zero but can't be read without committing them.
func sendDirty(mf *pgalloc.MemoryFile, frs []memmap.FileRange, sink pgalloc.PrecopySink) (pgalloc.PrecopyStats, error) {
	allocated := mf.AllocatedRanges()
	stats, err := mf.SendRanges(pgalloc.IntersectRanges(frs, allocated), sink)
	if err != nil {
		return stats, err
	}
	for _, fr := range pgalloc.SubtractRanges(frs, allocated) {
		if err := sink.WriteZero(fr.Start, fr.Length()); err != nil {
			return stats, err
		}
		stats.ZeroBytes += fr.Length()
	}
	return stats, nil
}

// PrecopyDone ends a pre-copy migration started by Precopy, after the final
// SaveTo or after Precopy failed, and returns its statistics.
func (k *Kernel) PrecopyDone() PrecopyStats {
	s := k.precopy
	if s == nil {
		return PrecopyStats{}
	}
	for _, mf := range s.mfs {
		mf.StopDirtyTracking()
	}
	k.precopy = nil
	return s.stats
}
