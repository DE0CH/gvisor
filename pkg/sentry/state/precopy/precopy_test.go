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

package precopy

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gvisor.dev/gvisor/pkg/sentry/checkpoint"
	"gvisor.dev/gvisor/pkg/sentry/pgalloc"
	"gvisor.dev/gvisor/pkg/sentry/state/checkpointfiles"
)

// pipe returns the ends of a pipe; the read end is an *os.File.
func pipe(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	t.Cleanup(func() {
		r.Close()
		w.Close()
	})
	return r, w
}

// imageDir returns a directory for a received image. Pre-copy pages files
// are sparse files up to 2^63 bytes, which need tmpfs (or XFS, btrfs).
func imageDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/dev/shm", "precopy-test")
	if err != nil {
		t.Skipf("no tmpfs at /dev/shm: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestRoundTrip(t *testing.T) {
	r, wf := pipe(t)
	dir := imageDir(t)

	page := func(b byte) []byte { return bytes.Repeat([]byte{b}, 4096) }
	private := checkpoint.ResourceID{ContainerName: "c", Path: "/upper"}
	privBase := pgalloc.PrecopyPagesFileOffset(private)

	errc := make(chan error, 1)
	go func() {
		defer wf.Close()
		errc <- func() error {
			w, err := NewWriter(int(wf.Fd()))
			if err != nil {
				return err
			}
			main := w.MemoryFileSink(checkpoint.ResourceID{})
			priv := w.MemoryFileSink(private)
			// Round 0: two pages of the main MemoryFile, one of the private one.
			if err := main.WritePages(0, [][]byte{page(1), page(2)}); err != nil {
				return err
			}
			if err := priv.WritePages(4096, [][]byte{page(3)}); err != nil {
				return err
			}
			if err := w.Round(0, 3*4096); err != nil {
				return err
			}
			// Round 1: page 1 rewritten, page 0 zeroed.
			if err := main.WritePages(4096, [][]byte{page(4)}); err != nil {
				return err
			}
			if err := main.WriteZero(0, 4096); err != nil {
				return err
			}
			// Final: kernel state, larger than one File record.
			state := w.File(FileState)
			if _, err := state.Write(bytes.Repeat([]byte("s"), 3<<20)); err != nil {
				return err
			}
			if err := state.Close(); err != nil {
				return err
			}
			meta := w.File(FilePagesMetadata)
			if _, err := meta.Write([]byte("meta")); err != nil {
				return err
			}
			if err := meta.Close(); err != nil {
				return err
			}
			return w.End()
		}()
	}()

	var rounds int
	stats, err := Receive(r, dir, func(uint32, uint64) { rounds++ })
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("writer: %v", err)
	}
	if rounds != 1 || stats.Rounds != 1 {
		t.Errorf("got %d rounds (stats %d), want 1", rounds, stats.Rounds)
	}

	readPage := func(name string, off uint64) []byte {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		got := make([]byte, 4096)
		if _, err := f.ReadAt(got, int64(off)); err != nil {
			t.Fatalf("reading %s at %#x: %v", name, off, err)
		}
		return got
	}
	// The main MemoryFile's pages are in the memory file, others in the
	// pages file.
	if !bytes.Equal(readPage(MemoryFileName, 0), make([]byte, 4096)) {
		t.Errorf("main page 0 not zeroed")
	}
	if !bytes.Equal(readPage(MemoryFileName, 4096), page(4)) {
		t.Errorf("main page 1 has the old contents")
	}
	if !bytes.Equal(readPage(checkpointfiles.PagesFileName, privBase+4096), page(3)) {
		t.Errorf("private page has wrong contents")
	}
	state, err := os.ReadFile(filepath.Join(dir, checkpointfiles.StateFileName))
	if err != nil {
		t.Fatal(err)
	}
	if len(state) != 3<<20 {
		t.Errorf("state file: got %d bytes, want %d", len(state), 3<<20)
	}
	meta, err := os.ReadFile(filepath.Join(dir, checkpointfiles.PagesMetadataFileName))
	if err != nil || string(meta) != "meta" {
		t.Errorf("pages metadata: got %q, %v", meta, err)
	}
}

func TestIncomplete(t *testing.T) {
	r, wf := pipe(t)
	w, err := NewWriter(int(wf.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		w.MemoryFileSink(checkpoint.ResourceID{}).WritePages(0, [][]byte{make([]byte, 4096)})
		wf.Close() // No End record: the source failed.
	}()
	if _, err := Receive(r, t.TempDir(), nil); !errors.Is(err, ErrIncomplete) {
		t.Errorf("Receive: got error %v, want %v", err, ErrIncomplete)
	}
}

func TestBadMagic(t *testing.T) {
	r, wf := pipe(t)
	go func() {
		wf.Write([]byte("notastream"))
		wf.Close()
	}()
	if _, err := Receive(r, t.TempDir(), nil); err == nil {
		t.Errorf("Receive accepted a stream with a bad header")
	}
}
