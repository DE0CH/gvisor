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

// Package precopy implements the stream used by pre-copy live migration.
//
// The source sandbox writes the stream to a connection (usually TCP) to the
// destination host, where Receive turns it into a checkpoint image directory
// that `runsc restore` can restore from:
//
//   - While the source sandbox runs, MemoryFile pages are sent (repeatedly, as
//     they are dirtied) as Pages and Zero records, addressed by their offset in
//     a pre-copy pages file (see pgalloc.PrecopyPagesFileOffset). Receive
//     stores the main MemoryFile's region (offsets below
//     pgalloc.PrecopyRegionSize) in MemoryFileName, which the restored sandbox
//     uses as its main MemoryFile's backing file, so that its pages need no
//     loading; other regions go to the pages file.
//
//   - While the source sandbox is stopped, the state file and pages metadata
//     file are sent as File records, interleaved with the last Pages and Zero
//     records, followed by an End record.
//
// The stream begins with Magic. Each record begins with a one-byte type,
// followed by little-endian fields:
//
//	Pages: offset u64, length u64, then length bytes of page contents.
//	Zero:  offset u64, length u64. The range contains only zeroes.
//	File:  file u8, length u32, then length bytes appended to that file.
//	Round: round u32, bytes u64. Informational: a pre-copy round ended.
//	End:   no fields. The image is complete.
package precopy

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/sentry/checkpoint"
	"gvisor.dev/gvisor/pkg/sentry/pgalloc"
	"gvisor.dev/gvisor/pkg/sentry/state/checkpointfiles"
	"gvisor.dev/gvisor/pkg/sync"
)

// Magic begins every stream.
const Magic = "gVprcp01"

// MetadataKey is the state file metadata key that marks an image written by
// pre-copy migration, whose pages file is laid out by MemoryFile offset.
const MetadataKey = "precopy_pages_layout"

// MetadataValue is the value of MetadataKey.
const MetadataValue = "file-offset-v1"

// MemoryFileName is the name of the file in a received image that holds the
// main MemoryFile's pages at their MemoryFile offsets.
const MemoryFileName = "memory.img"

// Record types.
const (
	recPages = 1
	recZero  = 2
	recFile  = 3
	recRound = 4
	recEnd   = 5
)

// File IDs in File records.
const (
	// FileState is the state file (checkpoint.img).
	FileState = 0
	// FilePagesMetadata is the pages metadata file (pages_meta.img).
	FilePagesMetadata = 1
)

var fileNames = [...]string{
	FileState:         checkpointfiles.StateFileName,
	FilePagesMetadata: checkpointfiles.PagesMetadataFileName,
}

// maxIovecs is the maximum number of buffers passed to one writev(2).
const maxIovecs = 1024

// Writer writes a pre-copy stream to a file descriptor. It is safe for
// concurrent use; records are written atomically with respect to each other.
type Writer struct {
	mu sync.Mutex

	// fd is the stream. Writer does not own it.
	fd int

	// bytes is the number of bytes written so far, protected by mu.
	bytes uint64

	// err is the first write error, protected by mu. Once set, all writes
	// fail with it.
	err error
}

// NewWriter returns a Writer for the stream fd, after writing the stream's
// Magic. fd must be in blocking mode.
func NewWriter(fd int) (*Writer, error) {
	w := &Writer{fd: fd}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writeLocked([][]byte{[]byte(Magic)}); err != nil {
		return nil, err
	}
	return w, nil
}

// Bytes returns the number of bytes written to the stream so far.
func (w *Writer) Bytes() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytes
}

// writeLocked writes bufs to the stream in full.
//
// Preconditions: w.mu is locked.
func (w *Writer) writeLocked(bufs [][]byte) error {
	if w.err != nil {
		return w.err
	}
	for len(bufs) != 0 {
		iovs := bufs[:min(len(bufs), maxIovecs)]
		n, err := unix.Writev(w.fd, iovs)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			w.err = fmt.Errorf("writing pre-copy stream: %w", err)
			return w.err
		}
		w.bytes += uint64(n)
		// Drop fully written buffers and advance into a partially written
		// one.
		for n > 0 {
			if n >= len(bufs[0]) {
				n -= len(bufs[0])
				bufs = bufs[1:]
			} else {
				bufs[0] = bufs[0][n:]
				n = 0
			}
		}
		for len(bufs) != 0 && len(bufs[0]) == 0 {
			bufs = bufs[1:]
		}
	}
	return nil
}

// Pages writes a Pages record for data at the given pages file offset.
func (w *Writer) Pages(off uint64, data [][]byte) error {
	var length uint64
	for _, d := range data {
		length += uint64(len(d))
	}
	var hdr [17]byte
	hdr[0] = recPages
	binary.LittleEndian.PutUint64(hdr[1:], off)
	binary.LittleEndian.PutUint64(hdr[9:], length)
	bufs := make([][]byte, 0, len(data)+1)
	bufs = append(bufs, hdr[:])
	bufs = append(bufs, data...)
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeLocked(bufs)
}

// Zero writes a Zero record.
func (w *Writer) Zero(off, length uint64) error {
	var hdr [17]byte
	hdr[0] = recZero
	binary.LittleEndian.PutUint64(hdr[1:], off)
	binary.LittleEndian.PutUint64(hdr[9:], length)
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeLocked([][]byte{hdr[:]})
}

// Round writes a Round record.
func (w *Writer) Round(round uint32, bytes uint64) error {
	var hdr [13]byte
	hdr[0] = recRound
	binary.LittleEndian.PutUint32(hdr[1:], round)
	binary.LittleEndian.PutUint64(hdr[5:], bytes)
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeLocked([][]byte{hdr[:]})
}

// End writes the End record.
func (w *Writer) End() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeLocked([][]byte{{recEnd}})
}

// fileChunk is the size of File records written by File writers.
const fileChunk = 1 << 20

// File returns a writer whose contents are sent as File records for the given
// file. Writes are buffered; the returned writer must be closed to send the
// remaining data. Closing it does not close the stream.
func (w *Writer) File(file uint8) io.WriteCloser {
	return &fileWriter{bw: bufio.NewWriterSize(fileRecordWriter{w: w, file: file}, fileChunk)}
}

type fileWriter struct {
	bw *bufio.Writer
}

// Write implements io.Writer.Write.
func (fw *fileWriter) Write(p []byte) (int, error) {
	return fw.bw.Write(p)
}

// Close implements io.Closer.Close.
func (fw *fileWriter) Close() error {
	return fw.bw.Flush()
}

// fileRecordWriter sends everything written to it as File records.
type fileRecordWriter struct {
	w    *Writer
	file uint8
}

// Write implements io.Writer.Write.
func (frw fileRecordWriter) Write(p []byte) (int, error) {
	n := 0
	for len(p) != 0 {
		chunk := p[:min(len(p), fileChunk)]
		var hdr [6]byte
		hdr[0] = recFile
		hdr[1] = frw.file
		binary.LittleEndian.PutUint32(hdr[2:], uint32(len(chunk)))
		frw.w.mu.Lock()
		err := frw.w.writeLocked([][]byte{hdr[:], chunk})
		frw.w.mu.Unlock()
		if err != nil {
			return n, err
		}
		n += len(chunk)
		p = p[len(chunk):]
	}
	return n, nil
}

// MemoryFileSink returns a pgalloc.PrecopySink that writes pages of the
// MemoryFile with the given resource ID to w.
func (w *Writer) MemoryFileSink(id checkpoint.ResourceID) pgalloc.PrecopySink {
	return &mfSink{w: w, base: pgalloc.PrecopyPagesFileOffset(id)}
}

type mfSink struct {
	w    *Writer
	base uint64
}

// WritePages implements pgalloc.PrecopySink.WritePages.
func (s *mfSink) WritePages(off uint64, data [][]byte) error {
	return s.w.Pages(s.base+off, data)
}

// WriteZero implements pgalloc.PrecopySink.WriteZero.
func (s *mfSink) WriteZero(off, length uint64) error {
	return s.w.Zero(s.base+off, length)
}

// ReceiveStats describes a received stream.
type ReceiveStats struct {
	// Bytes is the number of stream bytes read.
	Bytes uint64
	// PageBytes and ZeroBytes count the contents of Pages and Zero records.
	PageBytes uint64
	ZeroBytes uint64
	// FileBytes counts the contents of File records.
	FileBytes uint64
	// Rounds is the number of Round records.
	Rounds int
}

// ErrIncomplete is returned by Receive if the stream ends before its End
// record, which happens if migration fails on the source.
var ErrIncomplete = errors.New("pre-copy stream ended before it was complete")

// Receive reads a pre-copy stream from r and writes the checkpoint image it
// describes to dir, which must exist. onRound, if not nil, is called for every
// Round record. Receive returns when the End record has been read.
func Receive(r io.Reader, dir string, onRound func(round uint32, bytes uint64)) (stats ReceiveStats, err error) {
	cr := &countingReader{r: r}
	br := bufio.NewReaderSize(cr, 4<<20)
	defer func() { stats.Bytes = cr.n }()

	magic := make([]byte, len(Magic))
	if _, err := io.ReadFull(br, magic); err != nil {
		return stats, fmt.Errorf("reading stream header: %w", err)
	}
	if string(magic) != Magic {
		return stats, fmt.Errorf("not a pre-copy stream (header %q)", magic)
	}

	pages, err := os.OpenFile(filepath.Join(dir, checkpointfiles.PagesFileName), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return stats, err
	}
	defer pages.Close()
	memory, err := os.OpenFile(filepath.Join(dir, MemoryFileName), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return stats, err
	}
	defer memory.Close()
	// fileAt returns the file and offset in it for a pre-copy pages file
	// offset.
	fileAt := func(off uint64) (*os.File, uint64) {
		if off < pgalloc.PrecopyRegionSize {
			return memory, off
		}
		return pages, off
	}
	var files [len(fileNames)]*os.File
	for i, name := range fileNames {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return stats, err
		}
		defer f.Close()
		files[i] = f
	}

	buf := make([]byte, 4<<20)
	var hdr [16]byte
	for {
		typ, err := br.ReadByte()
		if err == io.EOF {
			return stats, ErrIncomplete
		}
		if err != nil {
			return stats, err
		}
		switch typ {
		case recPages:
			if _, err := io.ReadFull(br, hdr[:16]); err != nil {
				return stats, unexpectedEOF(err)
			}
			off := binary.LittleEndian.Uint64(hdr[0:])
			length := binary.LittleEndian.Uint64(hdr[8:])
			stats.PageBytes += length
			f, foff := fileAt(off)
			for length != 0 {
				n := min(length, uint64(len(buf)))
				if _, err := io.ReadFull(br, buf[:n]); err != nil {
					return stats, unexpectedEOF(err)
				}
				if _, err := f.WriteAt(buf[:n], int64(foff)); err != nil {
					return stats, err
				}
				foff += n
				off += n
				length -= n
			}
		case recZero:
			if _, err := io.ReadFull(br, hdr[:16]); err != nil {
				return stats, unexpectedEOF(err)
			}
			off := binary.LittleEndian.Uint64(hdr[0:])
			length := binary.LittleEndian.Uint64(hdr[8:])
			stats.ZeroBytes += length
			f, foff := fileAt(off)
			if err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, int64(foff), int64(length)); err != nil {
				return stats, fmt.Errorf("punching hole at %#x, length %#x: %w", off, length, err)
			}
		case recFile:
			if _, err := io.ReadFull(br, hdr[:5]); err != nil {
				return stats, unexpectedEOF(err)
			}
			file := hdr[0]
			length := binary.LittleEndian.Uint32(hdr[1:])
			if int(file) >= len(files) {
				return stats, fmt.Errorf("invalid file %d in pre-copy stream", file)
			}
			if _, err := io.CopyN(files[file], br, int64(length)); err != nil {
				return stats, unexpectedEOF(err)
			}
			stats.FileBytes += uint64(length)
		case recRound:
			if _, err := io.ReadFull(br, hdr[:12]); err != nil {
				return stats, unexpectedEOF(err)
			}
			stats.Rounds++
			if onRound != nil {
				onRound(binary.LittleEndian.Uint32(hdr[0:]), binary.LittleEndian.Uint64(hdr[4:]))
			}
		case recEnd:
			return stats, nil
		default:
			return stats, fmt.Errorf("invalid record type %d in pre-copy stream", typ)
		}
	}
}

func unexpectedEOF(err error) error {
	if err == io.EOF {
		return ErrIncomplete
	}
	return err
}

type countingReader struct {
	r io.Reader
	n uint64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += uint64(n)
	return n, err
}
