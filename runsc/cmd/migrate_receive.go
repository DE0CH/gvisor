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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/google/subcommands"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"gvisor.dev/gvisor/pkg/log"
	"gvisor.dev/gvisor/pkg/sentry/state/precopy"
	"gvisor.dev/gvisor/runsc/cmd/util"
	"gvisor.dev/gvisor/runsc/config"
	"gvisor.dev/gvisor/runsc/flag"
)

// MigrateReceive implements subcommands.Command for the "migrate-receive"
// command.
type MigrateReceive struct {
	listen    string
	imagePath string
	timeout   time.Duration
}

// Name implements subcommands.Command.Name.
func (*MigrateReceive) Name() string {
	return "migrate-receive"
}

// Synopsis implements subcommands.Command.Synopsis.
func (*MigrateReceive) Synopsis() string {
	return "receive a container migrated with `checkpoint --migrate-to` into a checkpoint image (experimental)"
}

// Usage implements subcommands.Command.Usage.
func (*MigrateReceive) Usage() string {
	return `migrate-receive --listen <host:port> --image-path <dir> - accept one pre-copy migration stream and write the checkpoint image it carries to <dir>.

When it exits successfully, restore the container with
"runsc restore --image-path <dir> --background" (the same flags as the source). It prints the
stream's statistics as JSON on stdout. "runsc restore --migrate-listen" does both in one step, and
creates the sandbox while the stream is still arriving.
`
}

// FetchSpec implements util.SubCommand.FetchSpec.
func (*MigrateReceive) FetchSpec(*config.Config, *flag.FlagSet) (string, *specs.Spec, error) {
	// Not associated with a container.
	return "", nil, nil
}

// SetFlags implements subcommands.Command.SetFlags.
func (m *MigrateReceive) SetFlags(f *flag.FlagSet) {
	f.StringVar(&m.listen, "listen", "", "address to listen on for the migration stream, host:port")
	f.StringVar(&m.imagePath, "image-path", "", "directory to write the checkpoint image to. Its pages file is sparse and up to 2^63 bytes long, so this must be on tmpfs (or XFS, btrfs); tmpfs also keeps restore fast")
	f.DurationVar(&m.timeout, "accept-timeout", 0, "give up if no stream arrives within this time (0: wait forever)")
}

// Execute implements subcommands.Command.Execute.
func (m *MigrateReceive) Execute(_ context.Context, f *flag.FlagSet, args ...any) subcommands.ExitStatus {
	if f.NArg() != 0 || m.listen == "" || m.imagePath == "" {
		f.Usage()
		return subcommands.ExitUsageError
	}
	ln, err := net.Listen("tcp", m.listen)
	if err != nil {
		util.Fatalf("listening: %v", err)
	}
	stats, err := receiveMigration(ln, m.imagePath, m.timeout)
	if err != nil {
		util.Fatalf("%v", err)
	}
	b, _ := json.Marshal(stats)
	fmt.Println(string(b))
	return subcommands.ExitSuccess
}

// migrationStats describes a received pre-copy migration stream.
type migrationStats struct {
	From      string  `json:"from"`
	Bytes     uint64  `json:"bytes"`
	PageBytes uint64  `json:"page_bytes"`
	ZeroBytes uint64  `json:"zero_bytes"`
	FileBytes uint64  `json:"file_bytes"`
	Rounds    int     `json:"rounds"`
	TotalMS   float64 `json:"total_ms"`
	// FinalMS is the time from the end of the last live round to the end of
	// the stream: roughly, how long the source was stopped before the image
	// was complete.
	FinalMS float64 `json:"final_ms"`
	// RestoreMS is set by `runsc restore --migrate-listen`: the time from the
	// end of the stream until the container was restored.
	RestoreMS float64 `json:"restore_ms,omitempty"`

	// end is when the stream ended.
	end time.Time
}

func msSince(t, start time.Time) float64 {
	return float64(t.Sub(start).Microseconds()) / 1000
}

// receiveMigration accepts one pre-copy migration stream on ln, writes the
// checkpoint image it carries to dir, and closes ln.
func receiveMigration(ln net.Listener, dir string, acceptTimeout time.Duration) (*migrationStats, error) {
	defer ln.Close()
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("creating image directory: %w", err)
	}
	if acceptTimeout > 0 {
		if tl, ok := ln.(*net.TCPListener); ok {
			tl.SetDeadline(time.Now().Add(acceptTimeout))
		}
	}
	conn, err := ln.Accept()
	if err != nil {
		return nil, fmt.Errorf("accepting the migration stream: %w", err)
	}
	defer conn.Close()
	ln.Close()

	start := time.Now()
	var lastRound time.Time
	rs, err := precopy.Receive(conn, dir, func(round uint32, bytes uint64) {
		lastRound = time.Now()
		log.Infof("Pre-copy round %d done: %d bytes", round, bytes)
	})
	if err != nil {
		return nil, fmt.Errorf("receiving migration from %s: %w", conn.RemoteAddr(), err)
	}
	stats := &migrationStats{
		From:      conn.RemoteAddr().String(),
		Bytes:     rs.Bytes,
		PageBytes: rs.PageBytes,
		ZeroBytes: rs.ZeroBytes,
		FileBytes: rs.FileBytes,
		Rounds:    rs.Rounds,
		end:       time.Now(),
	}
	stats.TotalMS = msSince(stats.end, start)
	if !lastRound.IsZero() {
		stats.FinalMS = msSince(stats.end, lastRound)
	}
	return stats, nil
}
