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
stream's statistics as JSON on stdout.
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
	f.StringVar(&m.imagePath, "image-path", "", "directory to write the checkpoint image to; a tmpfs keeps restore fast")
	f.DurationVar(&m.timeout, "accept-timeout", 0, "give up if no stream arrives within this time (0: wait forever)")
}

// Execute implements subcommands.Command.Execute.
func (m *MigrateReceive) Execute(_ context.Context, f *flag.FlagSet, args ...any) subcommands.ExitStatus {
	if f.NArg() != 0 || m.listen == "" || m.imagePath == "" {
		f.Usage()
		return subcommands.ExitUsageError
	}
	if err := os.MkdirAll(m.imagePath, 0700); err != nil {
		util.Fatalf("creating image directory: %v", err)
	}
	ln, err := net.Listen("tcp", m.listen)
	if err != nil {
		util.Fatalf("listening: %v", err)
	}
	defer ln.Close()
	if m.timeout > 0 {
		ln.(*net.TCPListener).SetDeadline(time.Now().Add(m.timeout))
	}
	conn, err := ln.Accept()
	if err != nil {
		util.Fatalf("accepting the migration stream: %v", err)
	}
	defer conn.Close()
	ln.Close()

	start := time.Now()
	var lastRound time.Time
	stats, err := precopy.Receive(conn, m.imagePath, func(round uint32, bytes uint64) {
		lastRound = time.Now()
		log.Infof("Pre-copy round %d done: %d bytes", round, bytes)
	})
	if err != nil {
		util.Fatalf("receiving migration from %s: %v", conn.RemoteAddr(), err)
	}
	end := time.Now()
	out := struct {
		From      string  `json:"from"`
		Bytes     uint64  `json:"bytes"`
		PageBytes uint64  `json:"page_bytes"`
		ZeroBytes uint64  `json:"zero_bytes"`
		FileBytes uint64  `json:"file_bytes"`
		Rounds    int     `json:"rounds"`
		TotalMS   float64 `json:"total_ms"`
		// FinalMS is the time from the last live round's end to the end of
		// the stream: roughly, how long the source was stopped before the
		// image was complete.
		FinalMS float64 `json:"final_ms"`
	}{
		From:      conn.RemoteAddr().String(),
		Bytes:     stats.Bytes,
		PageBytes: stats.PageBytes,
		ZeroBytes: stats.ZeroBytes,
		FileBytes: stats.FileBytes,
		Rounds:    stats.Rounds,
		TotalMS:   float64(end.Sub(start).Microseconds()) / 1000,
	}
	if !lastRound.IsZero() {
		out.FinalMS = float64(end.Sub(lastRound).Microseconds()) / 1000
	}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
	return subcommands.ExitSuccess
}
