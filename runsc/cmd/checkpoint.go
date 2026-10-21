// Copyright 2018 The gVisor Authors.
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
	"fmt"
	"net"
	"os"
	"time"

	"github.com/google/subcommands"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/log"
	"gvisor.dev/gvisor/pkg/sentry/control/controlapi"
	"gvisor.dev/gvisor/pkg/sentry/state/checkpointfiles"
	"gvisor.dev/gvisor/pkg/state/statefile"
	"gvisor.dev/gvisor/runsc/cmd/util"
	"gvisor.dev/gvisor/runsc/config"
	"gvisor.dev/gvisor/runsc/container"
	"gvisor.dev/gvisor/runsc/flag"
	"gvisor.dev/gvisor/runsc/sandbox"
)

// Checkpoint implements subcommands.Command for the "checkpoint" command.
type Checkpoint struct {
	containerLoader
	imagePath                 string
	leaveRunning              bool
	compression               CheckpointCompression
	excludeCommittedZeroPages bool
	cudaCheckpointPath        string
	cudaCheckpointSequential  bool
	saveRestoreExecArgv       string
	saveRestoreExecTimeout    time.Duration
	splitFSCheckpointPaths    pathVar

	// direct indicates whether O_DIRECT should be used for writing the
	// checkpoint pages file. It bypasses the kernel page cache. It is beneficial
	// if the checkpoint files are not expected to be read again on this host.
	// For example, if the checkpoint files will be stored on a network block
	// device, which will be detached after the checkpoint is done.
	direct bool

	// migrateTo, if set, is the host:port of a `runsc migrate-receive` to
	// which the container is migrated with pre-copy live migration, instead
	// of being saved to imagePath.
	migrateTo          string
	migrateDowntime    time.Duration
	migrateMaxRounds   int
	migrateMaxDowntime time.Duration
}

// Name implements subcommands.Command.Name.
func (*Checkpoint) Name() string {
	return "checkpoint"
}

// Synopsis implements subcommands.Command.Synopsis.
func (*Checkpoint) Synopsis() string {
	return "checkpoint current state of container (experimental)"
}

// Usage implements subcommands.Command.Usage.
func (*Checkpoint) Usage() string {
	return "checkpoint [flags] <container id> - save current state of container.\n"
}

// SetFlags implements subcommands.Command.SetFlags.
func (c *Checkpoint) SetFlags(f *flag.FlagSet) {
	f.StringVar(&c.imagePath, "image-path", "", "directory path to saved container image")
	f.BoolVar(&c.leaveRunning, "leave-running", false, "restart the container after checkpointing")
	f.Var(newCheckpointCompressionValue(statefile.CompressionLevelDefault, &c.compression), "compression", "compress checkpoint image on disk. Values: none|flate-best-speed.")
	f.BoolVar(&c.excludeCommittedZeroPages, "exclude-committed-zero-pages", false, "exclude committed zero-filled pages from checkpoint")
	f.BoolVar(&c.direct, "direct", false, "use O_DIRECT for writing checkpoint pages file")
	f.StringVar(&c.cudaCheckpointPath, "cuda-checkpoint-path", "", "path to the cuda-checkpoint binary in the container")
	f.BoolVar(&c.cudaCheckpointSequential, "cuda-checkpoint-sequential", false, "run cuda-checkpoint sequentially in the container")
	f.StringVar(&c.saveRestoreExecArgv, "save-restore-exec-argv", "", "argv (split by spaces) for a save/restore binary that's automatically executed in the sandbox before saving and after restoring. If the execution fails, the save/restore process will fail.")
	f.DurationVar(&c.saveRestoreExecTimeout, "save-restore-exec-timeout", controlapi.DefaultSaveRestoreExecTimeout, "timeout for the binary pointed to by save-restore-exec-argv.")
	// Must match the default prefix applied by sandbox.Sandbox.Checkpoint.
	c.splitFSCheckpointPaths.defaultPrefix = checkpointfiles.FSCheckpointDir
	f.Var(&c.splitFSCheckpointPaths, "fs-checkpoint-paths", `[<prefix>=][<container>:]<path> target to include in the filesystem checkpoint (can be repeated). <prefix> defaults to "fs". The files are written to --image-path as <prefix>_*, or into a <prefix>/ subdirectory if <prefix> ends with "/". Paths containing "=" require an explicit prefix. For capturing all of tmpfs, the path should be "all-tmpfs".`)

	f.StringVar(&c.migrateTo, "migrate-to", "", "host:port of a `runsc migrate-receive` on the destination host. Instead of saving to --image-path, the container's memory is copied there while it keeps running (pre-copy live migration), then it is stopped and the rest of its state is sent. Requires the systrap or ptrace platform.")
	f.DurationVar(&c.migrateDowntime, "migrate-downtime", 100*time.Millisecond, "with --migrate-to: target duration for sending the last dirty memory pages while the container is stopped.")
	f.IntVar(&c.migrateMaxRounds, "migrate-max-rounds", 20, "with --migrate-to: maximum number of pre-copy rounds after the first full copy.")
	f.DurationVar(&c.migrateMaxDowntime, "migrate-max-downtime", 0, "with --migrate-to: if non-zero, abort the migration and keep the container running here instead of stopping it when the last dirty pages are estimated to take longer than this to send (e.g. it rewrites memory about as fast as the link sends).")

	// Unimplemented flags necessary for compatibility with docker.
	var wp string
	f.StringVar(&wp, "work-path", "", "ignored")
}

// FetchSpec implements util.SubCommand.FetchSpec.
func (c *Checkpoint) FetchSpec(conf *config.Config, f *flag.FlagSet) (string, *specs.Spec, error) {
	cont, err := c.loadContainer(conf, f, container.LoadOpts{})
	if err != nil {
		return "", nil, fmt.Errorf("loading container: %w", err)
	}
	return cont.ID, cont.Spec, nil
}

// Execute implements subcommands.Command.Execute.
func (c *Checkpoint) Execute(_ context.Context, f *flag.FlagSet, args ...any) subcommands.ExitStatus {
	if f.NArg() != 1 {
		f.Usage()
		return subcommands.ExitUsageError
	}

	conf := args[0].(*config.Config)

	cont, err := c.loadContainer(conf, f, container.LoadOpts{})
	if err != nil {
		util.Fatalf("loading container: %v", err)
	}

	if c.migrateTo != "" {
		if err := c.migrate(conf, cont); err != nil {
			util.Fatalf("migration failed: %v", err)
		}
		return subcommands.ExitSuccess
	}

	if c.imagePath == "" {
		util.Fatalf("image-path flag must be provided")
	}

	if err := os.MkdirAll(c.imagePath, 0755); err != nil {
		util.Fatalf("making directories at path provided: %v", err)
	}

	opts := sandbox.CheckpointOpts{
		Compression:                c.compression.Level(),
		Resume:                     c.leaveRunning,
		Direct:                     c.direct,
		ExcludeCommittedZeroPages:  c.excludeCommittedZeroPages,
		CudaCheckpointPath:         c.cudaCheckpointPath,
		CudaCheckpointSequential:   c.cudaCheckpointSequential,
		SaveRestoreExecArgv:        c.saveRestoreExecArgv,
		SaveRestoreExecTimeout:     c.saveRestoreExecTimeout,
		SaveRestoreExecContainerID: cont.ID,
		SplitFSCheckpointPaths:     c.splitFSCheckpointPaths.targets,
	}

	if err := cont.Checkpoint(conf, c.imagePath, opts); err != nil {
		util.Fatalf("checkpoint failed: %v", err)
	}

	return subcommands.ExitSuccess
}

// migrate migrates cont to c.migrateTo with pre-copy live migration.
func (c *Checkpoint) migrate(conf *config.Config, cont *container.Container) error {
	switch conf.Platform {
	case "systrap", "ptrace":
	default:
		// Other platforms map MemoryFile pages in ways that dirty tracking
		// doesn't cover (see pgalloc/dirty.go).
		return fmt.Errorf("pre-copy migration is not supported on platform %q", conf.Platform)
	}
	if c.imagePath != "" || c.leaveRunning || len(c.splitFSCheckpointPaths.targets) > 0 {
		return fmt.Errorf("--migrate-to can't be combined with --image-path, --leave-running or --fs-checkpoint-paths")
	}
	conn, err := net.DialTimeout("tcp", c.migrateTo, 10*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := conn.(*net.TCPConn).File()
	if err != nil {
		return err
	}
	defer stream.Close()
	// The sentry writes to the stream with blocking writes.
	if err := unix.SetNonblock(int(stream.Fd()), false); err != nil {
		return err
	}
	start := time.Now()
	opts := sandbox.CheckpointOpts{
		// The state file is sent over the network uncompressed: compression
		// is slower than the network within a region.
		Compression:                statefile.CompressionLevelNone,
		CudaCheckpointPath:         c.cudaCheckpointPath,
		CudaCheckpointSequential:   c.cudaCheckpointSequential,
		SaveRestoreExecArgv:        c.saveRestoreExecArgv,
		SaveRestoreExecTimeout:     c.saveRestoreExecTimeout,
		SaveRestoreExecContainerID: cont.ID,
		Precopy: &sandbox.PrecopyOpts{
			Stream:      stream,
			Downtime:    c.migrateDowntime,
			MaxRounds:   c.migrateMaxRounds,
			MaxDowntime: c.migrateMaxDowntime,
		},
	}
	if err := cont.Checkpoint(conf, "", opts); err != nil {
		return err
	}
	log.Infof("Migrated container %q to %s in %s", cont.ID, c.migrateTo, time.Since(start))
	return nil
}

// CheckpointCompression represents checkpoint image writer behavior. The
// default behavior is to compress because the default behavior used to be to
// always compress.
type CheckpointCompression statefile.CompressionLevel

func newCheckpointCompressionValue(val statefile.CompressionLevel, p *CheckpointCompression) *CheckpointCompression {
	*p = CheckpointCompression(val)
	return (*CheckpointCompression)(p)
}

// Set implements flag.Value.
func (g *CheckpointCompression) Set(v string) error {
	t, err := statefile.CompressionLevelFromString(v)
	if err != nil {
		return fmt.Errorf("invalid checkpoint compression type %q", v)
	}

	*g = CheckpointCompression(t)

	return nil
}

// Get implements flag.Getter.
func (g *CheckpointCompression) Get() any {
	return *g
}

// String implements flag.Value.
func (g CheckpointCompression) String() string {
	return string(g)
}

// Level returns corresponding statefile.CompressionLevel value.
func (g CheckpointCompression) Level() statefile.CompressionLevel {
	return statefile.CompressionLevel(g)
}
