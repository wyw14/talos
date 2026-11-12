// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package capture implements the optional resumable support bundle collection mode.
//
// Instead of streaming collector results straight into the final archive, the capture engine
// persists every collector result as a separately sealed, content-addressed artifact inside a
// local workspace, together with a checkpoint describing the collection inputs and the state of
// every artifact. The final archive is assembled from the sealed artifacts in a single pass, so
// an interrupted run only has to redo the collectors which never reached a sealed state.
package capture

import (
	"io"
	"time"

	"github.com/siderolabs/go-talos-support/support/bundle"
)

// FormatVersion is the version of the on-disk checkpoint/workspace layout understood by this
// package. Any change to the layout or the fingerprint semantics bumps it, which makes older
// checkpoints fail loading and get discarded.
const FormatVersion = 1

const (
	defaultTTL               = 30 * time.Minute
	defaultMaxCollectorBytes = int64(512) * 1024 * 1024
	defaultMaxDiskBytes      = int64(2) * 1024 * 1024 * 1024
	defaultNumWorkers        = 1

	checkpointName = "checkpoint.json"
	// checkpointTempName is created next to checkpointName so the final rename is same-directory.
	checkpointTempName = "checkpoint.json.tmp"
	chunksDirName      = "chunks"

	// ManifestName is the name of the manifest entry written into the final archive.
	ManifestName = "capture-manifest.json"
)

// Options configures the capture engine.
//
// The zero value is not usable; use NewOptions. Limits and TTL are not part of the collection
// input fingerprint, so changing them neither invalidates sealed artifacts nor changes their
// contents.
type Options struct {
	// WorkspaceDir holds the checkpoint and the plaintext chunk files. It must be a directory
	// dedicated to this run; the engine only ever removes files it created inside it.
	WorkspaceDir string

	// TTL bounds how long a sealed artifact remains reusable across runs.
	TTL time.Duration

	// MaxCollectorBytes is the hard upper bound for a single collector result.
	MaxCollectorBytes int64

	// MaxDiskBytes is the hard upper bound for the total size of all chunks in the workspace.
	MaxDiskBytes int64

	// NumWorkers is the number of collectors executed concurrently.
	NumWorkers int

	// Progress, when non-nil, receives the same progress events the default support flow emits.
	Progress chan bundle.Progress

	// LogOutput receives human-readable operational messages (checkpoint discard, resume, ...).
	LogOutput io.Writer

	// Now returns the current time. It is an option so tests can exercise TTL behavior
	// deterministically.
	Now func() time.Time
}

// Option configures Options.
type Option func(*Options)

// NewOptions builds Options with safe defaults and applies the given overrides.
func NewOptions(opts ...Option) *Options {
	options := &Options{
		TTL:               defaultTTL,
		MaxCollectorBytes: defaultMaxCollectorBytes,
		MaxDiskBytes:      defaultMaxDiskBytes,
		NumWorkers:        defaultNumWorkers,
		Now:               time.Now,
	}

	for _, opt := range opts {
		opt(options)
	}

	return options
}

// WithWorkspaceDir sets the workspace directory.
func WithWorkspaceDir(dir string) Option {
	return func(o *Options) {
		o.WorkspaceDir = dir
	}
}

// WithTTL sets the sealed artifact time-to-live.
func WithTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.TTL = ttl
	}
}

// WithMaxCollectorBytes sets the per-collector byte limit.
func WithMaxCollectorBytes(limit int64) Option {
	return func(o *Options) {
		o.MaxCollectorBytes = limit
	}
}

// WithMaxDiskBytes sets the workspace total byte budget.
func WithMaxDiskBytes(limit int64) Option {
	return func(o *Options) {
		o.MaxDiskBytes = limit
	}
}

// WithNumWorkers sets the collector concurrency.
func WithNumWorkers(count int) Option {
	return func(o *Options) {
		o.NumWorkers = count
	}
}

// WithProgressChan sets the progress channel.
func WithProgressChan(progress chan bundle.Progress) Option {
	return func(o *Options) {
		o.Progress = progress
	}
}

// WithLogOutput sets the log sink.
func WithLogOutput(w io.Writer) Option {
	return func(o *Options) {
		o.LogOutput = w
	}
}

// WithClock overrides the time source.
func WithClock(now func() time.Time) Option {
	return func(o *Options) {
		o.Now = now
	}
}
