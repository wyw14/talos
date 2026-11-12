// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/siderolabs/gen/channel"
	"github.com/siderolabs/go-talos-support/support/bundle"
	"github.com/siderolabs/go-talos-support/support/collectors"
	"golang.org/x/sync/errgroup"
)

// RunResult summarizes one collection pass over the collector set.
type RunResult struct {
	// Partial is true when at least one collector is missing, empty-with-no-seal or in error
	// state. The final archive can still be assembled, but it must not be presented as complete.
	Partial bool
	Sealed  int
	Empty   int
	Errors  int
	Records []ArtifactRecord
}

// Engine runs resumable collector passes against one workspace.
type Engine struct {
	config  RunConfig
	options *Options
	store   *Store
}

// NewEngine opens the workspace and validates the checkpoint against cfg.
func NewEngine(cfg RunConfig, opts ...Option) (*Engine, error) {
	options := NewOptions(opts...)

	store, err := OpenStore(cfg, options)
	if err != nil {
		return nil, err
	}

	return &Engine{
		config:  cfg,
		options: options,
		store:   store,
	}, nil
}

// Store exposes the underlying workspace store, mainly for testing and assembly.
func (e *Engine) Store() *Store {
	return e.store
}

// Run executes every collector which does not have a reusable sealed artifact. Reusable
// collectors are never invoked: their collect functions are not called at all.
//
// Individual collector failures are non-fatal, matching the default support flow: they are
// recorded as error artifacts and reported through the progress channel. Fatal errors are
// context cancellation and exhausted workspace budgets, which make further collection impossible.
func (e *Engine) Run(ctx context.Context, cols []*collectors.Collector) (*RunResult, error) {
	if err := e.validateCollectors(cols); err != nil {
		return nil, err
	}

	totals := calculateTotals(cols)
	tracker := NewPathTracker()
	tasks := make(chan int)

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(e.options.NumWorkers)

	for range e.options.NumWorkers {
		eg.Go(func() error {
			for {
				select {
				case ordinal, ok := <-tasks:
					if !ok {
						return nil
					}

					if err := e.runOne(ctx, ordinal, cols[ordinal], tracker, totals); err != nil {
						return err
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	for ordinal := range cols {
		if !channel.SendWithContext(ctx, tasks, ordinal) {
			break
		}
	}

	close(tasks)

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return e.result(), nil
}

// runOne handles one collector: resume decision, execution and state persistence.
func (e *Engine) runOne(ctx context.Context, ordinal int, col *collectors.Collector,
	tracker *PathTracker, totals map[string]int,
) error {
	identity := e.config.Collectors[ordinal]

	if record, ok := e.store.Record(ordinal); ok && e.store.Reusable(record) {
		e.reportProgress(bundle.Progress{
			Source: identity.Source,
			State:  "restored from sealed artifact",
			Total:  totals[identity.Source],
		})

		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	startedAt := e.options.Now()

	part, err := e.store.CreatePart(ordinal)
	if err != nil {
		// budget exhaustion is fatal: no further collector can make progress in this workspace
		return fmt.Errorf("collector %d (%s): %w", ordinal, identity.Name, err)
	}

	archive := &captureArchive{
		store:   e.store,
		part:    part,
		tracker: tracker,
	}

	runErr := col.Run(archive)

	finishedAt := e.options.Now()

	switch {
	case runErr != nil:
		if recordErr := e.store.RecordError(identity, startedAt, finishedAt, part, runErr); recordErr != nil {
			return recordErr
		}

		e.reportProgress(bundle.Progress{
			Error:  runErr,
			Source: identity.Source,
			State:  identity.Name,
			Total:  totals[identity.Source],
		})

		// a canceled/deadlined context is an interruption of the whole run, not a collector
		// failure: the error is durably recorded above, then the run aborts so the caller can
		// resume from the sealed artifacts
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	case part.writes == 0:
		e.store.Abort(part)

		if recordErr := e.store.RecordEmpty(identity, startedAt, finishedAt); recordErr != nil {
			return recordErr
		}

		e.reportProgress(bundle.Progress{
			Source: identity.Source,
			State:  identity.Name,
			Total:  totals[identity.Source],
		})
	default:
		if _, sealErr := e.store.Seal(part, identity, startedAt, finishedAt); sealErr != nil {
			return fmt.Errorf("collector %d (%s): %w", ordinal, identity.Name, sealErr)
		}

		e.reportProgress(bundle.Progress{
			Source: identity.Source,
			State:  identity.Name,
			Total:  totals[identity.Source],
		})
	}

	return nil
}

func (e *Engine) validateCollectors(cols []*collectors.Collector) error {
	if len(cols) != len(e.config.Collectors) {
		return fmt.Errorf("%w: checkpoint expects %d collectors, got %d",
			ErrCollectorSetMismatch, len(e.config.Collectors), len(cols))
	}

	for i, col := range cols {
		identity := e.config.Collectors[i]
		if col.Source() != identity.Source || col.String() != identity.Name {
			return fmt.Errorf("%w: ordinal %d identity changed", ErrCollectorSetMismatch, i)
		}
	}

	return nil
}

func (e *Engine) reportProgress(progress bundle.Progress) {
	if e.options.Progress == nil {
		return
	}

	// non-blocking best effort, mirroring the upstream engine which drops progress when the
	// consumer is gone
	select {
	case e.options.Progress <- progress:
	default:
	}
}

func (e *Engine) result() *RunResult {
	cp := e.store.Snapshot()
	result := &RunResult{Records: append([]ArtifactRecord(nil), cp.Artifacts...)}

	for _, record := range cp.Artifacts {
		switch record.Status {
		case StatusSealed:
			result.Sealed++
		case StatusEmpty:
			result.Empty++
		case StatusError:
			result.Errors++
		}
	}

	result.Partial = result.Errors > 0 || len(cp.Artifacts) != len(e.config.Collectors)

	return result
}

func calculateTotals(cols []*collectors.Collector) map[string]int {
	totals := map[string]int{}

	for _, col := range cols {
		totals[col.Source()]++
	}

	return totals
}

// captureArchive redirects a collector's single Archive.Write into its private part, enforcing
// the run-wide path registry while doing so.
type captureArchive struct {
	store   *Store
	part    *Part
	tracker *PathTracker
}

func (a *captureArchive) Write(entryPath string, contents []byte) error {
	// a collector owns exactly one archive entry: a second write can only mean a contract
	// violation that would silently concatenate payloads, so reject it explicitly
	if a.part.writes > 0 {
		return fmt.Errorf("%w: collector %d wrote more than once", ErrMultipleWrites, a.part.ordinal)
	}

	if path.Clean(strings.ReplaceAll(entryPath, `\`, "/")) == ManifestName {
		return fmt.Errorf("%w: %q is reserved for the capture manifest", ErrPathDuplicate, entryPath)
	}

	if err := a.tracker.Register(entryPath); err != nil {
		return err
	}

	return a.part.Write(a.store, entryPath, contents)
}

func (a *captureArchive) Close() error {
	return nil
}

// ErrCollectorSetMismatch is returned when the live collector set no longer matches the identities
// captured in the run configuration.
var ErrCollectorSetMismatch = errors.New("collector set does not match the run configuration")

// ErrMultipleWrites is returned when a collector writes more than once into its capture archive.
var ErrMultipleWrites = errors.New("collector invoked archive write more than once")

// compile-time check that captureArchive is a bundle.Archive.
var _ bundle.Archive = (*captureArchive)(nil)
