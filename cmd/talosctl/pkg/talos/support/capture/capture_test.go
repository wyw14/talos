// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/siderolabs/go-talos-support/support/bundle"
	"github.com/siderolabs/go-talos-support/support/collectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/siderolabs/talos/cmd/talosctl/pkg/talos/support/capture"
)

type fakeCollector struct {
	path   string
	source string
	data   []byte
	err    error
	calls  atomic.Int32
}

func (f *fakeCollector) build() *collectors.Collector {
	col := collectors.NewCollector(f.path, func() ([]byte, error) {
		f.calls.Add(1)

		if f.err != nil {
			return nil, f.err
		}

		return f.data, nil
	})

	if f.source != "" {
		col = collectors.WithSource([]*collectors.Collector{col}, f.source)[0]
	}

	return col
}

func buildSet(fakes ...*fakeCollector) ([]*collectors.Collector, []*fakeCollector) {
	cols := make([]*collectors.Collector, 0, len(fakes))
	for _, f := range fakes {
		cols = append(cols, f.build())
	}

	return cols, fakes
}

func configFor(cols []*collectors.Collector) capture.RunConfig {
	return capture.BuildRunConfig(cols, []string{"node-1"}, nil, false,
		map[string]bool{"node-1": true}, capture.EncryptionConfig{Enabled: false})
}

func TestRunSealsAndResumesWithoutRecalling(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()

	cols, fakes := buildSet(
		&fakeCollector{path: "a.log", data: []byte("aaa")},
		&fakeCollector{path: "b.log", source: "node-1", data: []byte("bbb")},
		&fakeCollector{path: "c.log", data: []byte("ccc")},
	)

	cfg := configFor(cols)

	engine, err := capture.NewEngine(cfg,
		capture.WithWorkspaceDir(workspace), capture.WithNumWorkers(2))
	require.NoError(t, err)

	result, err := engine.Run(context.Background(), cols)
	require.NoError(t, err)
	assert.False(t, result.Partial)
	assert.Equal(t, 3, result.Sealed)

	for _, f := range fakes {
		assert.Equal(t, int32(1), f.calls.Load(), "collector %s should run once", f.path)
	}

	// second run against the same workspace: everything is reusable
	engine2, err := capture.NewEngine(cfg, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	result2, err := engine2.Run(context.Background(), cols)
	require.NoError(t, err)
	assert.False(t, result2.Partial)

	for _, f := range fakes {
		assert.Equal(t, int32(1), f.calls.Load(), "sealed collector %s must not be recalled", f.path)
	}
}

func TestRunInterruptionResumesOnlyUnfinished(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()

	var (
		blockedCalls atomic.Int32
		block        = make(chan struct{})
	)

	fast1 := &fakeCollector{path: "a.log", data: []byte("aaa")}
	fast2 := &fakeCollector{path: "b.log", data: []byte("bbb")}

	cols, _ := buildSet(fast1, fast2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cols = append(cols, collectors.NewCollector("blocked.log", func() ([]byte, error) {
		blockedCalls.Add(1)

		select {
		case <-block:
			return []byte("done"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))

	cfg := configFor(cols)
	progress := make(chan bundle.Progress, 8)

	engine, err := capture.NewEngine(cfg,
		capture.WithWorkspaceDir(workspace),
		capture.WithProgressChan(progress),
		capture.WithNumWorkers(1))
	require.NoError(t, err)

	// run the engine in the background, then wait for the two fast collectors to be sealed and
	// kill the run like a process interruption
	runErr := make(chan error, 1)

	go func() {
		_, runError := engine.Run(ctx, cols)
		runErr <- runError
	}()

	sealed := 0
	for sealed < 2 {
		select {
		case p := <-progress:
			if p.Error == nil {
				sealed++
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for fast collectors")
		}
	}

	cancel()

	err = <-runErr
	require.Error(t, err)

	assert.Equal(t, int32(1), fast1.calls.Load())
	assert.Equal(t, int32(1), fast2.calls.Load())
	assert.GreaterOrEqual(t, blockedCalls.Load(), int32(1))

	// resume in a new engine/process-like context with the blocker released
	close(block)

	repaired := make([]*collectors.Collector, 0, len(cols))
	repaired = append(repaired, cols[:2]...)
	repaired = append(repaired, collectors.NewCollector("blocked.log", func() ([]byte, error) {
		blockedCalls.Add(1)

		return []byte("done"), nil
	}))

	cfg2 := configFor(repaired)
	require.Equal(t, mustFingerprint(t, cfg), mustFingerprint(t, cfg2))

	engine2, err := capture.NewEngine(cfg2, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	result, err := engine2.Run(context.Background(), repaired)
	require.NoError(t, err)
	assert.False(t, result.Partial)
	assert.Equal(t, 3, result.Sealed)

	assert.Equal(t, int32(1), fast1.calls.Load(), "sealed collector must not rerun")
	assert.Equal(t, int32(1), fast2.calls.Load(), "sealed collector must not rerun")
	assert.Equal(t, int32(2), blockedCalls.Load(), "interrupted collector must rerun once")
}

func TestRunEndFailureIsRecordedAndCanBeRepaired(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()

	ok1 := &fakeCollector{path: "a.log", data: []byte("aaa")}
	ok2 := &fakeCollector{path: "b.log", data: []byte("bbb")}
	bad := &fakeCollector{path: "c.log", err: stringError("collector exploded")}

	cols, _ := buildSet(ok1, ok2, bad)
	cfg := configFor(cols)

	engine, err := capture.NewEngine(cfg, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	result, err := engine.Run(context.Background(), cols)
	require.NoError(t, err, "collector errors stay non-fatal")
	assert.True(t, result.Partial)
	assert.Equal(t, 1, result.Errors)
	assert.Equal(t, int32(1), bad.calls.Load())

	// repair: same identity, healthy implementation
	repairedSet, repairedFakes := buildSet(ok1, ok2, &fakeCollector{path: "c.log", data: []byte("ccc")})
	cfg2 := configFor(repairedSet)
	require.Equal(t, mustFingerprint(t, cfg), mustFingerprint(t, cfg2))

	engine2, err := capture.NewEngine(cfg2, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	result2, err := engine2.Run(context.Background(), repairedSet)
	require.NoError(t, err)
	assert.False(t, result2.Partial)
	assert.Equal(t, 0, result2.Errors)

	assert.Equal(t, int32(1), ok1.calls.Load())
	assert.Equal(t, int32(1), ok2.calls.Load())
	assert.Equal(t, int32(1), repairedFakes[2].calls.Load(), "only the failed collector reruns")
}

func TestRunRejectsUnsafeAndDuplicatePaths(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()

	cols := []*collectors.Collector{
		collectors.NewCollector("good.log", func() ([]byte, error) { return []byte("ok"), nil }),
		collectors.NewCollector("../evil.log", func() ([]byte, error) { return []byte("evil"), nil }),
		collectors.NewCollector("dup.log", func() ([]byte, error) { return []byte("first"), nil }),
		collectors.NewCollector("dup.log", func() ([]byte, error) { return []byte("second"), nil }),
	}

	cfg := configFor(cols)

	engine, err := capture.NewEngine(cfg, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	result, err := engine.Run(context.Background(), cols)
	require.NoError(t, err)
	assert.True(t, result.Partial)
	assert.Equal(t, 2, result.Errors)

	// nothing escaped the workspace
	_, err = os.Stat(filepath.Join(filepath.Dir(workspace), "evil.log"))
	assert.ErrorIs(t, err, os.ErrNotExist)

	require.Len(t, result.Records, 4)
	assert.Equal(t, capture.StatusSealed, result.Records[0].Status)
	assert.Equal(t, capture.StatusError, result.Records[1].Status)
	assert.Equal(t, capture.StatusSealed, result.Records[2].Status)
	assert.Equal(t, capture.StatusError, result.Records[3].Status)
	assert.Contains(t, result.Records[1].Error, "traversal")
	assert.Contains(t, result.Records[3].Error, "duplicate")
}

func TestRunCorruptChunkIsReplacedWithoutOrphans(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()

	fake := &fakeCollector{path: "a.log", data: []byte("aaa")}
	cols, _ := buildSet(fake)
	cfg := configFor(cols)

	engine, err := capture.NewEngine(cfg, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	_, err = engine.Run(nilCtx(t), cols)
	require.NoError(t, err)

	chunkPath, err := capture.SealedChunkPathForTest(workspace, 0)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(chunkPath, []byte("tampered"), 0o600))

	// a second engine must rerun the collector, replace the chunk and leave no orphan files
	engine2, err := capture.NewEngine(cfg, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	_, err = engine2.Run(nilCtx(t), cols)
	require.NoError(t, err)
	assert.Equal(t, int32(2), fake.calls.Load())

	entries, err := os.ReadDir(filepath.Join(workspace, "chunks"))
	require.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, "0.chunk", entries[0].Name())

	content, err := os.ReadFile(chunkPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("aaa"), content)
}

func TestRunConfigChangeDiscardsCheckpoint(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()

	fake := &fakeCollector{path: "a.log", data: []byte("aaa")}
	cols, _ := buildSet(fake)

	cfg := configFor(cols)

	engine, err := capture.NewEngine(cfg, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	_, err = engine.Run(context.Background(), cols)
	require.NoError(t, err)
	require.Equal(t, int32(1), fake.calls.Load())

	// redaction protocol change: old checkpoint must be rejected wholesale
	changed := cfg
	changed.Redaction = map[string]bool{"node-1": false}

	engine2, err := capture.NewEngine(changed, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	_, err = engine2.Run(context.Background(), cols)
	require.NoError(t, err)
	assert.Equal(t, int32(2), fake.calls.Load(), "checkpoint discard must force full rerun")
}

func TestRunFingerprintDimensionsForceFullRerun(t *testing.T) {
	t.Parallel()

	variants := []struct {
		name string
		mut  func(cfg capture.RunConfig) capture.RunConfig
	}{
		{
			name: "insecure authorization changes",
			mut: func(cfg capture.RunConfig) capture.RunConfig {
				cfg.Insecure = true

				return cfg
			},
		},
		{
			name: "cert fingerprints change",
			mut: func(cfg capture.RunConfig) capture.RunConfig {
				cfg.CertFingerprints = []string{"fp-new"}

				return cfg
			},
		},
		{
			name: "encryption recipients change",
			mut: func(cfg capture.RunConfig) capture.RunConfig {
				cfg.Encryption = capture.NewEncryptionConfig(true, []string{"age1changed"})

				return cfg
			},
		},
	}

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			t.Parallel()

			workspace := t.TempDir()

			fake := &fakeCollector{path: "a.log", data: []byte("aaa")}
			cols, _ := buildSet(fake)
			cfg := configFor(cols)

			engine, err := capture.NewEngine(cfg, capture.WithWorkspaceDir(workspace))
			require.NoError(t, err)

			_, err = engine.Run(nilCtx(t), cols)
			require.NoError(t, err)
			require.Equal(t, int32(1), fake.calls.Load())

			changed := variant.mut(cfg)

			engine2, err := capture.NewEngine(changed, capture.WithWorkspaceDir(workspace))
			require.NoError(t, err)

			_, err = engine2.Run(nilCtx(t), cols)
			require.NoError(t, err)
			assert.Equal(t, int32(2), fake.calls.Load(), "variant %q must invalidate the checkpoint", variant.name)
		})
	}
}

func TestRunNonIdentityOptionsDoNotDiscard(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()

	fake := &fakeCollector{path: "a.log", data: []byte("aaa")}
	cols, _ := buildSet(fake)
	cfg := configFor(cols)

	engine, err := capture.NewEngine(cfg,
		capture.WithWorkspaceDir(workspace), capture.WithTTL(time.Minute))
	require.NoError(t, err)

	_, err = engine.Run(context.Background(), cols)
	require.NoError(t, err)

	// different TTL and limits, same collection inputs: the sealed artifact stays valid
	engine2, err := capture.NewEngine(cfg,
		capture.WithWorkspaceDir(workspace),
		capture.WithTTL(10*time.Minute),
		capture.WithMaxDiskBytes(4096),
		capture.WithMaxCollectorBytes(2048))
	require.NoError(t, err)

	_, err = engine2.Run(context.Background(), cols)
	require.NoError(t, err)
	assert.Equal(t, int32(1), fake.calls.Load())
}

func TestRunProgressReportsEveryCollector(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()

	cols, _ := buildSet(
		&fakeCollector{path: "a.log", data: []byte("aaa")},
		&fakeCollector{path: "b.log", data: []byte("bbb")},
	)

	progress := make(chan bundle.Progress, 4)

	engine, err := capture.NewEngine(configFor(cols),
		capture.WithWorkspaceDir(workspace), capture.WithProgressChan(progress))
	require.NoError(t, err)

	_, err = engine.Run(context.Background(), cols)
	require.NoError(t, err)
	close(progress)

	events := 0
	for range progress {
		events++
	}

	assert.Equal(t, 2, events)

	// second run reports restore events too
	progress2 := make(chan bundle.Progress, 4)

	engine2, err := capture.NewEngine(configFor(cols),
		capture.WithWorkspaceDir(workspace), capture.WithProgressChan(progress2))
	require.NoError(t, err)

	_, err = engine2.Run(context.Background(), cols)
	require.NoError(t, err)
	close(progress2)

	for p := range progress2 {
		assert.Contains(t, p.State, "sealed artifact")
	}
}
