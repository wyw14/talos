// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/siderolabs/talos/cmd/talosctl/pkg/talos/support/capture"
)

func newTestStore(t *testing.T, opts ...capture.Option) *capture.Store {
	t.Helper()

	allOpts := append([]capture.Option{capture.WithWorkspaceDir(t.TempDir())}, opts...)

	store, err := capture.OpenStore(baseConfig(), capture.NewOptions(allOpts...))
	require.NoError(t, err)

	return store
}

func identity(ordinal int, name string) capture.CollectorIdentity {
	return capture.CollectorIdentity{Ordinal: ordinal, Source: "cluster", Name: name}
}

func TestStoreSealAndReuse(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	started := time.Now().Add(-time.Second)
	finished := time.Now()

	part, err := store.CreatePart(0)
	require.NoError(t, err)
	require.NoError(t, part.Write(store, "dmesg.log", []byte("hello")))

	record, err := store.Seal(part, identity(0, "collect dmesg.log"), started, finished)
	require.NoError(t, err)
	assert.Equal(t, capture.StatusSealed, record.Status)
	assert.Equal(t, int64(5), record.Length)
	assert.Len(t, record.SHA256, 64)

	chunkPath, err := capture.SealedChunkPathForTest(store.Root(), 0)
	require.NoError(t, err)

	info, err := os.Stat(chunkPath)
	require.NoError(t, err)

	if runtime.GOOS != "windows" {
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "plaintext chunk must be owner-only")
	}

	partPath, err := capture.PartChunkPathForTest(store.Root(), 0)
	require.NoError(t, err)

	_, err = os.Stat(partPath)
	assert.ErrorIs(t, err, os.ErrNotExist, "part file must be renamed away")

	require.True(t, store.Reusable(record))

	persisted, ok := store.Record(0)
	require.True(t, ok)
	assert.Equal(t, record, persisted)
}

func TestStoreReusableRejectsCorruptMissingExpired(t *testing.T) {
	// subtests mutate the same sealed chunk and must not run in parallel with each other
	now := time.Now()
	clock := func() time.Time { return now }

	store := newTestStore(t, capture.WithClock(clock))

	part, err := store.CreatePart(0)
	require.NoError(t, err)
	require.NoError(t, part.Write(store, "dmesg.log", []byte("hello")))

	record, err := store.Seal(part, identity(0, "collect dmesg.log"), now.Add(-time.Second), now)
	require.NoError(t, err)
	require.True(t, store.Reusable(record))

	t.Run("corrupted chunk", func(t *testing.T) {
		chunkPath, pathErr := capture.SealedChunkPathForTest(store.Root(), 0)
		require.NoError(t, pathErr)
		require.NoError(t, os.WriteFile(chunkPath, []byte("tampered"), 0o600))
		assert.False(t, store.Reusable(record))
	})

	t.Run("missing chunk", func(t *testing.T) {
		chunkPath, pathErr := capture.SealedChunkPathForTest(store.Root(), 0)
		require.NoError(t, pathErr)
		require.NoError(t, os.Remove(chunkPath))
		assert.False(t, store.Reusable(record))
	})

	t.Run("expired TTL", func(t *testing.T) {
		// restore a valid chunk first
		chunkPath, pathErr := capture.SealedChunkPathForTest(store.Root(), 0)
		require.NoError(t, pathErr)
		require.NoError(t, os.WriteFile(chunkPath, []byte("hello"), 0o600))
		require.True(t, store.Reusable(record))

		expiredStore, err := capture.OpenStore(baseConfig(), capture.NewOptions(
			capture.WithWorkspaceDir(store.Root()),
			capture.WithTTL(time.Minute),
			capture.WithClock(func() time.Time { return now.Add(2 * time.Minute) }),
		))
		require.NoError(t, err)
		assert.False(t, expiredStore.Reusable(record))
	})
}

func TestStoreEmptyAndErrorRecords(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Now()

	require.NoError(t, store.RecordEmpty(identity(0, "collect empty"), now, now))

	empty, ok := store.Record(0)
	require.True(t, ok)
	assert.Equal(t, capture.StatusEmpty, empty.Status)
	assert.True(t, store.Reusable(empty))

	part, err := store.CreatePart(1)
	require.NoError(t, err)
	require.NoError(t, part.Write(store, "failing.log", []byte("partial")))
	require.NoError(t, store.RecordError(identity(1, "collect failing"), now, now, part, assertError("boom")))

	failed, ok := store.Record(1)
	require.True(t, ok)
	assert.Equal(t, capture.StatusError, failed.Status)
	assert.Contains(t, failed.Error, "boom")
	assert.False(t, store.Reusable(failed), "error records are never reused")

	partPath, err := capture.PartChunkPathForTest(store.Root(), 1)
	require.NoError(t, err)

	_, err = os.Stat(partPath)
	assert.ErrorIs(t, err, os.ErrNotExist, "failed part must be cleaned up")
}

func TestStoreEnforcesCollectorLimit(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, capture.WithMaxCollectorBytes(4))

	part, err := store.CreatePart(0)
	require.NoError(t, err)

	err = part.Write(store, "big.log", []byte("12345"))
	require.Error(t, err)
	assert.ErrorIs(t, err, capture.ErrCollectorLimitExceeded)

	store.Abort(part)
}

func TestStoreEnforcesDiskBudget(t *testing.T) {
	t.Parallel()

	store := newTestStore(t,
		capture.WithMaxCollectorBytes(1024),
		capture.WithMaxDiskBytes(6),
	)

	part, err := store.CreatePart(0)
	require.NoError(t, err)
	require.NoError(t, part.Write(store, "a.log", []byte("1234")))

	record, err := store.Seal(part, identity(0, "collect a"), time.Now(), time.Now())
	require.NoError(t, err)
	assert.Equal(t, int64(4), record.Length)

	// 4 bytes in use, another 4 would exceed the budget of 6
	part2, err := store.CreatePart(1)
	require.NoError(t, err)

	err = part2.Write(store, "b.log", []byte("5678"))
	require.Error(t, err)
	assert.ErrorIs(t, err, capture.ErrDiskBudgetExceeded)
	store.Abort(part2)

	// once the budget is fully consumed even opening a new part is refused
	part3, err := store.CreatePart(2)
	require.NoError(t, err)
	require.NoError(t, part3.Write(store, "c.log", []byte("56")))
	_, err = store.Seal(part3, identity(2, "collect c"), time.Now(), time.Now())
	require.NoError(t, err)

	_, err = store.CreatePart(3)
	require.Error(t, err)
	assert.ErrorIs(t, err, capture.ErrDiskBudgetExceeded)
}

func TestStoreResetContainedInsideWorkspace(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()

	// a sentinel next to the workspace must never be touched
	sentinel := filepath.Join(filepath.Dir(workspace), "sentinel-"+filepath.Base(workspace))
	require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0o600))

	store, err := capture.OpenStore(baseConfig(), capture.NewOptions(capture.WithWorkspaceDir(workspace)))
	require.NoError(t, err)

	part, err := store.CreatePart(0)
	require.NoError(t, err)
	require.NoError(t, part.Write(store, "a.log", []byte("data")))
	_, err = store.Seal(part, identity(0, "collect a"), time.Now(), time.Now())
	require.NoError(t, err)

	// opening with changed inputs discards the workspace
	changed := baseConfig()
	changed.Nodes = []string{"other-node"}

	store2, err := capture.OpenStore(changed, capture.NewOptions(capture.WithWorkspaceDir(workspace)))
	require.NoError(t, err)

	_, ok := store2.Record(0)
	assert.False(t, ok, "old records must be discarded")

	chunkPath, err := capture.SealedChunkPathForTest(workspace, 0)
	require.NoError(t, err)

	_, err = os.Stat(chunkPath)
	assert.ErrorIs(t, err, os.ErrNotExist)

	kept, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	assert.Equal(t, "keep", string(kept))
}

type stringError string

func (e stringError) Error() string { return string(e) }

func assertError(msg string) error { return stringError(msg) }
