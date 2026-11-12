// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/siderolabs/talos/cmd/talosctl/pkg/talos/support/capture"
)

func TestCaptureArchiveRejectsSecondWrite(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	part, err := store.CreatePart(0)
	require.NoError(t, err)

	archive := capture.NewCaptureArchiveForTest(store, part, capture.NewPathTracker())

	require.NoError(t, archive.Write("a.log", []byte("first")))

	err = archive.Write("b.log", []byte("second"))
	require.Error(t, err)
	assert.ErrorIs(t, err, capture.ErrMultipleWrites)

	store.Abort(part)
}

func TestCaptureArchiveReservesManifestName(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	part, err := store.CreatePart(0)
	require.NoError(t, err)

	archive := capture.NewCaptureArchiveForTest(store, part, capture.NewPathTracker())

	err = archive.Write(capture.ManifestName, []byte("forged"))
	require.Error(t, err)
	assert.ErrorIs(t, err, capture.ErrPathDuplicate)

	store.Abort(part)
}

func TestOpenStoreRejectsForeignDirectory(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(workspace, "user-data.txt"), []byte("mine"), 0o600))

	_, err := capture.OpenStore(baseConfig(), capture.NewOptions(capture.WithWorkspaceDir(workspace)))
	require.Error(t, err)
	assert.ErrorIs(t, err, capture.ErrWorkspaceNotOwned)

	// the unrelated file must stay untouched
	kept, err := os.ReadFile(filepath.Join(workspace, "user-data.txt"))
	require.NoError(t, err)
	assert.Equal(t, "mine", string(kept))
}

func TestOpenStoreAcceptsMarkedWorkspace(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(workspace, "chunks"), 0o700))

	store, err := capture.OpenStore(baseConfig(), capture.NewOptions(capture.WithWorkspaceDir(workspace)))
	require.NoError(t, err)
	assert.Equal(t, workspace, store.Root())
}

func TestLoadCheckpointRejectsTrailingData(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	cp := &capture.Checkpoint{
		FormatVersion: capture.FormatVersion,
		Fingerprint:   repeat("a", 64),
		Artifacts:     []capture.ArtifactRecord{},
	}

	require.NoError(t, capture.SaveCheckpointForTest(root, cp))

	checkpointPath := filepath.Join(root, "checkpoint.json")

	original, err := os.ReadFile(checkpointPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(checkpointPath, append(original, []byte(" {\"extra\":1}")...), 0o600))

	_, err = capture.LoadCheckpointForTest(root, 1024)
	require.Error(t, err)
	assert.ErrorIs(t, err, capture.ErrCheckpointCorrupt)
}
