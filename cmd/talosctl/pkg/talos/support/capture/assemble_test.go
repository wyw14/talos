// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/siderolabs/go-talos-support/support/bundle/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/siderolabs/talos/cmd/talosctl/pkg/talos/support/capture"
)

func advancingClock() (*time.Time, func() time.Time) {
	tick := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)

	return &tick, func() time.Time {
		tick = tick.Add(time.Second)

		return tick
	}
}

func readZip(t *testing.T, data []byte) map[string][]byte {
	t.Helper()

	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	files := map[string][]byte{}

	for _, f := range reader.File {
		rc, err := f.Open()
		require.NoError(t, err)

		content, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())

		files[f.Name] = content
	}

	return files
}

func TestAssemblePlainArchive(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	output := filepath.Join(t.TempDir(), "support.zip")

	_, clock := advancingClock()

	cols, fakes := buildSet(
		&fakeCollector{path: "a.log", data: []byte("aaa")},
		&fakeCollector{path: "n1/b.log", source: "node-1", data: []byte("bbb")},
	)

	engine, err := capture.NewEngine(configFor(cols),
		capture.WithWorkspaceDir(workspace), capture.WithClock(clock))
	require.NoError(t, err)

	_, err = engine.Run(nilCtx(t), cols)
	require.NoError(t, err)

	result, err := engine.Assemble(output, capture.PlainSink{})
	require.NoError(t, err)
	assert.False(t, result.Partial)
	assert.Equal(t, capture.OverallComplete, result.Manifest.OverallStatus)

	raw, err := os.ReadFile(output)
	require.NoError(t, err)

	files := readZip(t, raw)
	assert.Equal(t, []byte("aaa"), files["a.log"])
	assert.Equal(t, []byte("bbb"), files["n1/b.log"])

	manifestRaw, ok := files[capture.ManifestName]
	require.True(t, ok)

	var manifest capture.Manifest
	require.NoError(t, json.Unmarshal(manifestRaw, &manifest))
	assert.Equal(t, capture.OverallComplete, manifest.OverallStatus)
	assert.Contains(t, manifest.SnapshotSemantics, "not an atomic point-in-time snapshot")
	assert.Equal(t, 2, manifest.Counts.Sealed)
	assert.Len(t, manifest.Artifacts, 2)

	finishedAt := map[string]struct{}{}
	for _, record := range manifest.Artifacts {
		finishedAt[record.FinishedAt] = struct{}{}
		assert.NotEmpty(t, record.StartedAt)
		assert.Len(t, record.SHA256, 64)
	}

	assert.GreaterOrEqual(t, len(finishedAt), 2, "each artifact keeps its own collection time")

	// complete runs remove the workspace and the sealed chunks
	_, err = os.Stat(workspace)
	assert.ErrorIs(t, err, os.ErrNotExist)

	for _, f := range fakes {
		assert.Equal(t, int32(1), f.calls.Load())
	}
}

func TestAssemblePartialManifestIsHonestAndWorkspaceKept(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	output := filepath.Join(t.TempDir(), "support.zip")

	cols, _ := buildSet(
		&fakeCollector{path: "a.log", data: []byte("aaa")},
		&fakeCollector{path: "b.log", err: stringError("nope")},
	)

	engine, err := capture.NewEngine(configFor(cols), capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	run, err := engine.Run(nilCtx(t), cols)
	require.NoError(t, err)
	require.True(t, run.Partial)

	result, err := engine.Assemble(output, capture.PlainSink{})
	require.NoError(t, err)
	assert.True(t, result.Partial)
	assert.Equal(t, capture.OverallPartial, result.Manifest.OverallStatus)
	assert.Equal(t, 1, result.Manifest.Counts.Error)

	var errorRecord *capture.ArtifactRecord

	for i := range result.Manifest.Artifacts {
		if result.Manifest.Artifacts[i].Status == capture.StatusError {
			errorRecord = &result.Manifest.Artifacts[i]
		}
	}

	require.NotNil(t, errorRecord)
	assert.Contains(t, errorRecord.Error, "nope")

	files := readZip(t, mustReadFile(t, output))
	_, hasFailedEntry := files["b.log"]
	assert.False(t, hasFailedEntry, "error collectors must not produce archive entries")
	assert.Contains(t, files, capture.ManifestName)

	// partial runs keep the workspace so the next run can resume only the failed collector
	_, err = os.Stat(workspace)
	require.NoError(t, err)
}

func TestAssembleEncryptedRoundTripWithoutKeyMaterial(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	output := filepath.Join(t.TempDir(), "support.zip.age")

	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	recipient := identity.Recipient().String()

	cols, _ := buildSet(
		&fakeCollector{path: "a.log", data: []byte("secret-payload")},
	)

	cfg := capture.BuildRunConfig(cols, []string{"node-1"}, nil, false, map[string]bool{"node-1": true},
		capture.NewEncryptionConfig(true, []string{recipient}))

	engine, err := capture.NewEngine(cfg, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	_, err = engine.Run(nilCtx(t), cols)
	require.NoError(t, err)

	// no private key material may land anywhere in the workspace
	err = filepath.Walk(workspace, func(path string, info os.FileInfo, walkErr error) error {
		require.NoError(t, walkErr)

		if info.IsDir() {
			return nil
		}

		content, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		assert.NotContains(t, string(content), identity.String())
		assert.NotContains(t, string(content), "AGE-SECRET")

		return nil
	})
	require.NoError(t, err)

	sink := capture.AgeSink{Options: []encryption.Option{encryption.WithRecipients(recipient)}}

	_, err = engine.Assemble(output, sink)
	require.NoError(t, err)

	encrypted, err := os.ReadFile(output)
	require.NoError(t, err)
	assert.NotContains(t, string(encrypted), "secret-payload", "plaintext must not leak into the ciphertext file")

	decrypted, err := age.Decrypt(bytes.NewReader(encrypted), identity)
	require.NoError(t, err)

	raw, err := io.ReadAll(decrypted)
	require.NoError(t, err)

	files := readZip(t, raw)
	assert.Equal(t, []byte("secret-payload"), files["a.log"])
	assert.Contains(t, files, capture.ManifestName)
}

func TestAssembleRewritesOutputAsFreshIndependentCiphertext(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	output := filepath.Join(t.TempDir(), "support.zip.age")

	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	recipient := identity.Recipient().String()
	sink := capture.AgeSink{Options: []encryption.Option{encryption.WithRecipients(recipient)}}

	cols, _ := buildSet(&fakeCollector{path: "a.log", data: []byte("payload")})
	cfg := configFor(cols)

	engine, err := capture.NewEngine(cfg, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	_, err = engine.Run(nilCtx(t), cols)
	require.NoError(t, err)

	// first assembly, then pretend an old ciphertext was left behind and assemble again
	_, err = engine.Assemble(output, sink)
	require.NoError(t, err)

	first, err := os.ReadFile(output)
	require.NoError(t, err)

	// the first complete assembly removes the workspace; rebuild one to prove truncation semantics
	engine2, err := capture.NewEngine(cfg, capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	_, err = engine2.Run(nilCtx(t), cols)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(output, []byte("stale-ciphertext-prefix"), 0o644))

	_, err = engine2.Assemble(output, sink)
	require.NoError(t, err)

	second, err := os.ReadFile(output)
	require.NoError(t, err)

	assert.NotContains(t, string(second), "stale-ciphertext-prefix", "output must be truncated, never appended")
	assert.NotEqual(t, first, second, "each assembly creates an independent fresh age stream")

	for _, blob := range [][]byte{first, second} {
		reader, decErr := age.Decrypt(bytes.NewReader(blob), identity)
		require.NoError(t, decErr)

		raw, readErr := io.ReadAll(reader)
		require.NoError(t, readErr)
		assert.Contains(t, readZip(t, raw), "a.log")
	}
}

func TestAssembleFailureRemovesPartialOutputAndKeepsWorkspace(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	output := filepath.Join(t.TempDir(), "support.zip")

	cols, _ := buildSet(&fakeCollector{path: "a.log", data: []byte("aaa")})

	engine, err := capture.NewEngine(configFor(cols), capture.WithWorkspaceDir(workspace))
	require.NoError(t, err)

	_, err = engine.Run(nilCtx(t), cols)
	require.NoError(t, err)

	_, err = engine.Assemble(output, failingSink{})
	require.Error(t, err)

	_, err = os.Stat(output)
	assert.ErrorIs(t, err, os.ErrNotExist, "partial output must be cleaned up")

	chunkPath, err := capture.SealedChunkPathForTest(workspace, 0)
	require.NoError(t, err)

	_, err = os.Stat(chunkPath)
	require.NoError(t, err, "workspace must survive assembly failure for resume")
}

type failingSink struct{}

func (failingSink) Wrap(io.Writer) (io.WriteCloser, error) {
	return nil, errors.New("sink boom")
}

func nilCtx(t *testing.T) context.Context {
	t.Helper()

	return context.Background()
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	return data
}
