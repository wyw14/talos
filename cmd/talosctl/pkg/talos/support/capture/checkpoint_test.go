// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/siderolabs/go-talos-support/support/collectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/siderolabs/talos/cmd/talosctl/pkg/talos/support/capture"
)

func baseCollectors() []*collectors.Collector {
	return []*collectors.Collector{
		collectors.NewCollector("dmesg.log", func() ([]byte, error) { return []byte("dmesg"), nil }),
		collectors.WithSource(
			[]*collectors.Collector{
				collectors.NewCollector("summary", func() ([]byte, error) { return []byte("summary"), nil }),
			}, "node-1",
		)[0],
	}
}

func baseConfig() capture.RunConfig {
	return capture.BuildRunConfig(
		baseCollectors(),
		[]string{"node-1"},
		[]string{"fp-1"},
		false,
		map[string]bool{"node-1": true},
		capture.NewEncryptionConfig(true, []string{"age1recipient"}),
	)
}

func TestFingerprintStableAndInputSensitive(t *testing.T) {
	t.Parallel()

	cfg := baseConfig()

	fp1, err := cfg.Fingerprint()
	require.NoError(t, err)
	require.Len(t, fp1, 64)

	fp2, err := cfg.Fingerprint()
	require.NoError(t, err)
	assert.Equal(t, fp1, fp2)

	// slice ordering must not matter for the node and fingerprint inputs
	reordered := cfg
	reordered.Nodes = []string{"node-2", "node-1"}
	reordered.CertFingerprints = []string{"fp-2", "fp-1"}

	cfg2 := cfg
	cfg2.Nodes = []string{"node-1", "node-2"}
	cfg2.CertFingerprints = []string{"fp-1", "fp-2"}

	fpReordered, err := reordered.Fingerprint()
	require.NoError(t, err)

	fpSorted, err := cfg2.Fingerprint()
	require.NoError(t, err)
	assert.Equal(t, fpSorted, fpReordered)

	variants := []capture.RunConfig{
		func() capture.RunConfig {
			c := cfg
			c.Insecure = true

			return c
		}(),
		func() capture.RunConfig {
			c := cfg
			c.Redaction = map[string]bool{"node-1": false}

			return c
		}(),
		func() capture.RunConfig {
			c := cfg
			c.Encryption = capture.EncryptionConfig{Enabled: false}

			return c
		}(),
		func() capture.RunConfig {
			c := cfg
			c.Encryption = capture.NewEncryptionConfig(true, []string{"age1other"})

			return c
		}(),
	}

	for i, variant := range variants {
		fp, err := variant.Fingerprint()
		require.NoError(t, err)
		assert.NotEqual(t, fp1, fp, "variant %d must change the fingerprint", i)
	}
}

func TestCheckpointRoundTrip(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

	cp := &capture.Checkpoint{
		FormatVersion: capture.FormatVersion,
		Fingerprint:   mustFingerprint(t, baseConfig()),
		CreatedAt:     now.Format(time.RFC3339Nano),
		Artifacts: []capture.ArtifactRecord{
			{
				Ordinal:    0,
				Source:     "cluster",
				Name:       "collect dmesg.log",
				Path:       "dmesg.log",
				Status:     capture.StatusSealed,
				StartedAt:  now.Format(time.RFC3339Nano),
				FinishedAt: now.Add(time.Second).Format(time.RFC3339Nano),
				Length:     5,
				SHA256:     "a" + repeat("b", 63),
				Chunk:      "chunks/0.chunk",
			},
		},
	}

	require.NoError(t, capture.SaveCheckpointForTest(root, cp))

	loaded, err := capture.LoadCheckpointForTest(root, 1024)
	require.NoError(t, err)
	assert.Equal(t, cp, loaded)
}

func TestCheckpointRejectsInvalidPayloads(t *testing.T) {
	// subtests share the same checkpoint file and must not run in parallel with each other
	root := t.TempDir()
	fp := mustFingerprint(t, baseConfig())

	validRecord := func() capture.ArtifactRecord {
		return capture.ArtifactRecord{
			Ordinal: 0, Source: "cluster", Name: "collect dmesg.log",
			Status: capture.StatusSealed, Length: 1, SHA256: repeat("a", 64), Chunk: "chunks/0.chunk",
		}
	}

	write := func(cp *capture.Checkpoint) {
		require.NoError(t, capture.SaveCheckpointForTest(root, cp))
	}

	t.Run("corrupt JSON", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(root, "checkpoint.json"), []byte("{not-json"), 0o600))
		_, err := capture.LoadCheckpointForTest(root, 1024)
		require.Error(t, err)
		assert.ErrorIs(t, err, capture.ErrCheckpointCorrupt)
	})

	t.Run("unknown version", func(t *testing.T) {
		write(&capture.Checkpoint{FormatVersion: 999, Fingerprint: fp, Artifacts: []capture.ArtifactRecord{}})
		_, err := capture.LoadCheckpointForTest(root, 1024)
		require.Error(t, err)
		assert.ErrorIs(t, err, capture.ErrCheckpointUnsupported)
	})

	t.Run("bad fingerprint", func(t *testing.T) {
		write(&capture.Checkpoint{FormatVersion: capture.FormatVersion, Fingerprint: "xyz", Artifacts: []capture.ArtifactRecord{}})
		_, err := capture.LoadCheckpointForTest(root, 1024)
		require.Error(t, err)
		assert.ErrorIs(t, err, capture.ErrCheckpointCorrupt)
	})

	t.Run("length over limit", func(t *testing.T) {
		record := validRecord()
		record.Length = 2048
		write(&capture.Checkpoint{FormatVersion: capture.FormatVersion, Fingerprint: fp, Artifacts: []capture.ArtifactRecord{record}})
		_, err := capture.LoadCheckpointForTest(root, 1024)
		require.Error(t, err)
		assert.ErrorIs(t, err, capture.ErrCheckpointCorrupt)
	})

	t.Run("negative length", func(t *testing.T) {
		record := validRecord()
		record.Length = -1
		write(&capture.Checkpoint{FormatVersion: capture.FormatVersion, Fingerprint: fp, Artifacts: []capture.ArtifactRecord{record}})
		_, err := capture.LoadCheckpointForTest(root, 1024)
		require.Error(t, err)
		assert.ErrorIs(t, err, capture.ErrCheckpointCorrupt)
	})

	t.Run("unknown status", func(t *testing.T) {
		record := validRecord()
		record.Status = "mystery"
		write(&capture.Checkpoint{FormatVersion: capture.FormatVersion, Fingerprint: fp, Artifacts: []capture.ArtifactRecord{record}})
		_, err := capture.LoadCheckpointForTest(root, 1024)
		require.Error(t, err)
		assert.ErrorIs(t, err, capture.ErrCheckpointCorrupt)
	})

	t.Run("chunk name escapes", func(t *testing.T) {
		record := validRecord()
		record.Chunk = "chunks/../0.chunk"
		write(&capture.Checkpoint{FormatVersion: capture.FormatVersion, Fingerprint: fp, Artifacts: []capture.ArtifactRecord{record}})
		_, err := capture.LoadCheckpointForTest(root, 1024)
		require.Error(t, err)
		assert.ErrorIs(t, err, capture.ErrCheckpointCorrupt)
	})

	t.Run("duplicate ordinal", func(t *testing.T) {
		record := validRecord()
		write(&capture.Checkpoint{FormatVersion: capture.FormatVersion, Fingerprint: fp, Artifacts: []capture.ArtifactRecord{record, record}})
		_, err := capture.LoadCheckpointForTest(root, 1024)
		require.Error(t, err)
		assert.ErrorIs(t, err, capture.ErrCheckpointCorrupt)
	})
}

func TestCheckpointCarriesNoKeyMaterial(t *testing.T) {
	t.Parallel()

	cfg := baseConfig()

	encodedConfig, err := json.Marshal(cfg)
	require.NoError(t, err)

	cp := capture.Checkpoint{
		FormatVersion: capture.FormatVersion,
		Fingerprint:   mustFingerprint(t, cfg),
		Artifacts: []capture.ArtifactRecord{{
			Ordinal: 0, Status: capture.StatusSealed, Length: 1,
			SHA256: repeat("a", 64), Chunk: "chunks/0.chunk",
		}},
	}

	encodedCheckpoint, err := json.Marshal(cp)
	require.NoError(t, err)

	// no field may hint at persisted secret/key/identity material
	forbidden := regexp.MustCompile(`(?i)(secret|private|decrypt|identity)`)
	assert.NotRegexp(t, forbidden, string(encodedConfig))
	assert.NotRegexp(t, forbidden, string(encodedCheckpoint))
}

func mustFingerprint(t *testing.T, cfg capture.RunConfig) string {
	t.Helper()

	fp, err := cfg.Fingerprint()
	require.NoError(t, err)

	return fp
}

func repeat(s string, n int) string {
	out := make([]byte, 0, n)
	for range n {
		out = append(out, s...)
	}

	return string(out)
}
