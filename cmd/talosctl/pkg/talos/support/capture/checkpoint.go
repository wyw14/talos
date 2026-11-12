// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/siderolabs/go-talos-support/support/collectors"
)

// Artifact statuses persisted in the checkpoint and surfaced in the manifest.
const (
	// StatusSealed marks a collector whose result was written, verified and sealed as a chunk.
	StatusSealed = "sealed"
	// StatusEmpty marks a collector which completed without producing any data (nil result).
	StatusEmpty = "empty"
	// StatusError marks a collector whose run failed; the error text is kept in the record.
	StatusError = "error"
)

// CollectorIdentity is the stable, pre-run identity of a collector: its position in the ordered
// collector set, its source (node name or the cluster placeholder) and its String() description.
type CollectorIdentity struct {
	Ordinal int    `json:"ordinal"`
	Source  string `json:"source"`
	Name    string `json:"name"`
}

// EncryptionConfig describes the encryption part of the collection inputs. Only public material
// is referenced: whether encryption is enabled and a hash of the recipient lines. No decryption
// identity or key ever reaches the checkpoint, because the age content key is generated inside
// the encryption layer at assembly time and never leaves it.
type EncryptionConfig struct {
	Enabled        bool   `json:"enabled"`
	RecipientsHash string `json:"recipients_hash,omitempty"`
}

// RunConfig is the full set of collection inputs the sealed artifacts are bound to. Any change in
// these inputs changes the fingerprint and discards the old checkpoint.
type RunConfig struct {
	FormatVersion    int                 `json:"format_version"`
	Collectors       []CollectorIdentity `json:"collectors"`
	Nodes            []string            `json:"nodes"`
	Insecure         bool                `json:"insecure"`
	CertFingerprints []string            `json:"cert_fingerprints,omitempty"`
	Redaction        map[string]bool     `json:"redaction,omitempty"`
	Encryption       EncryptionConfig    `json:"encryption"`
}

// ArtifactRecord is the per-collector state persisted in the checkpoint.
type ArtifactRecord struct {
	Ordinal    int    `json:"ordinal"`
	Source     string `json:"source"`
	Name       string `json:"name"`
	Path       string `json:"path,omitempty"`
	Status     string `json:"status"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at"`
	Length     int64  `json:"length"`
	SHA256     string `json:"sha256,omitempty"`
	Chunk      string `json:"chunk,omitempty"`
	Error      string `json:"error,omitempty"`
}

// Checkpoint is the on-disk resume state of a capture run.
type Checkpoint struct {
	FormatVersion int              `json:"format_version"`
	Fingerprint   string           `json:"fingerprint"`
	CreatedAt     string           `json:"created_at"`
	Artifacts     []ArtifactRecord `json:"artifacts"`
}

// BuildRunConfig assembles the RunConfig from the ordered collector set and the other collection
// inputs. Slices are copied so later mutation by the caller cannot change the fingerprint inputs.
func BuildRunConfig(cols []*collectors.Collector, nodes, certFingerprints []string, insecure bool,
	redaction map[string]bool, encryption EncryptionConfig,
) RunConfig {
	identities := make([]CollectorIdentity, 0, len(cols))

	for i, col := range cols {
		identities = append(identities, CollectorIdentity{
			Ordinal: i,
			Source:  col.Source(),
			Name:    col.String(),
		})
	}

	cfg := RunConfig{
		FormatVersion:    FormatVersion,
		Collectors:       identities,
		Nodes:            append([]string(nil), nodes...),
		Insecure:         insecure,
		CertFingerprints: append([]string(nil), certFingerprints...),
		Redaction:        make(map[string]bool, len(redaction)),
		Encryption:       encryption,
	}

	for node, supported := range redaction {
		cfg.Redaction[node] = supported
	}

	return cfg
}

// Fingerprint returns the hex SHA-256 of the canonical JSON encoding of the config. Order-only
// differences in the node and fingerprint slices normalize away; the ordered collector list is
// kept verbatim because its order is part of the collector set identity.
func (cfg RunConfig) Fingerprint() (string, error) {
	normalized := cfg
	normalized.FormatVersion = FormatVersion
	normalized.Nodes = append([]string(nil), cfg.Nodes...)
	normalized.CertFingerprints = append([]string(nil), cfg.CertFingerprints...)
	slices.Sort(normalized.Nodes)
	slices.Sort(normalized.CertFingerprints)

	encoded, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("failed to encode run config for fingerprinting: %w", err)
	}

	digest := sha256.Sum256(encoded)

	return hex.EncodeToString(digest[:]), nil
}

// saveCheckpoint atomically writes the checkpoint into root: a private temp file, an fsync and a
// same-directory rename, so a crash can never leave a half-written checkpoint behind.
func saveCheckpoint(root string, cp *Checkpoint) error {
	encoded, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode checkpoint: %w", err)
	}

	tmpPath, err := ResolveWorkspacePath(root, checkpointTempName)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint temp file: %w", err)
	}

	_, writeErr := file.Write(encoded)
	if writeErr == nil {
		writeErr = file.Sync()
	}

	closeErr := file.Close()

	if writeErr != nil {
		os.Remove(tmpPath) //nolint:errcheck // best effort, the original error is returned

		return fmt.Errorf("failed to write checkpoint: %w", writeErr)
	}

	if closeErr != nil {
		os.Remove(tmpPath) //nolint:errcheck

		return fmt.Errorf("failed to close checkpoint temp file: %w", closeErr)
	}

	finalPath, err := ResolveWorkspacePath(root, checkpointName)
	if err != nil {
		os.Remove(tmpPath) //nolint:errcheck

		return err
	}

	if err = os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("failed to seal checkpoint: %w", err)
	}

	syncDirectory(root)

	return nil
}

// loadCheckpoint reads and strictly validates the checkpoint. Length and chunk-name validation
// happens before any chunk file is opened, so a corrupted checkpoint cannot drive an allocation
// or an out-of-workspace access.
func loadCheckpoint(root string, maxCollectorBytes int64) (*Checkpoint, error) {
	checkpointPath, err := ResolveWorkspacePath(root, checkpointName)
	if err != nil {
		return nil, err
	}

	encoded, err := os.ReadFile(checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()

	var cp Checkpoint

	if err = decoder.Decode(&cp); err != nil {
		return nil, fmt.Errorf("%w: failed to decode checkpoint: %w", ErrCheckpointCorrupt, err)
	}

	// the checkpoint must be exactly one JSON document: trailing data indicates tampering
	var extra json.RawMessage
	if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: trailing data after checkpoint document", ErrCheckpointCorrupt)
	}

	if cp.FormatVersion != FormatVersion {
		return nil, fmt.Errorf("%w: checkpoint version %d, expected %d",
			ErrCheckpointUnsupported, cp.FormatVersion, FormatVersion)
	}

	if len(cp.Fingerprint) != sha256.Size*2 {
		return nil, fmt.Errorf("%w: malformed fingerprint %q", ErrCheckpointCorrupt, cp.Fingerprint)
	}

	if _, err = hex.DecodeString(cp.Fingerprint); err != nil {
		return nil, fmt.Errorf("%w: malformed fingerprint: %w", ErrCheckpointCorrupt, err)
	}

	seenOrdinals := map[int]struct{}{}

	for i := range cp.Artifacts {
		record := &cp.Artifacts[i]

		if record.Ordinal < 0 {
			return nil, fmt.Errorf("%w: negative ordinal %d", ErrCheckpointCorrupt, record.Ordinal)
		}

		if _, ok := seenOrdinals[record.Ordinal]; ok {
			return nil, fmt.Errorf("%w: duplicate ordinal %d", ErrCheckpointCorrupt, record.Ordinal)
		}

		seenOrdinals[record.Ordinal] = struct{}{}

		if !isKnownStatus(record.Status) {
			return nil, fmt.Errorf("%w: unknown status %q for ordinal %d",
				ErrCheckpointCorrupt, record.Status, record.Ordinal)
		}

		if record.Length < 0 || record.Length > maxCollectorBytes {
			return nil, fmt.Errorf("%w: length %d out of bounds for ordinal %d",
				ErrCheckpointCorrupt, record.Length, record.Ordinal)
		}

		if err = validateStoredChunkName(record.Ordinal, record.Chunk); err != nil {
			return nil, fmt.Errorf("%w: invalid chunk name", ErrCheckpointCorrupt)
		}
	}

	return &cp, nil
}

// Checkpoint load failures.
var (
	// ErrCheckpointUnsupported marks a checkpoint whose layout version is unknown.
	ErrCheckpointUnsupported = errors.New("unsupported checkpoint format")
	// ErrCheckpointCorrupt marks a checkpoint whose contents fail validation.
	ErrCheckpointCorrupt = errors.New("corrupt checkpoint")
)

func isKnownStatus(status string) bool {
	switch status {
	case StatusSealed, StatusEmpty, StatusError:
		return true
	default:
		return false
	}
}

// timestamp renders t as an RFC3339Nano UTC string, the fixed wire format of all record times.
func timestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// syncDirectory fsyncs a directory so a rename inside it is durable. Directory syncing is not
// supported on Windows, where the rename is already flushed by the file close.
func syncDirectory(dir string) {
	if runtime.GOOS == "windows" {
		return
	}

	d, err := os.Open(dir)
	if err != nil {
		return
	}

	defer d.Close() //nolint:errcheck

	_ = d.Sync()
}

// NewEncryptionConfig builds the encryption part of a RunConfig from the resolved, public
// recipient lines. It never sees or stores a decryption identity.
func NewEncryptionConfig(enabled bool, recipientLines []string) EncryptionConfig {
	return EncryptionConfig{
		Enabled:        enabled,
		RecipientsHash: hashRecipients(enabled, recipientLines),
	}
}

// hashRecipients returns the hex SHA-256 of the sorted recipient lines, or an empty string when
// encryption is disabled. The lines are public keys only.
func hashRecipients(enabled bool, recipients []string) string {
	if !enabled {
		return ""
	}

	sorted := append([]string(nil), recipients...)
	slices.Sort(sorted)

	digest := sha256.Sum256([]byte(strings.Join(sorted, "\x00")))

	return hex.EncodeToString(digest[:])
}
