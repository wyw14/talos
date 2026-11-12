// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/siderolabs/go-talos-support/support/bundle/encryption"
)

// Overall archive statuses written into the manifest.
const (
	OverallComplete = "complete"
	OverallPartial  = "partial"
)

// SnapshotNotice is the honest collection-semantics statement carried by every manifest: the
// per-collector results were captured at different moments, so nobody may read the bundle as an
// atomic point-in-time snapshot of the cluster.
const SnapshotNotice = "per-collector results were captured at distinct wall-clock timestamps " +
	"on potentially different nodes; this bundle is not an atomic point-in-time snapshot of the cluster"

// EncryptionSink wraps the final output with the archive encryption layer. The indirection keeps
// the engine unaware of any encryption identity or key material.
type EncryptionSink interface {
	Wrap(dst io.Writer) (io.WriteCloser, error)
}

// PlainSink writes the archive as-is, without encryption.
type PlainSink struct{}

// Wrap implements EncryptionSink.
func (PlainSink) Wrap(dst io.Writer) (io.WriteCloser, error) {
	return nopWriteCloser{dst}, nil
}

// AgeSink encrypts the archive with age using the same option semantics as the default support
// flow. A fresh age stream is created for every assembly, so ciphertext is never resumed or
// concatenated across runs.
type AgeSink struct {
	Options []encryption.Option
}

// Wrap implements EncryptionSink.
func (s AgeSink) Wrap(dst io.Writer) (io.WriteCloser, error) {
	return encryption.Encrypt(dst, s.Options...)
}

// ManifestCounts summarizes the per-artifact states in the manifest.
type ManifestCounts struct {
	Sealed int `json:"sealed"`
	Empty  int `json:"empty"`
	Error  int `json:"error"`
	Total  int `json:"total"`
}

// Manifest is the honest status document embedded into every assembled archive.
type Manifest struct {
	FormatVersion     int              `json:"format_version"`
	Fingerprint       string           `json:"fingerprint"`
	GeneratedAt       string           `json:"generated_at"`
	OverallStatus     string           `json:"overall_status"`
	SnapshotSemantics string           `json:"snapshot_semantics"`
	Counts            ManifestCounts   `json:"counts"`
	Nodes             []string         `json:"nodes"`
	Redaction         map[string]bool  `json:"redaction,omitempty"`
	Encryption        EncryptionConfig `json:"encryption"`
	Artifacts         []ArtifactRecord `json:"artifacts"`
}

// AssemblyResult reports the outcome of an assembly.
type AssemblyResult struct {
	OutputPath string
	Partial    bool
	Manifest   Manifest
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

// Assemble builds the final support archive from the sealed artifacts.
//
// The output file is always created fresh and truncated: encryption streams are not resumable,
// so assembly never appends to an existing ciphertext. On failure the partial output is removed
// and the workspace is retained for another run; on success the workspace is removed only when
// every collector reached a terminal non-error state.
func (e *Engine) Assemble(outputPath string, sink EncryptionSink) (*AssemblyResult, error) {
	cp := e.store.Snapshot()

	records := append([]ArtifactRecord(nil), cp.Artifacts...)
	slices.SortFunc(records, func(a, b ArtifactRecord) int {
		return a.Ordinal - b.Ordinal
	})

	for _, record := range records {
		if err := e.store.VerifyForAssembly(record); err != nil {
			return nil, fmt.Errorf("refusing to assemble: %w", err)
		}
	}

	manifest := e.buildManifest(records)

	output, err := os.OpenFile(outputPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open output archive %q: %w", outputPath, err)
	}

	if err = e.writeArchive(output, sink, records, manifest); err != nil {
		return nil, e.discardOutput(output, outputPath, err)
	}

	if err = output.Sync(); err != nil {
		return nil, e.discardOutput(output, outputPath, fmt.Errorf("failed to sync output archive: %w", err))
	}

	if err = output.Close(); err != nil {
		return nil, e.discardOutput(nil, outputPath, fmt.Errorf("failed to close output archive: %w", err))
	}

	result := &AssemblyResult{
		OutputPath: outputPath,
		Partial:    manifest.OverallStatus == OverallPartial,
		Manifest:   manifest,
	}

	if !result.Partial {
		if err = e.store.Remove(); err != nil {
			return result, fmt.Errorf("archive written but failed to remove capture workspace: %w", err)
		}
	}

	return result, nil
}

// discardOutput closes and removes a partial output file after an assembly failure. The file was
// created or truncated by this assembly, so removing it only ever removes our own output. The
// workspace is deliberately retained for the next run.
func (e *Engine) discardOutput(output *os.File, outputPath string, cause error) error {
	if output != nil {
		_ = output.Close()
	}

	if removeErr := os.Remove(outputPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return errors.Join(cause, fmt.Errorf("failed to remove partial output: %w", removeErr))
	}

	return cause
}

// writeArchive streams the chunks and the manifest through the zip and optional encryption layer,
// closing the layers in the required order.
func (e *Engine) writeArchive(output *os.File, sink EncryptionSink, records []ArtifactRecord, manifest Manifest) error {
	encryptedWriter, err := sink.Wrap(output)
	if err != nil {
		return fmt.Errorf("failed to initialize archive encryption: %w", err)
	}

	zipWriter := zip.NewWriter(encryptedWriter)

	// defense in depth: the run-wide path registry already rejects duplicates while collecting,
	// but never allow two sealed records to collapse onto one archive entry at assembly time
	writtenEntries := map[string]struct{}{}

	for _, record := range records {
		if record.Status != StatusSealed {
			continue
		}

		if _, ok := writtenEntries[record.Path]; ok {
			return fmt.Errorf("%w: %q", ErrPathDuplicate, record.Path)
		}

		writtenEntries[record.Path] = struct{}{}

		if err = e.writeZipEntry(zipWriter, record); err != nil {
			return err
		}
	}

	encodedManifest, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode manifest: %w", err)
	}

	manifestWriter, err := zipWriter.Create(ManifestName)
	if err != nil {
		return fmt.Errorf("failed to create manifest entry: %w", err)
	}

	if _, err = manifestWriter.Write(encodedManifest); err != nil {
		return fmt.Errorf("failed to write manifest entry: %w", err)
	}

	if err = zipWriter.Close(); err != nil {
		return fmt.Errorf("failed to finalize zip archive: %w", err)
	}

	if err = encryptedWriter.Close(); err != nil {
		return fmt.Errorf("failed to flush archive encryption: %w", err)
	}

	return nil
}

// writeZipEntry copies one sealed chunk into its archive entry without loading it fully into
// memory.
func (e *Engine) writeZipEntry(zipWriter *zip.Writer, record ArtifactRecord) error {
	chunk, info, err := e.store.OpenChunk(record)
	if err != nil {
		return err
	}

	defer chunk.Close() //nolint:errcheck

	entry, err := zipWriter.Create(record.Path)
	if err != nil {
		return fmt.Errorf("failed to create archive entry %q: %w", record.Path, err)
	}

	written, err := io.Copy(entry, io.LimitReader(chunk, info.Size()))
	if err != nil {
		return fmt.Errorf("failed to copy chunk for collector %d: %w", record.Ordinal, err)
	}

	if written != record.Length {
		return fmt.Errorf("short chunk for collector %d: copied %d of %d bytes",
			record.Ordinal, written, record.Length)
	}

	return nil
}

func (e *Engine) buildManifest(records []ArtifactRecord) Manifest {
	counts := ManifestCounts{Total: len(e.config.Collectors)}

	for _, record := range records {
		switch record.Status {
		case StatusSealed:
			counts.Sealed++
		case StatusEmpty:
			counts.Empty++
		case StatusError:
			counts.Error++
		}
	}

	overall := OverallComplete
	if counts.Error > 0 || len(records) != counts.Total {
		overall = OverallPartial
	}

	return Manifest{
		FormatVersion:     FormatVersion,
		Fingerprint:       e.store.Fingerprint(),
		GeneratedAt:       timestamp(e.options.Now()),
		OverallStatus:     overall,
		SnapshotSemantics: SnapshotNotice,
		Counts:            counts,
		Nodes:             append([]string(nil), e.config.Nodes...),
		Redaction:         cloneRedaction(e.config.Redaction),
		Encryption:        e.config.Encryption,
		Artifacts:         records,
	}
}

func cloneRedaction(redaction map[string]bool) map[string]bool {
	if len(redaction) == 0 {
		return nil
	}

	cloned := make(map[string]bool, len(redaction))
	for node, supported := range redaction {
		cloned[node] = supported
	}

	return cloned
}
