// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture

// Test-only accessors for package internals. The testpackage linter allows same-package test
// helpers through the export_test.go naming convention.

// SaveCheckpointForTest exposes the atomic checkpoint writer.
func SaveCheckpointForTest(root string, cp *Checkpoint) error {
	return saveCheckpoint(root, cp)
}

// LoadCheckpointForTest exposes the strict checkpoint loader.
func LoadCheckpointForTest(root string, maxCollectorBytes int64) (*Checkpoint, error) {
	return loadCheckpoint(root, maxCollectorBytes)
}

// SealedChunkPathForTest returns the on-disk path of a sealed chunk.
func SealedChunkPathForTest(root string, ordinal int) (string, error) {
	return ResolveWorkspacePath(root, chunkSealedName(ordinal))
}

// PartChunkPathForTest returns the on-disk path of an in-progress chunk.
func PartChunkPathForTest(root string, ordinal int) (string, error) {
	return ResolveWorkspacePath(root, chunkPartName(ordinal))
}

// NewCaptureArchiveForTest exposes the capture archive for direct write-contract tests.
func NewCaptureArchiveForTest(store *Store, part *Part, tracker *PathTracker) interface {
	Write(path string, contents []byte) error
	Close() error
} {
	return &captureArchive{store: store, part: part, tracker: tracker}
}
