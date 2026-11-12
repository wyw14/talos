// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/siderolabs/talos/pkg/safepath"
)

// Archive path rejection reasons. They are wrapped with the offending path at the call site.
var (
	ErrPathEmpty       = errors.New("archive path is empty")
	ErrPathAbsolute    = errors.New("archive path must be relative")
	ErrPathTraversal   = errors.New("archive path must not contain traversal segments")
	ErrPathControlChar = errors.New("archive path must not contain control characters")
	ErrPathVolume      = errors.New("archive path must not contain a volume name")
	ErrPathDuplicate   = errors.New("duplicate archive path")
	ErrPathChunkEscape = errors.New("chunk path escapes the workspace")
)

// ValidateArchivePath validates a path reported by a collector before it is used as an entry
// name in the final archive and as a key in the run-wide path registry.
//
// The check is deliberately lexical and stricter than what a zip reader accepts: only cleaned
// relative paths without volume names, traversal segments or control characters are allowed.
// Both slash and backslash are treated as separators so the rule is identical on Windows and
// Linux, regardless of where the bundle is assembled or extracted.
func ValidateArchivePath(entryPath string) error {
	if entryPath == "" {
		return ErrPathEmpty
	}

	for _, r := range entryPath {
		if r < 0x20 {
			return fmt.Errorf("%w: %q", ErrPathControlChar, entryPath)
		}
	}

	// filepath.VolumeName catches "C:" and UNC roots on Windows and is a no-op on Linux; the
	// explicit colon check makes volume-qualified names fail on every platform.
	if filepath.VolumeName(entryPath) != "" || strings.ContainsRune(entryPath, ':') {
		return fmt.Errorf("%w: %q", ErrPathVolume, entryPath)
	}

	if filepath.IsAbs(entryPath) || strings.HasPrefix(entryPath, "/") || strings.HasPrefix(entryPath, `\`) {
		return fmt.Errorf("%w: %q", ErrPathAbsolute, entryPath)
	}

	segments := strings.FieldsFunc(entryPath, func(r rune) bool {
		return r == '/' || r == '\\'
	})

	for _, segment := range segments {
		if segment == ".." {
			return fmt.Errorf("%w: %q", ErrPathTraversal, entryPath)
		}
	}

	// CleanPath collapses any remaining "." segments and proves the lexical result stays
	// relative. An empty result means the input only contained dots.
	cleaned := safepath.CleanPath(entryPath)
	if cleaned == "" || cleaned == "." || filepath.IsAbs(cleaned) {
		return fmt.Errorf("%w: %q", ErrPathTraversal, entryPath)
	}

	return nil
}

// PathTracker registers archive paths for the duration of one collection run and rejects
// duplicates, so two collectors can never silently overwrite the same archive entry.
type PathTracker struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewPathTracker creates an empty tracker.
func NewPathTracker() *PathTracker {
	return &PathTracker{seen: map[string]struct{}{}}
}

// Register validates and remembers entryPath. A second registration of the same cleaned path
// returns an error even if the spelling differs.
func (t *PathTracker) Register(entryPath string) error {
	if err := ValidateArchivePath(entryPath); err != nil {
		return err
	}

	cleaned := path.Clean(strings.ReplaceAll(entryPath, `\`, "/"))

	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.seen[cleaned]; ok {
		return fmt.Errorf("%w: %q", ErrPathDuplicate, cleaned)
	}

	t.seen[cleaned] = struct{}{}

	return nil
}

// chunkPartName returns the workspace-relative name of the in-progress chunk for the ordinal.
func chunkPartName(ordinal int) string {
	return path.Join(chunksDirName, fmt.Sprintf("%d.part", ordinal))
}

// chunkSealedName returns the workspace-relative name of the sealed chunk for the ordinal.
func chunkSealedName(ordinal int) string {
	return path.Join(chunksDirName, fmt.Sprintf("%d.chunk", ordinal))
}

// ResolveWorkspacePath joins root and rel while guaranteeing the result stays inside root.
//
// rel must be a cleaned relative path; chunk names are only ever derived from an ordinal, and
// checkpoint names are package constants, so an external value can never reach here. The guard
// still exists so a corrupted checkpoint cannot trick the store into touching an outside file.
func ResolveWorkspacePath(root, rel string) (string, error) {
	if filepath.IsAbs(rel) || filepath.VolumeName(rel) != "" {
		return "", fmt.Errorf("%w: %q", ErrPathChunkEscape, rel)
	}

	cleaned := filepath.Clean(rel)
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %q", ErrPathChunkEscape, rel)
	}

	return filepath.Join(root, cleaned), nil
}

// validateStoredChunkName is used when loading a checkpoint: the recorded chunk name must have
// the exact ordinal-derived shape, never an attacker-controlled path.
func validateStoredChunkName(ordinal int, rel string) error {
	if rel == "" {
		return nil
	}

	expected := chunkSealedName(ordinal)
	if filepath.ToSlash(filepath.Clean(rel)) != expected {
		return fmt.Errorf("%w: %q does not match %q", ErrPathChunkEscape, rel, expected)
	}

	return nil
}
