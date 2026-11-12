// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package capture

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Limit errors returned while writing a chunk.
var (
	// ErrCollectorLimitExceeded is returned when a single collector result exceeds the configured
	// per-collector byte limit. The collector is recorded as failed, no oversized chunk is sealed.
	ErrCollectorLimitExceeded = errors.New("collector result exceeds the per-collector size limit")
	// ErrDiskBudgetExceeded is returned when the workspace would grow beyond its total byte budget.
	ErrDiskBudgetExceeded = errors.New("capture workspace disk budget exhausted")
	// ErrWorkspaceNotOwned is returned when the configured workspace directory is non-empty but
	// contains no capture layout, so the engine must not create or delete anything inside it.
	ErrWorkspaceNotOwned = errors.New("workspace directory is not a capture workspace")
)

// Store owns the workspace directory: the checkpoint, the sealed chunks and the in-progress
// parts. It never references paths outside its root, and every file it removes is one it created
// from an ordinal-derived name.
type Store struct {
	root        string
	chunksDir   string
	options     *Options
	fingerprint string

	// mu guards the checkpoint, the run-wide byte accounting and concurrent part writes.
	mu         sync.Mutex
	cp         *Checkpoint
	totalBytes int64

	logf func(string, ...any)
}

// Part is an in-progress, private chunk of one collector.
type Part struct {
	ordinal int
	file    *os.File
	hasher  hash.Hash
	written int64
	path    string
	writes  int
	opts    *Options
}

// OpenStore opens (or creates) the workspace for cfg and validates any checkpoint found in it.
//
// A missing, corrupt, unsupported or fingerprint-mismatching checkpoint discards the whole
// workspace: partial reuse across different collection inputs is intentionally impossible.
func OpenStore(cfg RunConfig, options *Options) (*Store, error) {
	if options.WorkspaceDir == "" {
		return nil, errors.New("capture workspace directory is not set")
	}

	if options.MaxCollectorBytes <= 0 || options.MaxDiskBytes <= 0 || options.TTL <= 0 || options.NumWorkers <= 0 {
		return nil, errors.New("capture limits, TTL and worker count must all be positive")
	}

	fingerprint, err := cfg.Fingerprint()
	if err != nil {
		return nil, err
	}

	root, err := filepath.Abs(options.WorkspaceDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve workspace path: %w", err)
	}

	if err = os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create workspace %q: %w", root, err)
	}

	if err = ensureOwnedWorkspace(root); err != nil {
		return nil, err
	}

	chunksDir, err := ResolveWorkspacePath(root, chunksDirName)
	if err != nil {
		return nil, err
	}

	if err = os.MkdirAll(chunksDir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create chunks directory: %w", err)
	}

	store := &Store{
		root:        root,
		chunksDir:   chunksDir,
		options:     options,
		fingerprint: fingerprint,
		cp:          newCheckpoint(fingerprint, options.Now()),
		logf:        makeLogger(options),
	}

	if err = store.adoptOrDiscard(options.MaxCollectorBytes); err != nil {
		return nil, err
	}

	return store, nil
}

// adoptOrDiscard loads the checkpoint when it matches the current inputs, and resets the
// workspace otherwise.
func (s *Store) adoptOrDiscard(maxCollectorBytes int64) error {
	cp, err := loadCheckpoint(s.root, maxCollectorBytes)

	switch {
	case err == nil:
		if cp.Fingerprint != s.fingerprint {
			s.logf("discarding capture checkpoint: collection inputs changed (fingerprint %s -> %s)",
				cp.Fingerprint, s.fingerprint)

			return s.reset(cp)
		}

		s.cp = cp
	case errors.Is(err, os.ErrNotExist):
		// first run in this workspace, nothing to adopt
	default:
		s.logf("discarding capture checkpoint: %s", err)

		if resetErr := s.reset(nil); resetErr != nil {
			return resetErr
		}
	}

	return s.reconcileChunks()
}

// reset wipes the workspace contents and installs a fresh checkpoint.
func (s *Store) reset(_ *Checkpoint) error {
	if err := clearDirectory(s.root); err != nil {
		return fmt.Errorf("failed to reset capture workspace: %w", err)
	}

	if err := os.MkdirAll(s.chunksDir, 0o700); err != nil {
		return fmt.Errorf("failed to recreate chunks directory: %w", err)
	}

	s.cp = newCheckpoint(s.fingerprint, s.options.Now())
	s.totalBytes = 0

	return saveCheckpoint(s.root, s.cp)
}

// reconcileChunks removes orphaned parts (left behind by a killed process) and recomputes the
// disk accounting from the sealed chunk files actually present.
func (s *Store) reconcileChunks() error {
	entries, err := os.ReadDir(s.chunksDir)
	if err != nil {
		return fmt.Errorf("failed to scan chunks directory: %w", err)
	}

	var total int64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		switch filepath.Ext(name) {
		case ".part":
			// parts are never reusable; a surviving one means the previous process died mid-write
			if err = os.Remove(filepath.Join(s.chunksDir, name)); err != nil {
				return fmt.Errorf("failed to remove orphaned part %q: %w", name, err)
			}
		case ".chunk":
			info, statErr := entry.Info()
			if statErr != nil {
				return fmt.Errorf("failed to stat chunk %q: %w", name, statErr)
			}

			total += info.Size()
		}
	}

	s.totalBytes = total

	return nil
}

// CreatePart opens a private in-progress chunk file for the ordinal, rejecting the attempt up
// front when the disk budget is already exhausted.
//
// A rerun means the ordinal's previous artifact, if any, was not reusable: its stale sealed chunk
// is removed here so a failed rerun can never leave unreferenced bytes behind. The checkpoint
// still points at the old chunk until the new record is persisted, so a crash in between simply
// makes the next run rerun the collector again.
func (s *Store) CreatePart(ordinal int) (*Part, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.removeStaleChunkLocked(ordinal); err != nil {
		return nil, err
	}

	if s.totalBytes >= s.options.MaxDiskBytes {
		return nil, fmt.Errorf("%w: %d bytes in use", ErrDiskBudgetExceeded, s.totalBytes)
	}

	partName, err := ResolveWorkspacePath(s.root, chunkPartName(ordinal))
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(partName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to create part for collector %d: %w", ordinal, err)
	}

	return &Part{
		ordinal: ordinal,
		file:    file,
		hasher:  sha256.New(),
		opts:    s.options,
	}, nil
}

// removeStaleChunkLocked deletes an unreusable sealed chunk of the ordinal and drops its size
// from the disk accounting. Callers must hold s.mu.
func (s *Store) removeStaleChunkLocked(ordinal int) error {
	sealedName, err := ResolveWorkspacePath(s.root, chunkSealedName(ordinal))
	if err != nil {
		return err
	}

	info, err := os.Stat(sealedName)
	switch {
	case err == nil:
		if err = os.Remove(sealedName); err != nil {
			return fmt.Errorf("failed to remove stale chunk for collector %d: %w", ordinal, err)
		}

		s.totalBytes -= info.Size()

		if s.totalBytes < 0 {
			s.totalBytes = 0
		}
	case errors.Is(err, os.ErrNotExist):
		// nothing stale to remove
	default:
		return fmt.Errorf("failed to stat stale chunk for collector %d: %w", ordinal, err)
	}

	return nil
}

// Write streams one collector payload into the part, enforcing both the per-collector limit and
// the workspace-wide disk budget while updating the running digest and counters.
func (p *Part) Write(s *Store, entryPath string, contents []byte) error {
	size := int64(len(contents))

	if p.written+size > p.opts.MaxCollectorBytes {
		return fmt.Errorf("%w: collector %d wrote %d bytes, limit is %d",
			ErrCollectorLimitExceeded, p.ordinal, p.written+size, p.opts.MaxCollectorBytes)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.totalBytes+size > s.options.MaxDiskBytes {
		return fmt.Errorf("%w: need %d more bytes, %d of %d in use",
			ErrDiskBudgetExceeded, size, s.totalBytes, s.options.MaxDiskBytes)
	}

	if _, err := p.hasher.Write(contents); err != nil {
		return fmt.Errorf("failed to hash collector %d output: %w", p.ordinal, err)
	}

	if _, err := p.file.Write(contents); err != nil {
		return fmt.Errorf("failed to write collector %d chunk: %w", p.ordinal, err)
	}

	p.written += size
	s.totalBytes += size
	p.path = entryPath
	p.writes++

	return nil
}

// Seal finalizes the part: the file is synced, closed and atomically renamed to the sealed chunk
// name, then the record replaces any previous entry of the same ordinal in the checkpoint.
func (s *Store) Seal(p *Part, identity CollectorIdentity, startedAt, finishedAt time.Time) (ArtifactRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if p.writes == 0 {
		return ArtifactRecord{}, errors.New("cannot seal an empty part")
	}

	if err := p.file.Sync(); err != nil {
		_ = p.file.Close()

		return ArtifactRecord{}, fmt.Errorf("failed to sync part for collector %d: %w", p.ordinal, err)
	}

	if err := p.file.Close(); err != nil {
		return ArtifactRecord{}, fmt.Errorf("failed to close part for collector %d: %w", p.ordinal, err)
	}

	partPath, err := ResolveWorkspacePath(s.root, chunkPartName(p.ordinal))
	if err != nil {
		return ArtifactRecord{}, err
	}

	sealedPath, err := ResolveWorkspacePath(s.root, chunkSealedName(p.ordinal))
	if err != nil {
		return ArtifactRecord{}, err
	}

	if err = os.Rename(partPath, sealedPath); err != nil {
		return ArtifactRecord{}, fmt.Errorf("failed to seal chunk for collector %d: %w", p.ordinal, err)
	}

	syncDirectory(s.chunksDir)

	digest := hex.EncodeToString(p.hasher.Sum(nil))

	record := ArtifactRecord{
		Ordinal:    p.ordinal,
		Source:     identity.Source,
		Name:       identity.Name,
		Path:       p.path,
		Status:     StatusSealed,
		StartedAt:  timestamp(startedAt),
		FinishedAt: timestamp(finishedAt),
		Length:     p.written,
		SHA256:     digest,
		Chunk:      chunkSealedName(p.ordinal),
	}

	if err = s.upsertRecordLocked(record); err != nil {
		return ArtifactRecord{}, err
	}

	return record, nil
}

// Abort closes and removes the part without sealing it. Used when a collector fails.
func (s *Store) Abort(p *Part) {
	if p == nil {
		return
	}

	_ = p.file.Close()

	path, err := ResolveWorkspacePath(s.root, chunkPartName(p.ordinal))
	if err == nil {
		_ = os.Remove(path)
	}

	s.mu.Lock()
	s.totalBytes -= p.written
	if s.totalBytes < 0 {
		s.totalBytes = 0
	}
	s.mu.Unlock()
}

// RecordEmpty persists an empty collector result without creating a chunk.
func (s *Store) RecordEmpty(identity CollectorIdentity, startedAt, finishedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := ArtifactRecord{
		Ordinal:    identity.Ordinal,
		Source:     identity.Source,
		Name:       identity.Name,
		Status:     StatusEmpty,
		StartedAt:  timestamp(startedAt),
		FinishedAt: timestamp(finishedAt),
	}

	return s.upsertRecordLocked(record)
}

// RecordError persists a failed collector result and removes its part if one was opened.
func (s *Store) RecordError(identity CollectorIdentity, startedAt, finishedAt time.Time, part *Part, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if part != nil {
		_ = part.file.Close()

		if path, err := ResolveWorkspacePath(s.root, chunkPartName(part.ordinal)); err == nil {
			_ = os.Remove(path)
		}

		s.totalBytes -= part.written
		if s.totalBytes < 0 {
			s.totalBytes = 0
		}
	}

	record := ArtifactRecord{
		Ordinal:    identity.Ordinal,
		Source:     identity.Source,
		Name:       identity.Name,
		Path:       partPath(part),
		Status:     StatusError,
		StartedAt:  timestamp(startedAt),
		FinishedAt: timestamp(finishedAt),
		Error:      cause.Error(),
	}

	return s.upsertRecordLocked(record)
}

// Reusable reports whether a previously persisted record can be reused instead of rerunning the
// collector. It never allocates a length-sized buffer: digest verification is streamed.
func (s *Store) Reusable(record ArtifactRecord) bool {
	switch record.Status {
	case StatusEmpty:
		return s.withinTTL(record)
	case StatusSealed:
		if !s.withinTTL(record) {
			return false
		}

		ok, err := s.verifyChunk(record)
		if err != nil {
			s.logf("sealed artifact for collector %d failed verification, rerunning: %s", record.Ordinal, err)

			return false
		}

		return ok
	default:
		return false
	}
}

// VerifyForAssembly re-checks a sealed record immediately before its content is copied into the
// final archive. TTL does not apply here: the artifacts were accepted for this run already.
func (s *Store) VerifyForAssembly(record ArtifactRecord) error {
	if record.Status != StatusSealed {
		return nil
	}

	ok, err := s.verifyChunk(record)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("chunk verification failed for collector %d", record.Ordinal)
	}

	return nil
}

// OpenChunk opens the sealed chunk for streaming reads.
func (s *Store) OpenChunk(record ArtifactRecord) (io.ReadCloser, os.FileInfo, error) {
	chunkPath, err := ResolveWorkspacePath(s.root, record.Chunk)
	if err != nil {
		return nil, nil, err
	}

	info, err := os.Stat(chunkPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to stat chunk for collector %d: %w", record.Ordinal, err)
	}

	if info.Size() != record.Length {
		return nil, nil, fmt.Errorf("chunk size mismatch for collector %d: got %d, recorded %d",
			record.Ordinal, info.Size(), record.Length)
	}

	file, err := os.Open(chunkPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open chunk for collector %d: %w", record.Ordinal, err)
	}

	return file, info, nil
}

// Record returns the persisted record for an ordinal, if any.
func (s *Store) Record(ordinal int) (ArtifactRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, record := range s.cp.Artifacts {
		if record.Ordinal == ordinal {
			return record, true
		}
	}

	return ArtifactRecord{}, false
}

// Snapshot returns a deep-enough copy of the checkpoint for read-only consumers.
func (s *Store) Snapshot() Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := *s.cp
	cp.Artifacts = append([]ArtifactRecord(nil), s.cp.Artifacts...)

	return cp
}

// Fingerprint returns the collection input fingerprint of the open store.
func (s *Store) Fingerprint() string {
	return s.fingerprint
}

// Remove deletes the whole workspace after a fully successful assembly.
func (s *Store) Remove() error {
	return os.RemoveAll(s.root)
}

// Root returns the workspace root path.
func (s *Store) Root() string {
	return s.root
}

// verifyChunk streams the chunk through SHA-256 and compares size and digest with the record.
func (s *Store) verifyChunk(record ArtifactRecord) (bool, error) {
	if record.Length < 0 || record.Length > s.options.MaxCollectorBytes {
		return false, fmt.Errorf("recorded length %d out of bounds", record.Length)
	}

	if err := validateStoredChunkName(record.Ordinal, record.Chunk); err != nil {
		return false, err
	}

	chunkPath, err := ResolveWorkspacePath(s.root, record.Chunk)
	if err != nil {
		return false, err
	}

	info, err := os.Stat(chunkPath)
	if err != nil {
		return false, fmt.Errorf("chunk unavailable: %w", err)
	}

	if info.Size() != record.Length {
		return false, nil
	}

	file, err := os.Open(chunkPath)
	if err != nil {
		return false, fmt.Errorf("chunk unreadable: %w", err)
	}

	defer file.Close() //nolint:errcheck

	hasher := sha256.New()

	// LimitReader makes a malformed size header unable to drive an unbounded read.
	if _, err = io.Copy(hasher, io.LimitReader(file, record.Length+1)); err != nil {
		return false, fmt.Errorf("chunk digest computation failed: %w", err)
	}

	if hex.EncodeToString(hasher.Sum(nil)) != record.SHA256 {
		return false, nil
	}

	return true, nil
}

func (s *Store) withinTTL(record ArtifactRecord) bool {
	finishedAt, err := time.Parse(time.RFC3339Nano, record.FinishedAt)
	if err != nil {
		s.logf("invalid finished_at %q for collector %d", record.FinishedAt, record.Ordinal)

		return false
	}

	return s.options.Now().Sub(finishedAt) <= s.options.TTL
}

// upsertRecordLocked replaces (or appends) the record of an ordinal and persists the checkpoint
// immediately, so the death of the process cannot lose a sealed result.
func (s *Store) upsertRecordLocked(record ArtifactRecord) error {
	for i, existing := range s.cp.Artifacts {
		if existing.Ordinal == record.Ordinal {
			s.cp.Artifacts[i] = record

			return saveCheckpoint(s.root, s.cp)
		}
	}

	s.cp.Artifacts = append(s.cp.Artifacts, record)

	return saveCheckpoint(s.root, s.cp)
}

func newCheckpoint(fingerprint string, now time.Time) *Checkpoint {
	return &Checkpoint{
		FormatVersion: FormatVersion,
		Fingerprint:   fingerprint,
		CreatedAt:     timestamp(now),
		Artifacts:     []ArtifactRecord{},
	}
}

func partPath(part *Part) string {
	if part == nil {
		return ""
	}

	return part.path
}

// ensureOwnedWorkspace makes sure an existing, non-empty workspace directory was created by this
// package: the checkpoint file or the chunks directory are the ownership markers. This refuses to
// take over a user directory pointed at through --capture-dir, since reset and cleanup would
// otherwise remove its unrelated contents.
func ensureOwnedWorkspace(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("failed to inspect workspace %q: %w", root, err)
	}

	if len(entries) == 0 {
		return nil
	}

	for _, entry := range entries {
		if entry.Name() == checkpointName || (entry.IsDir() && entry.Name() == chunksDirName) {
			return nil
		}
	}

	return fmt.Errorf("%w: %q is non-empty but has no %s or %s marker",
		ErrWorkspaceNotOwned, root, checkpointName, chunksDirName)
}

// clearDirectory removes every direct child of dir while keeping dir itself. Only names returned
// by ReadDir are ever removed, so the operation is confined to the directory contents.
func clearDirectory(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err = os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}

	return nil
}

func makeLogger(options *Options) func(string, ...any) {
	return func(format string, args ...any) {
		if options.LogOutput == nil {
			return
		}

		msg := fmt.Sprintf(format, args...)
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}

		_, _ = io.WriteString(options.LogOutput, msg)
	}
}
