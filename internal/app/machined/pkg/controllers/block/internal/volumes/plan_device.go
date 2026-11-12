// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package volumes

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"slices"

	"github.com/google/uuid"
	"github.com/siderolabs/go-blockdevice/v2/partitioning/gpt"

	blockpb "github.com/siderolabs/talos/pkg/machinery/api/resource/definitions/block"
)

// GPT on-disk layout constants, matching the values used by the go-blockdevice library
// (UEFI mandates 128 entries of 128 bytes, i.e. a 16KiB entry array).
const (
	gptNumEntries  = 128
	gptEntrySize   = 128
	gptEntriesSize = gptNumEntries * gptEntrySize

	gptHeaderSignature = 0x5452415020494645 // "EFI PART"
	gptRevision        = 0x00010000
	gptHeaderSize      = 92

	// minGPTLastLBA is the smallest last LBA accepted by gpt.New/gpt.Read (lastLBA must be >= 33).
	minGPTLastLBA = 33

	minAlignmentBytes = 1048576 // 1MiB, see gpt.Table.init
)

// errUnknownGeometry marks a disk whose snapshot data is insufficient to simulate
// provisioning decisions. The planner reports such disks as Unknown instead of treating
// them as definitively unusable or absent.
var errUnknownGeometry = errors.New("unknown disk geometry")

// checkedMul returns a*b and false when the multiplication overflows uint64.
func checkedMul(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}

	return a * b, true
}

// checkedAdd returns a+b and false when the addition overflows uint64.
func checkedAdd(a, b uint64) (uint64, bool) {
	if a > math.MaxUint64-b {
		return 0, false
	}

	return a + b, true
}

// memoryDeviceHeadBytes bounds the physically backed portion of a memoryDevice: the GPT
// header and the 128-entry array always live in the first sectors, so the rest of the
// (potentially multi-TiB) device only needs logical, zero-filled storage.
const memoryDeviceHeadBytes = 2 * 1024 * 1024

// memoryDevice is an in-memory implementation of the gpt.Device interface.
//
// It allows running the real GPT parsing/allocation code against a snapshot without
// touching any physical block device. Only the head of the device is physically backed;
// reads past it return zeros, which is sufficient because a valid primary GPT never requires
// touching the backup copy. All mutating operations are counted by the embedded spy, so
// planning runs can assert that no device writes happened.
type memoryDevice struct {
	head        []byte
	logicalSize uint64
	sectorSize  uint
	ioSize      uint

	// spy counters: every mutating device operation is recorded.
	writeCalls  int
	syncCalls   int
	kernelCalls int
}

func newMemoryDevice(size, sectorSize, ioSize uint64) *memoryDevice {
	headSize := min(size, memoryDeviceHeadBytes)

	return &memoryDevice{
		head:        make([]byte, headSize),
		logicalSize: size,
		sectorSize:  uint(sectorSize),
		ioSize:      uint(ioSize),
	}
}

// WriteCalls returns the total number of WriteAt calls since construction.
func (d *memoryDevice) WriteCalls() int { return d.writeCalls }

// ResetWriteSpy zeroes the mutating-operation counters, delimiting the read-only
// planning phase from snapshot materialization.
func (d *memoryDevice) ResetWriteSpy() {
	d.writeCalls = 0
	d.syncCalls = 0
	d.kernelCalls = 0
}

// MutatingCalls returns the number of mutating operations since the last spy reset.
func (d *memoryDevice) MutatingCalls() int {
	return d.writeCalls + d.syncCalls + d.kernelCalls
}

// ReadAt implements gpt.Device.
func (d *memoryDevice) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || uint64(off) >= d.logicalSize {
		return 0, fmt.Errorf("read out of range: offset %d", off)
	}

	clear(p)

	if uint64(off) >= uint64(len(d.head)) {
		return len(p), nil
	}

	return copy(p, d.head[off:]), nil
}

// WriteAt implements gpt.Device and records every invocation via the spy.
func (d *memoryDevice) WriteAt(p []byte, off int64) (int, error) {
	d.writeCalls++

	if off < 0 {
		return 0, fmt.Errorf("negative write offset: %d", off)
	}

	end, ok := checkedAdd(uint64(off), uint64(len(p)))
	if !ok || end > d.logicalSize {
		return 0, fmt.Errorf("write out of range: offset %d length %d", off, len(p))
	}

	if uint64(off) < uint64(len(d.head)) {
		copy(d.head[off:], p)
	}

	return len(p), nil
}

// GetSectorSize implements gpt.Device.
func (d *memoryDevice) GetSectorSize() uint { return d.sectorSize }

// GetSize implements gpt.Device.
func (d *memoryDevice) GetSize() uint64 { return d.logicalSize }

// GetIOSize implements gpt.Device.
func (d *memoryDevice) GetIOSize() (uint, error) {
	return d.ioSize, nil
}

// Sync implements gpt.Device (no-op spy).
func (d *memoryDevice) Sync() error {
	d.syncCalls++

	return nil
}

// GetKernelLastPartitionNum implements gpt.Device (no-op spy).
func (d *memoryDevice) GetKernelLastPartitionNum() (int, error) {
	d.kernelCalls++

	return 0, nil
}

// KernelPartitionAdd implements gpt.Device (no-op spy).
func (d *memoryDevice) KernelPartitionAdd(int, uint64, uint64) error {
	d.kernelCalls++

	return nil
}

// KernelPartitionResize implements gpt.Device (no-op spy).
func (d *memoryDevice) KernelPartitionResize(int, uint64, uint64) error {
	d.kernelCalls++

	return nil
}

// KernelPartitionDelete implements gpt.Device (no-op spy).
func (d *memoryDevice) KernelPartitionDelete(int) error {
	d.kernelCalls++

	return nil
}

// simulatedDisk is the in-memory model of a single disk reconstructed from a snapshot.
type simulatedDisk struct {
	spec *blockpb.DiskSpec
	mem  *memoryDevice

	// table is the simulated GPT table; nil for disks without a GPT partition table
	// (empty disks and disks carrying an incompatible signature).
	table *gpt.Table

	// hasGPT/tableName describe the effective state while the simulation runs: allocating the
	// first partition on an empty disk flips them to the GPT state, exactly as writing the
	// fresh GPT would change the blkid probe result on a real device.
	hasGPT    bool
	tableName string
	// observedName is the immutable whole-disk signature captured by the snapshot; it feeds
	// the geometry digest so that in-memory simulation cannot perturb Apply-time revalidation.
	observedName string

	firstUsable  uint64
	lastUsable   uint64
	alignmentLBA uint64
	sectorSize   uint64
}

// FreeRange describes an unallocated, sector-aligned LBA interval.
type FreeRange struct {
	StartLBA uint64
	EndLBA   uint64 // inclusive
	Size     uint64 // bytes
}

func (s *simulatedDisk) alignUp(lba uint64) uint64 {
	return (lba + s.alignmentLBA - 1) / s.alignmentLBA * s.alignmentLBA
}

func (s *simulatedDisk) alignDown(lba uint64) uint64 {
	return lba / s.alignmentLBA * s.alignmentLBA
}

// freeRanges returns the unallocated LBA ranges of the simulated table.
//
// It mirrors the private gpt.Table.allocatableRanges algorithm using the exported
// Partitions() view and the geometry captured when the snapshot was built. The result is
// capped at limit ranges; the boolean result reports whether truncation happened.
func (s *simulatedDisk) freeRanges(limit int) ([]FreeRange, bool) {
	if s.table == nil {
		return nil, false
	}

	partitions := slices.Clone(s.table.Partitions())
	partitions = slices.DeleteFunc(partitions, func(p *gpt.Partition) bool { return p == nil })

	slices.SortFunc(partitions, func(a, b *gpt.Partition) int {
		return cmpUint64(a.FirstLBA, b.FirstLBA)
	})

	var (
		ranges []FreeRange
		lowLBA = s.firstUsable
	)

	addRange := func(upperLBA uint64) {
		alignedLowLBA := s.alignUp(lowLBA)
		if upperLBA > alignedLowLBA {
			sectors, ok := checkedSub(upperLBA, alignedLowLBA)
			if !ok {
				return
			}

			size, ok := checkedMul(sectors, s.sectorSize)
			if !ok {
				return
			}

			ranges = append(ranges, FreeRange{
				StartLBA: alignedLowLBA,
				EndLBA:   upperLBA - 1,
				Size:     size,
			})
		}
	}

	for _, partition := range partitions {
		addRange(partition.FirstLBA)
		// partitions might overlap or be nested, so never move the lower bound backwards
		lowLBA = max(lowLBA, partition.LastLBA+1)
	}

	addRange(s.alignDown(s.lastUsable))

	truncated := false

	if limit > 0 && len(ranges) > limit {
		ranges = ranges[:limit]
		truncated = true
	}

	return ranges, truncated
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func checkedSub(a, b uint64) (uint64, bool) {
	if a < b {
		return 0, false
	}

	return a - b, true
}

// syntheticEntry is a partition entry to place into a synthetic GPT image.
type syntheticEntry struct {
	index    int
	firstLBA uint64
	lastLBA  uint64
	typeUUID uuid.UUID
	partUUID uuid.UUID
	name     string
}

// writeSyntheticGPT writes a minimal, valid primary GPT (header plus 128 entry slots) into
// the memory device so that the real gpt.Read can parse it. Only the primary copy is
// required: gpt.Read falls back to the backup header solely when the primary is absent.
func writeSyntheticGPT(dev *memoryDevice, firstUsable, lastUsable uint64, entries []syntheticEntry) error {
	sectorSize := uint64(dev.sectorSize)
	lastLBA := dev.GetSize()/sectorSize - 1

	entriesBuf := make([]byte, gptEntriesSize)

	for _, entry := range entries {
		if entry.index < 1 || entry.index > gptNumEntries {
			return fmt.Errorf("%w: partition index %d out of GPT range", errUnknownGeometry, entry.index)
		}

		offset := (entry.index - 1) * gptEntrySize
		buf := entriesBuf[offset : offset+gptEntrySize]

		copy(buf[0:16], gptGUID(entry.typeUUID))
		copy(buf[16:32], gptGUID(entry.partUUID))
		binary.LittleEndian.PutUint64(buf[32:40], entry.firstLBA)
		binary.LittleEndian.PutUint64(buf[40:48], entry.lastLBA)
		binary.LittleEndian.PutUint64(buf[48:56], 0) // attributes

		// partition name as UTF-16LE, zero padded
		name := entry.name
		if len(name) > 36 {
			name = name[:36]
		}

		for i, r := range name {
			binary.LittleEndian.PutUint16(buf[56+i*2:58+i*2], uint16(r))
		}
	}

	if _, err := dev.WriteAt(entriesBuf, int64(2)*int64(sectorSize)); err != nil {
		return err
	}

	header := make([]byte, sectorSize)
	binary.LittleEndian.PutUint64(header[0:8], gptHeaderSignature)
	binary.LittleEndian.PutUint32(header[8:12], gptRevision)
	binary.LittleEndian.PutUint32(header[12:16], gptHeaderSize)
	// header CRC at [16:20] left zero while computing the checksum
	binary.LittleEndian.PutUint64(header[24:32], 1)       // my LBA
	binary.LittleEndian.PutUint64(header[32:40], lastLBA) // alternate LBA
	binary.LittleEndian.PutUint64(header[40:48], firstUsable)
	binary.LittleEndian.PutUint64(header[48:56], lastUsable)
	copy(header[56:72], gptGUID(uuid.Nil))          // disk GUID
	binary.LittleEndian.PutUint64(header[72:80], 2) // partition entries LBA
	binary.LittleEndian.PutUint32(header[80:84], gptNumEntries)
	binary.LittleEndian.PutUint32(header[84:88], gptEntrySize)
	binary.LittleEndian.PutUint32(header[88:92], crc32.ChecksumIEEE(entriesBuf))
	binary.LittleEndian.PutUint32(header[16:20], crc32.ChecksumIEEE(header[:gptHeaderSize]))

	if _, err := dev.WriteAt(header, int64(1)*int64(sectorSize)); err != nil {
		return err
	}

	return nil
}

// gptGUID converts an RFC 4122 UUID into the mixed-endian byte order used by GPT entries.
func gptGUID(id uuid.UUID) []byte {
	u := id[:]

	return append(
		[]byte{
			u[3], u[2], u[1], u[0],
			u[5], u[4],
			u[7], u[6],
			u[8], u[9],
		},
		u[10:16]...,
	)
}

// diskGeometry holds the sector geometry derived from a disk snapshot.
type diskGeometry struct {
	sectorSize   uint64
	ioSize       uint64
	firstUsable  uint64
	lastUsable   uint64
	alignmentLBA uint64
}

// validateDiskGeometry checks the reported geometry and derives the GPT reservation
// parameters, matching the layout gpt.New would place on a real device.
func validateDiskGeometry(disk *blockpb.DiskSpec) (diskGeometry, error) {
	if disk.SectorSize == 0 {
		return diskGeometry{}, fmt.Errorf("%w: sector size is not reported for disk %q", errUnknownGeometry, disk.DevPath)
	}

	ss := disk.SectorSize

	if ss&(ss-1) != 0 {
		return diskGeometry{}, fmt.Errorf("%w: sector size %d is not a power of two for disk %q", errUnknownGeometry, disk.SectorSize, disk.DevPath)
	}

	if disk.Size == 0 {
		return diskGeometry{}, fmt.Errorf("%w: disk size is not reported for disk %q", errUnknownGeometry, disk.DevPath)
	}

	lastLBA := disk.Size/ss - 1
	if lastLBA < minGPTLastLBA {
		return diskGeometry{}, fmt.Errorf("%w: disk %q is too small for a GPT table (%d sectors)", errUnknownGeometry, disk.DevPath, lastLBA+1)
	}

	ioSize := disk.IoSize
	if ioSize == 0 {
		ioSize = ss
	}

	alignmentSize := max(ioSize, minAlignmentBytes)

	if _, ok := checkedAdd(alignmentSize, ss-1); !ok {
		return diskGeometry{}, fmt.Errorf("%w: alignment overflow for disk %q", errUnknownGeometry, disk.DevPath)
	}

	alignmentLBA := (alignmentSize + ss - 1) / ss

	lbasForEntries := (gptEntriesSize + disk.SectorSize - 1) / disk.SectorSize
	firstUsable := alignUpLBA(2+lbasForEntries, alignmentLBA)

	secondaryEntriesLBA, ok := checkedSub(lastLBA, lbasForEntries)
	if !ok {
		return diskGeometry{}, fmt.Errorf("%w: secondary GPT entries overflow for disk %q", errUnknownGeometry, disk.DevPath)
	}

	lastUsable, ok := checkedSub(secondaryEntriesLBA, 1)
	if !ok || lastUsable < firstUsable {
		return diskGeometry{}, fmt.Errorf("%w: no usable LBA range for disk %q", errUnknownGeometry, disk.DevPath)
	}

	return diskGeometry{
		sectorSize:   ss,
		ioSize:       ioSize,
		firstUsable:  firstUsable,
		lastUsable:   lastUsable,
		alignmentLBA: alignmentLBA,
	}, nil
}

// buildSimulatedDisk reconstructs the in-memory topology of a single disk from snapshot data.
//
// It never opens a real device: GPT geometry is synthesized into a memory device and parsed
// back with the real gpt.Read, so all subsequent range/allocation decisions run the same
// code as the real provisioning path. Insufficient or inconsistent snapshot data yields
// errUnknownGeometry, which the planner surfaces as an Unknown state.
func buildSimulatedDisk(disk *blockpb.DiskSpec, wholeDV *blockpb.DiscoveredVolumeSpec, parts []*blockpb.DiscoveredVolumeSpec) (*simulatedDisk, error) {
	geometry, err := validateDiskGeometry(disk)
	if err != nil {
		return nil, err
	}

	if wholeDV == nil {
		return nil, fmt.Errorf("%w: whole-disk discovered volume is missing for disk %q", errUnknownGeometry, disk.DevPath)
	}

	mem := newMemoryDevice(disk.Size, disk.SectorSize, geometry.ioSize)

	sim := &simulatedDisk{
		spec:         disk,
		mem:          mem,
		firstUsable:  geometry.firstUsable,
		lastUsable:   geometry.lastUsable,
		alignmentLBA: geometry.alignmentLBA,
		sectorSize:   geometry.sectorSize,
		tableName:    wholeDV.Name,
		observedName: wholeDV.Name,
	}

	if err = sim.materializeTable(wholeDV, parts); err != nil {
		return nil, err
	}

	return sim, nil
}

// materializeTable builds the in-memory GPT table matching the probed signature of the disk.
func (s *simulatedDisk) materializeTable(wholeDV *blockpb.DiscoveredVolumeSpec, parts []*blockpb.DiscoveredVolumeSpec) error {
	switch wholeDV.Name {
	case "":
		// unpartitioned disk: provisioning would create a fresh GPT table
		table, err := gpt.New(s.mem)
		if err != nil {
			return fmt.Errorf("%w: cannot initialize GPT for disk %q: %w", errUnknownGeometry, s.spec.DevPath, err)
		}

		s.table = table
	case gptSignatureName:
		entries, err := buildSyntheticEntries(parts, s.firstUsable, s.lastUsable, s.sectorSize)
		if err != nil {
			return err
		}

		if err = writeSyntheticGPT(s.mem, s.firstUsable, s.lastUsable, entries); err != nil {
			return fmt.Errorf("%w: cannot synthesize GPT for disk %q: %w", errUnknownGeometry, s.spec.DevPath, err)
		}

		table, err := gpt.Read(s.mem)
		if err != nil {
			return fmt.Errorf("%w: synthesized GPT for disk %q is not readable: %w", errUnknownGeometry, s.spec.DevPath, err)
		}

		s.table = table
		s.hasGPT = true
	default:
		// a disk carrying another signature (filesystem, MBR, ...): no GPT simulation,
		// checkDiskFromProbe classifies it as WrongFormat.
	}

	return nil
}

func alignUpLBA(lba, alignment uint64) uint64 {
	return (lba + alignment - 1) / alignment * alignment
}

// buildSyntheticEntries validates the partition records from the snapshot and converts
// them into synthetic GPT entries, rejecting any layout that cannot be faithfully represented.
func buildSyntheticEntries(parts []*blockpb.DiscoveredVolumeSpec, firstUsable, lastUsable, ss uint64) ([]syntheticEntry, error) {
	// partition records in a snapshot are not guaranteed to be ordered; sort them by LBA
	// so the overlap check walks the disk in physical order
	ordered := slices.Clone(parts)
	slices.SortFunc(ordered, func(a, b *blockpb.DiscoveredVolumeSpec) int {
		if c := cmpUint64(a.Offset, b.Offset); c != 0 {
			return c
		}

		return cmpUint64(a.PartitionIndex, b.PartitionIndex)
	})

	result := make([]syntheticEntry, 0, len(ordered))

	seenIndexes := map[int]struct{}{}

	var prevLast uint64

	for idx, part := range ordered {
		entry, lastLBA, err := syntheticEntryFromPart(idx, part, firstUsable, lastUsable, ss)
		if err != nil {
			return nil, err
		}

		if idx > 0 && entry.firstLBA <= prevLast {
			return nil, fmt.Errorf("%w: partition %d at LBA %d overlaps the previous partition ending at %d",
				errUnknownGeometry, idx, entry.firstLBA, prevLast)
		}

		prevLast = lastLBA

		if _, exists := seenIndexes[entry.index]; exists {
			return nil, fmt.Errorf("%w: duplicate GPT entry index %d", errUnknownGeometry, entry.index)
		}

		seenIndexes[entry.index] = struct{}{}

		result = append(result, entry)
	}

	return result, nil
}

// syntheticEntryFromPart validates a single snapshot partition record and converts it into
// a synthetic GPT entry.
func syntheticEntryFromPart(idx int, part *blockpb.DiscoveredVolumeSpec, firstUsable, lastUsable, ss uint64) (syntheticEntry, uint64, error) {
	firstLBA, lastLBA, err := partitionSectorRange(idx, part, firstUsable, lastUsable, ss)
	if err != nil {
		return syntheticEntry{}, 0, err
	}

	if part.PartitionIndex == 0 || part.PartitionIndex > gptNumEntries {
		return syntheticEntry{}, 0, fmt.Errorf("%w: partition %d has invalid GPT entry index %d", errUnknownGeometry, idx, part.PartitionIndex)
	}

	typeUUID, partUUID, err := partitionGUIDs(idx, part)
	if err != nil {
		return syntheticEntry{}, 0, err
	}

	return syntheticEntry{
		index:    int(part.PartitionIndex),
		firstLBA: firstLBA,
		lastLBA:  lastLBA,
		typeUUID: typeUUID,
		partUUID: partUUID,
		name:     part.PartitionLabel,
	}, lastLBA, nil
}

// partitionSectorRange validates the sector geometry of a snapshot partition record.
func partitionSectorRange(idx int, part *blockpb.DiscoveredVolumeSpec, firstUsable, lastUsable, ss uint64) (uint64, uint64, error) {
	if part.Offset%ss != 0 {
		return 0, 0, fmt.Errorf("%w: partition %d offset %d is not aligned to sector size %d", errUnknownGeometry, idx, part.Offset, ss)
	}

	if part.Size == 0 || part.Size%ss != 0 {
		return 0, 0, fmt.Errorf("%w: partition %d size %d is not a non-zero multiple of sector size %d", errUnknownGeometry, idx, part.Size, ss)
	}

	firstLBA := part.Offset / ss

	lastLBA, ok := checkedAdd(firstLBA, part.Size/ss-1)
	if !ok {
		return 0, 0, fmt.Errorf("%w: partition %d end LBA overflows", errUnknownGeometry, idx)
	}

	if firstLBA < firstUsable || lastLBA > lastUsable {
		return 0, 0, fmt.Errorf("%w: partition %d LBA range %d-%d is outside the usable range %d-%d",
			errUnknownGeometry, idx, firstLBA, lastLBA, firstUsable, lastUsable)
	}

	return firstLBA, lastLBA, nil
}

// partitionGUIDs resolves the GPT type/partition UUIDs of a snapshot partition record.
func partitionGUIDs(idx int, part *blockpb.DiscoveredVolumeSpec) (uuid.UUID, uuid.UUID, error) {
	if part.PartitionType == "" {
		// a GPT entry without a type GUID is skipped by gpt.Read, so such a snapshot
		// cannot be faithfully reconstructed
		return uuid.Nil, uuid.Nil, fmt.Errorf("%w: partition %d has no partition type UUID", errUnknownGeometry, idx)
	}

	typeUUID, err := uuid.Parse(part.PartitionType)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("%w: partition %d has invalid type UUID %q: %w", errUnknownGeometry, idx, part.PartitionType, err)
	}

	partUUID := uuid.Nil

	if part.PartitionUuid != "" {
		parsed, parseErr := uuid.Parse(part.PartitionUuid)
		if parseErr == nil {
			partUUID = parsed
		}
	}

	return typeUUID, partUUID, nil
}
