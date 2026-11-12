// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package volumes

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/google/uuid"
	"github.com/siderolabs/gen/value"
	"github.com/siderolabs/go-blockdevice/v2/partitioning/gpt"
	"go.uber.org/zap"

	blockpb "github.com/siderolabs/talos/pkg/machinery/api/resource/definitions/block"
	"github.com/siderolabs/talos/pkg/machinery/cel/celenv"
	"github.com/siderolabs/talos/pkg/machinery/resources/block"
)

// Default planning limits, bounding the amount of topology a single planning run will process.
const (
	defaultMaxDisks   = 256
	defaultMaxVolumes = 256
	defaultMaxRanges  = 128
)

// PlanLimits caps the topology processed by a planning run.
//
// Zero values are replaced by the defaults from DefaultPlanLimits, so a plan can never grow
// unbounded regardless of the snapshot size.
type PlanLimits struct {
	// MaxDisks limits the number of disk resources considered.
	MaxDisks int
	// MaxVolumes limits the number of volume configs planned in a single run.
	MaxVolumes int
	// MaxRanges limits the number of free LBA ranges reported per disk candidate.
	MaxRanges int
}

// DefaultPlanLimits returns the default planning limits.
func DefaultPlanLimits() PlanLimits {
	return PlanLimits{
		MaxDisks:   defaultMaxDisks,
		MaxVolumes: defaultMaxVolumes,
		MaxRanges:  defaultMaxRanges,
	}
}

func (l PlanLimits) orDefault() PlanLimits {
	if l.MaxDisks <= 0 {
		l.MaxDisks = defaultMaxDisks
	}

	if l.MaxVolumes <= 0 {
		l.MaxVolumes = defaultMaxVolumes
	}

	if l.MaxRanges <= 0 {
		l.MaxRanges = defaultMaxRanges
	}

	return l
}

// PlanInput is the frozen topology fed into a read-only allocation planning run.
//
// All data is captured by value at construction time; planning never re-reads or opens
// the devices described here.
type PlanInput struct {
	// VolumeConfigs to plan for. They may be supplied in any order; the planner processes
	// them in the exact CompareVolumeConfigs order used by the volume manager.
	VolumeConfigs []*block.VolumeConfig
	// DiscoveredVolumes mirrors the DiscoveredVolume resources published by discovery.
	DiscoveredVolumes []*blockpb.DiscoveredVolumeSpec
	// Disks mirrors the Disk resources published by the disk controller.
	Disks []DiskContext
	// DevicesReady mirrors DiscoveredVolumesStatus.Ready.
	DevicesReady bool
	// DiskVersions optionally binds disk DevPaths to the resource versions captured with the
	// snapshot, enabling Apply-time revalidation.
	DiskVersions map[string]resource.Version
	// Limits bounds the processed topology; zero values select the defaults.
	Limits PlanLimits
}

// PlanDecision describes what the planner determined for a single volume.
type PlanDecision string

// Planning decisions.
const (
	// DecisionReuseExisting means an existing discovered volume matches the locator;
	// no new partition or disk allocation is required.
	DecisionReuseExisting PlanDecision = "reuse_existing"
	// DecisionAllocateNew means a new whole-disk or partition allocation is planned.
	DecisionAllocateNew PlanDecision = "allocate_new"
	// DecisionWaiting means discovery is incomplete or a previous provisioning wave has not
	// finished; the outcome cannot be determined yet.
	DecisionWaiting PlanDecision = "waiting"
	// DecisionMissing means no volume matches the locator and the config has no provisioning spec.
	DecisionMissing PlanDecision = "missing"
	// DecisionUnknown means the available geometry/discovery data is insufficient to decide
	// whether provisioning is possible.
	DecisionUnknown PlanDecision = "unknown"
	// DecisionNoCandidate means all candidates were evaluated with known geometry and rejected.
	DecisionNoCandidate PlanDecision = "no_candidate"
	// DecisionNotApplicable means the volume type never allocates block storage (tmpfs, ...).
	DecisionNotApplicable PlanDecision = "not_applicable"
	// DecisionError means the volume configuration is invalid; see ErrorMessage.
	DecisionError PlanDecision = "error"
)

// DiskIdentityKind enumerates the source used to identify a disk.
type DiskIdentityKind string

// Disk identity kinds, ordered from most to least stable.
const (
	IdentityKindWWID    DiskIdentityKind = "wwid"
	IdentityKindUUID    DiskIdentityKind = "uuid"
	IdentityKindSerial  DiskIdentityKind = "serial"
	IdentityKindDevPath DiskIdentityKind = "devpath"
)

// DiskIdentity is a non-sensitive handle to a disk across snapshots.
//
// The raw stable identifier (WWID/UUID/serial) is never embedded: only a non-reversible
// digest and a short suffix are kept, so a plan can be logged or exported without leaking
// device serials.
type DiskIdentity struct {
	// DevPath is the device path observed in the snapshot (not stable by itself).
	DevPath string
	// Kind is the identifier kind the digest was derived from.
	Kind DiskIdentityKind
	// Stable reports whether a hardware-backed identifier was available.
	Stable bool
	// Digest is a non-reversible digest of the full stable identifier.
	Digest string
	// Suffix keeps the last four characters of the identifier for human correlation.
	Suffix string
}

// PartitionRange is an occupied sector interval on a disk.
type PartitionRange struct {
	Offset uint64
	Size   uint64
}

// GeometrySummary is the digestable geometry of a disk captured by a snapshot.
type GeometrySummary struct {
	Size       uint64
	SectorSize uint
	IOSize     uint
	Readonly   bool
	// TableName is the whole-disk probe signature ("gpt", "" for empty, ...).
	TableName string
	// Partitions are the occupied ranges, ordered by offset.
	Partitions []PartitionRange
}

// Digest returns a stable, non-reversible digest of the geometry.
func (g GeometrySummary) Digest() string {
	h := fnv.New64a()

	writeField := func(name string, value any) {
		fmt.Fprintf(h, "%s=%v;", name, value)
	}

	writeField("size", g.Size)
	writeField("sectorSize", g.SectorSize)
	writeField("ioSize", g.IOSize)
	writeField("readonly", g.Readonly)
	writeField("table", g.TableName)

	for _, p := range g.Partitions {
		writeField("partOffset", p.Offset)
		writeField("partSize", p.Size)
	}

	return fmt.Sprintf("%016x", h.Sum64())
}

// AlignmentInfo describes the predicted partition alignment.
type AlignmentInfo struct {
	SectorSize        uint
	AlignmentBytes    uint64
	PredictedFirstLBA uint64
	Aligned           bool
}

// CandidateEvaluation records why a disk was or was not selectable for a volume.
type CandidateEvaluation struct {
	Disk            DiskIdentity
	MatchedSelector bool
	GeometryKnown   bool
	Readonly        bool
	DiskSize        uint64
	SectorSize      uint
	// AvailableBytes is the largest contiguous free range in bytes (zero if unknown).
	AvailableBytes uint64
	RequestedMin   uint64
	RequestedMax   uint64
	Alignment      AlignmentInfo
	// FreeRanges lists the unallocated ranges, capped by PlanLimits.MaxRanges.
	FreeRanges []FreeRange
	// RangesTruncated reports that FreeRanges was cut at the reporting limit.
	RangesTruncated bool
	Rejected        bool
	RejectedReason  DiskRejectedReason
	// Exclusion is a human-readable exclusion detail, empty when the candidate is viable.
	Exclusion string
}

// ReusePlan describes an existing discovered volume selected by the locator.
type ReusePlan struct {
	DevPath        string
	ParentDevPath  string
	PartitionIndex int
	UUID           string
	PartitionUUID  string
	Size           uint64
}

// AllocationPlan describes a predicted new allocation.
type AllocationPlan struct {
	Disk           DiskIdentity
	DiskVersion    resource.Version
	GeometryDigest string
	// Kind is the volume type the allocation satisfies (disk or partition).
	Kind block.VolumeType
	// HasGPT reports whether the disk already carries a GPT table.
	HasGPT bool
	// Partition fields; zero for whole-disk allocations.
	PartitionIndex int
	FirstLBA       uint64
	LastLBA        uint64
	Size           uint64
	SectorSize     uint
	AlignmentBytes uint64
	// AvailableBytes is the largest contiguous free range observed while planning.
	AvailableBytes uint64
}

// VolumePlan is the per-volume result of a planning run.
type VolumePlan struct {
	VolumeID            string
	VolumeConfigVersion resource.Version
	Type                block.VolumeType
	Decision            PlanDecision
	// EncryptionEnabled only records that encryption is configured; key material never
	// enters a plan.
	EncryptionEnabled bool

	// Reuse is set iff Decision == DecisionReuseExisting.
	Reuse *ReusePlan
	// Allocation is set iff Decision == DecisionAllocateNew.
	Allocation *AllocationPlan

	// Candidates lists every disk evaluated for the volume, in snapshot order.
	Candidates []CandidateEvaluation
	// Warnings carries non-fatal planning observations.
	Warnings []string
	// UnknownReasons explains why the decision is DecisionUnknown/DecisionWaiting.
	UnknownReasons []string
	// ErrorMessage carries the configuration error for DecisionError.
	ErrorMessage string
}

// VolumeAllocationPlan is the read-only allocation plan produced from a fixed snapshot.
//
// The plan is advisory: it predicts the choices the real provisioning path would make given
// the same topology, but it cannot guarantee executability if the block topology changes
// concurrently. Callers must re-validate the plan with Validate before applying it.
type VolumeAllocationPlan struct {
	DevicesReady bool
	// Truncated reports that input limits were hit while building the snapshot.
	Truncated bool
	Warnings  []string
	Volumes   []VolumePlan

	snapshot *Snapshot
}

// Snapshot is an immutable, read-only point-in-time view of the block topology.
type Snapshot struct {
	devicesReady bool
	limits       PlanLimits
	truncated    bool
	warnings     []string

	volumeConfigs []*block.VolumeConfig
	disks         []DiskContext
	diskVersions  map[string]resource.Version

	discovered []*blockpb.DiscoveredVolumeSpec
	wholeDVs   map[string]*blockpb.DiscoveredVolumeSpec
	parts      map[string][]*blockpb.DiscoveredVolumeSpec

	topology    map[string]*simulatedDisk
	unknownDisk map[string]string
}

// NewSnapshot freezes the planning input into an immutable, validated snapshot.
//
// Disk geometry is simulated in memory from the discovered topology; disks whose geometry
// cannot be reconstructed are retained and marked as unknown rather than dropped, so a
// partial discovery never makes a disk look definitively absent.
func NewSnapshot(input PlanInput) (*Snapshot, error) {
	limits := input.Limits.orDefault()

	snapshot := &Snapshot{
		devicesReady: input.DevicesReady,
		limits:       limits,
		diskVersions: map[string]resource.Version{},
		wholeDVs:     map[string]*blockpb.DiscoveredVolumeSpec{},
		parts:        map[string][]*blockpb.DiscoveredVolumeSpec{},
		topology:     map[string]*simulatedDisk{},
		unknownDisk:  map[string]string{},
	}

	maps.Copy(snapshot.diskVersions, input.DiskVersions)

	if err := snapshot.validateVolumeConfigs(input.VolumeConfigs); err != nil {
		return nil, err
	}

	snapshot.captureVolumeConfigs(input.VolumeConfigs)
	snapshot.groupDiscoveredVolumes(input.DiscoveredVolumes)

	if err := snapshot.captureDisks(input.Disks); err != nil {
		return nil, err
	}

	return snapshot, nil
}

func (s *Snapshot) validateVolumeConfigs(configs []*block.VolumeConfig) error {
	seenVolumeIDs := map[string]struct{}{}

	for _, vc := range configs {
		if vc == nil {
			return errors.New("snapshot contains a nil volume config")
		}

		id := vc.Metadata().ID()
		if _, exists := seenVolumeIDs[id]; exists {
			return fmt.Errorf("duplicate volume config ID in snapshot: %q", id)
		}

		seenVolumeIDs[id] = struct{}{}
	}

	return nil
}

func (s *Snapshot) captureVolumeConfigs(configs []*block.VolumeConfig) {
	if len(configs) > s.limits.MaxVolumes {
		s.truncated = true
		s.warnings = append(s.warnings,
			fmt.Sprintf("volume limit %d exceeded; planning only the first %d of %d volume configs",
				s.limits.MaxVolumes, s.limits.MaxVolumes, len(configs)))

		s.volumeConfigs = slices.Clone(configs[:s.limits.MaxVolumes])

		return
	}

	s.volumeConfigs = slices.Clone(configs)
}

func (s *Snapshot) groupDiscoveredVolumes(discovered []*blockpb.DiscoveredVolumeSpec) {
	s.discovered = slices.Clone(discovered)

	for _, dv := range s.discovered {
		if dv.ParentDevPath == "" {
			if dv.DevPath != "" {
				s.wholeDVs[dv.DevPath] = dv
			}

			continue
		}

		s.parts[dv.ParentDevPath] = append(s.parts[dv.ParentDevPath], dv)
	}
}

func (s *Snapshot) captureDisks(disks []DiskContext) error {
	for idx, diskCtx := range disks {
		if diskCtx.Disk == nil || diskCtx.Disk.DevPath == "" {
			s.warnings = append(s.warnings, fmt.Sprintf("disk entry %d has no device path; skipped", idx))

			continue
		}

		if len(s.disks) >= s.limits.MaxDisks {
			s.truncated = true
			s.warnings = append(s.warnings,
				fmt.Sprintf("disk limit %d exceeded; ignoring %d additional disk(s)",
					s.limits.MaxDisks, len(disks)-idx))

			return nil
		}

		s.disks = append(s.disks, diskCtx)

		if err := s.captureDiskTopology(diskCtx.Disk); err != nil {
			return err
		}
	}

	return nil
}

func (s *Snapshot) captureDiskTopology(disk *blockpb.DiskSpec) error {
	sim, err := buildSimulatedDisk(disk, s.wholeDVs[disk.DevPath], s.parts[disk.DevPath])
	if err == nil {
		s.topology[disk.DevPath] = sim

		return nil
	}

	if errors.Is(err, errUnknownGeometry) {
		s.unknownDisk[disk.DevPath] = err.Error()

		return nil
	}

	return err
}

// DevicesReady reports whether device discovery was complete for the snapshot.
func (s *Snapshot) DevicesReady() bool { return s.devicesReady }

// Limits returns the effective planning limits.
func (s *Snapshot) Limits() PlanLimits { return s.limits }

// Truncated reports whether the snapshot input exceeded the planning limits.
func (s *Snapshot) Truncated() bool { return s.truncated }

// Warnings returns snapshot-level warnings.
func (s *Snapshot) Warnings() []string { return slices.Clone(s.warnings) }

// DiskIdentity resolves the non-sensitive identity of a disk in the snapshot.
func (s *Snapshot) DiskIdentity(devPath string) (DiskIdentity, bool) {
	for _, diskCtx := range s.disks {
		if diskCtx.Disk.DevPath == devPath {
			return identifyDisk(diskCtx.Disk), true
		}
	}

	return DiskIdentity{}, false
}

// GeometryOf returns the geometry summary of a disk in the snapshot.
func (s *Snapshot) GeometryOf(devPath string) (GeometrySummary, bool) {
	for _, diskCtx := range s.disks {
		if diskCtx.Disk.DevPath != devPath {
			continue
		}

		summary := GeometrySummary{
			Size:       diskCtx.Disk.Size,
			SectorSize: uint(diskCtx.Disk.SectorSize),
			IOSize:     uint(diskCtx.Disk.IoSize),
			Readonly:   diskCtx.Disk.Readonly,
		}

		if sim, ok := s.topology[devPath]; ok {
			// use the observed signature, never the in-memory state mutated while planning
			summary.TableName = sim.observedName
		} else if dv := s.wholeDVs[devPath]; dv != nil {
			summary.TableName = dv.Name
		}

		for _, part := range s.parts[devPath] {
			summary.Partitions = append(summary.Partitions, PartitionRange{Offset: part.Offset, Size: part.Size})
		}

		slices.SortFunc(summary.Partitions, func(a, b PartitionRange) int { return cmpUint64(a.Offset, b.Offset) })

		return summary, true
	}

	return GeometrySummary{}, false
}

// identifyDisk derives a stable, non-sensitive identity for a disk.
func identifyDisk(disk *blockpb.DiskSpec) DiskIdentity {
	identity := DiskIdentity{DevPath: disk.DevPath, Kind: IdentityKindDevPath}

	switch {
	case disk.Wwid != "":
		identity.Kind = IdentityKindWWID
		identity.Stable = true
		identity.Digest = digestIdentifier(IdentityKindWWID, disk.Wwid)
		identity.Suffix = identifierSuffix(disk.Wwid)
	case disk.Uuid != "":
		identity.Kind = IdentityKindUUID
		identity.Stable = true
		identity.Digest = digestIdentifier(IdentityKindUUID, disk.Uuid)
		identity.Suffix = identifierSuffix(disk.Uuid)
	case disk.Serial != "":
		identity.Kind = IdentityKindSerial
		identity.Stable = true
		identity.Digest = digestIdentifier(IdentityKindSerial, disk.Serial)
		identity.Suffix = identifierSuffix(disk.Serial)
	default:
		identity.Digest = digestIdentifier(IdentityKindDevPath, disk.DevPath)
	}

	return identity
}

func digestIdentifier(kind DiskIdentityKind, raw string) string {
	h := fnv.New64a()
	fmt.Fprintf(h, "%s:%s", kind, raw)

	return fmt.Sprintf("%016x", h.Sum64())
}

// identifierSuffix keeps at most the last four characters of a sensitive identifier.
func identifierSuffix(raw string) string {
	if len(raw) <= 4 {
		return ""
	}

	return raw[len(raw)-4:]
}

// PlanAllocation runs the read-only allocation planning against an immutable snapshot.
//
// It reuses the locator, candidate selection and disk check logic of LocateAndProvision, but
// every device-facing operation is replaced by in-memory simulation: no formatting,
// partitioning, mounting, key fetching or device writes happen. A simulated GPT allocation
// mutates only the in-memory snapshot table, so several volumes sharing a disk are simulated
// in the deterministic provisioning order.
func PlanAllocation(ctx context.Context, logger *zap.Logger, snapshot *Snapshot) (*VolumeAllocationPlan, error) {
	if snapshot == nil {
		return nil, errors.New("allocation planning requires a snapshot")
	}

	// arm the write spies: anything materialized while building the snapshot is irrelevant,
	// the planning phase itself must not perform a single mutating device operation
	for _, sim := range snapshot.topology {
		sim.mem.ResetWriteSpy()
	}

	ordered := slices.Clone(snapshot.volumeConfigs)
	slices.SortStableFunc(ordered, CompareVolumeConfigs)

	plan := &VolumeAllocationPlan{
		DevicesReady: snapshot.devicesReady,
		Truncated:    snapshot.truncated,
		Warnings:     slices.Clone(snapshot.warnings),
		Volumes:      make([]VolumePlan, 0, len(ordered)),
		snapshot:     snapshot,
	}

	// wave gating mirrors VolumeManagerController: later waves wait until earlier ones provision
	fullyProvisionedWave := math.MaxInt

	for _, vc := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		previousWaveProvisioned := vc.TypedSpec().Provisioning.Wave <= fullyProvisionedWave

		volumePlan := planVolume(logger, snapshot, vc, previousWaveProvisioned)
		plan.Volumes = append(plan.Volumes, volumePlan)

		if decisionBlocksWave(volumePlan.Decision) && !value.IsZero(vc.TypedSpec().Provisioning) {
			fullyProvisionedWave = vc.TypedSpec().Provisioning.Wave - 1
		}
	}

	for devPath, sim := range snapshot.topology {
		if calls := sim.mem.MutatingCalls(); calls != 0 {
			return nil, fmt.Errorf("internal error: read-only planning performed %d mutating operation(s) on simulated disk %q", calls, devPath)
		}
	}

	return plan, nil
}

func decisionBlocksWave(decision PlanDecision) bool {
	switch decision {
	case DecisionWaiting, DecisionMissing, DecisionUnknown, DecisionNoCandidate, DecisionError:
		return true
	case DecisionReuseExisting, DecisionAllocateNew, DecisionNotApplicable:
		return false
	default:
		return false
	}
}

// planVolume computes the plan for a single volume, following the LocateAndProvision ordering.
func planVolume(logger *zap.Logger, snapshot *Snapshot, vc *block.VolumeConfig, previousWaveProvisioned bool) VolumePlan {
	spec := vc.TypedSpec()

	result := VolumePlan{
		VolumeID:            vc.Metadata().ID(),
		VolumeConfigVersion: vc.Metadata().Version(),
		Type:                spec.Type,
		EncryptionEnabled:   spec.Encryption.Provider != block.EncryptionProviderNone,
	}

	scratch := block.VolumeStatusSpec{}

	managerCtx := ManagerContext{
		Cfg:                     vc,
		Status:                  &scratch,
		DiscoveredVolumes:       snapshot.discovered,
		Disks:                   snapshot.disks,
		DevicesReady:            snapshot.devicesReady,
		PreviousWaveProvisioned: previousWaveProvisioned,
	}

	// 1. non-provisionable volume types reuse the real classification
	if handleSimpleVolumeTypes(managerCtx) {
		result.Decision = DecisionNotApplicable

		return result
	}

	// 2. locator validation, identical to LocateAndProvision
	if value.IsZero(spec.Locator) {
		result.Decision = DecisionError
		result.ErrorMessage = "volume locator is not set"

		return result
	}

	// 3. existing volume reuse runs through the real locator logic (CEL matching, DiskMatch rules)
	located, err := locateExistingVolume(managerCtx)
	if err != nil {
		result.Decision = DecisionError
		result.ErrorMessage = err.Error()

		return result
	}

	if located {
		result.Decision = DecisionReuseExisting
		result.Reuse = &ReusePlan{
			DevPath:        scratch.Location,
			ParentDevPath:  scratch.ParentLocation,
			PartitionIndex: scratch.PartitionIndex,
			UUID:           scratch.UUID,
			PartitionUUID:  scratch.PartitionUUID,
			Size:           scratch.Size,
		}

		return result
	}

	return snapshot.planProvisioning(logger, vc, managerCtx, previousWaveProvisioned)
}

// planProvisioning covers the states after the locator failed to match: waiting/missing
// gating and the read-only candidate selection and allocation simulation.
func (s *Snapshot) planProvisioning(logger *zap.Logger, vc *block.VolumeConfig, managerCtx ManagerContext, previousWaveProvisioned bool) VolumePlan {
	spec := vc.TypedSpec()

	result := VolumePlan{
		VolumeID:            vc.Metadata().ID(),
		VolumeConfigVersion: vc.Metadata().Version(),
		Type:                spec.Type,
		EncryptionEnabled:   spec.Encryption.Provider != block.EncryptionProviderNone,
	}

	// waiting/missing states follow the same order as LocateAndProvision/provisionNewVolume
	if !s.devicesReady {
		result.Decision = DecisionWaiting
		result.UnknownReasons = append(result.UnknownReasons,
			"device discovery is not complete; disks absent from the snapshot are not considered absent from the machine")

		return result
	}

	if value.IsZero(spec.Provisioning) {
		result.Decision = DecisionMissing

		return result
	}

	if !previousWaveProvisioned {
		result.Decision = DecisionWaiting
		result.UnknownReasons = append(result.UnknownReasons,
			fmt.Sprintf("provisioning wave %d waits for the previous wave", spec.Provisioning.Wave))

		return result
	}

	// candidate discovery reuses the real selector logic (readonly filtering, CEL, single-disk rule)
	matched, err := findCandidateDisks(managerCtx)
	if err != nil {
		result.Candidates = s.evaluateAllCandidates(logger, vc)

		if strings.Contains(err.Error(), "multiple disks matched") {
			result.Decision = DecisionError
			result.ErrorMessage = err.Error()

			return result
		}

		result.Decision = DecisionNoCandidate
		result.Warnings = append(result.Warnings, err.Error())

		return result
	}

	result.Candidates = s.evaluateAllCandidates(logger, vc)

	// the real first-fit selection loop, with the snapshot-backed disk check injected
	pickedDisk, checkResult, err := selectBestDiskWith(logger, matched, vc, s.snapshotChecker())
	if err != nil {
		if s.hasUnknownCandidate(matched) {
			result.Decision = DecisionUnknown
			result.UnknownReasons = append(result.UnknownReasons, s.unknownReasons(matched)...)
		} else {
			result.Decision = DecisionNoCandidate
		}

		result.Warnings = append(result.Warnings, err.Error())

		return result
	}

	allocation, err := s.simulateAllocation(vc, pickedDisk, checkResult)
	if err != nil {
		result.Decision = DecisionError
		result.ErrorMessage = err.Error()

		return result
	}

	result.Decision = DecisionAllocateNew
	result.Allocation = allocation

	return result
}

// snapshotChecker adapts the snapshot to the diskChecker signature used by selectBestDiskWith.
func (s *Snapshot) snapshotChecker() diskChecker {
	return func(_ *zap.Logger, diskPath string, volumeCfg *block.VolumeConfig) CheckDiskResult {
		sim, ok := s.topology[diskPath]
		if !ok {
			// unknown geometry: never claim the disk is provisionable
			return CheckDiskResult{RejectedReason: GeneralError}
		}

		info := diskProbeInfo{
			Name:       sim.tableName,
			Size:       sim.spec.Size,
			SectorSize: uint(sim.spec.SectorSize),
		}

		var largest uint64
		if sim.table != nil {
			largest = sim.table.LargestContiguousAllocatable()
		}

		return checkDiskFromProbe(volumeCfg, info, largest)
	}
}

// evaluateAllCandidates produces a deterministic, per-disk evaluation for the report.
func (s *Snapshot) evaluateAllCandidates(logger *zap.Logger, vc *block.VolumeConfig) []CandidateEvaluation {
	spec := vc.TypedSpec()
	result := make([]CandidateEvaluation, 0, len(s.disks))

	for _, diskCtx := range s.disks {
		evaluation := CandidateEvaluation{
			Disk:         identifyDisk(diskCtx.Disk),
			Readonly:     diskCtx.Disk.Readonly,
			DiskSize:     diskCtx.Disk.Size,
			SectorSize:   uint(diskCtx.Disk.SectorSize),
			RequestedMin: spec.Provisioning.PartitionSpec.MinSize,
			RequestedMax: spec.Provisioning.PartitionSpec.MaxSize,
		}

		if diskCtx.Disk.Readonly {
			evaluation.Rejected = true
			evaluation.RejectedReason = WrongFormat
			evaluation.Exclusion = "disk is read-only"
			result = append(result, evaluation)

			continue
		}

		matches, err := spec.Provisioning.DiskSelector.Match.EvalBool(celenv.DiskLocator(), diskCtx.ToCELContext())
		if err != nil {
			evaluation.Rejected = true
			evaluation.RejectedReason = GeneralError
			evaluation.Exclusion = fmt.Sprintf("error evaluating disk selector: %s", err.Error())
			result = append(result, evaluation)

			continue
		}

		evaluation.MatchedSelector = matches

		if !matches {
			evaluation.Rejected = true
			evaluation.Exclusion = "disk selector does not match"

			result = append(result, evaluation)

			continue
		}

		if reason, unknown := s.unknownDisk[diskCtx.Disk.DevPath]; unknown {
			evaluation.GeometryKnown = false
			evaluation.Rejected = true
			evaluation.RejectedReason = GeneralError
			evaluation.Exclusion = reason
			result = append(result, evaluation)

			continue
		}

		sim, known := s.topology[diskCtx.Disk.DevPath]
		if !known {
			// a disk retained in the snapshot without a usable topology is unknown, never absent
			evaluation.GeometryKnown = false
			evaluation.Rejected = true
			evaluation.RejectedReason = GeneralError
			evaluation.Exclusion = fmt.Sprintf("disk %q has no usable geometry in the snapshot", diskCtx.Disk.DevPath)
			result = append(result, evaluation)

			continue
		}

		evaluation.GeometryKnown = true

		var largest uint64
		if sim.table != nil {
			largest = sim.table.LargestContiguousAllocatable()
			ranges, truncated := sim.freeRanges(s.limits.MaxRanges)
			evaluation.FreeRanges = ranges
			evaluation.RangesTruncated = truncated
		}

		evaluation.AvailableBytes = largest
		evaluation.Alignment = s.predictAlignment(sim, spec)

		check := s.snapshotChecker()(logger, diskCtx.Disk.DevPath, vc)
		evaluation.Rejected = !check.CanProvision
		evaluation.RejectedReason = check.RejectedReason

		if evaluation.Rejected {
			evaluation.Exclusion = rejectionExplanation(check.RejectedReason, spec)
		}

		result = append(result, evaluation)
	}

	return result
}

// predictAlignment reports the alignment the real gpt.AllocatePartition would use.
func (s *Snapshot) predictAlignment(sim *simulatedDisk, spec *block.VolumeConfigSpec) AlignmentInfo {
	if sim == nil || sim.table == nil || spec.Type != block.VolumeTypePartition {
		return AlignmentInfo{SectorSize: sim.sectorSizeValue()}
	}

	alignmentBytes, ok := checkedMul(sim.alignmentLBA, sim.sectorSize)
	if !ok {
		return AlignmentInfo{SectorSize: uint(sim.sectorSize), AlignmentBytes: math.MaxUint64}
	}

	info := AlignmentInfo{
		SectorSize:     sim.sectorSizeValue(),
		AlignmentBytes: alignmentBytes,
	}

	// gpt.AllocatePartition picks the smallest fitting free range; mirror that for the prediction
	for _, freeRange := range s.sortedRanges(sim) {
		if freeRange.Size >= spec.Provisioning.PartitionSpec.MinSize {
			info.PredictedFirstLBA = freeRange.StartLBA
			info.Aligned = freeRange.StartLBA%sim.alignmentLBA == 0

			break
		}
	}

	return info
}

func (s *Snapshot) sortedRanges(sim *simulatedDisk) []FreeRange {
	ranges, _ := sim.freeRanges(s.limits.MaxRanges)

	slices.SortFunc(ranges, func(a, b FreeRange) int {
		if c := cmpUint64(a.Size, b.Size); c != 0 {
			return c
		}

		return cmpUint64(a.StartLBA, b.StartLBA)
	})

	return ranges
}

func (s *simulatedDisk) sectorSizeValue() uint {
	if s == nil {
		return 0
	}

	return uint(s.sectorSize)
}

func rejectionExplanation(reason DiskRejectedReason, spec *block.VolumeConfigSpec) string {
	switch reason {
	case NotEnoughSpace:
		return fmt.Sprintf("not enough contiguous space for the requested minimum size of %d bytes", spec.Provisioning.PartitionSpec.MinSize)
	case WrongFormat:
		return "disk carries an incompatible signature or partition table format"
	case GeneralError:
		return "disk could not be evaluated for provisioning"
	default:
		return "disk could not be evaluated for provisioning"
	}
}

func (s *Snapshot) hasUnknownCandidate(matched []string) bool {
	for _, diskPath := range matched {
		if _, unknown := s.unknownDisk[diskPath]; unknown {
			return true
		}
	}

	return false
}

func (s *Snapshot) unknownReasons(matched []string) []string {
	var reasons []string

	for _, diskPath := range matched {
		if reason, unknown := s.unknownDisk[diskPath]; unknown {
			reasons = append(reasons, reason)
		}
	}

	return reasons
}

// simulateAllocation predicts the provisioning outcome on the in-memory topology, replaying
// the exact size resolution and allocation code used by applyProvisioning/CreatePartition.
//
// For partition volumes the simulated GPT table entry is allocated as well, which commits the
// simulated allocation to the snapshot: subsequent volumes planning against the same disk see
// the reduced free space, exactly as sequential real provisioning would produce.
func (s *Snapshot) simulateAllocation(vc *block.VolumeConfig, diskPath string, checkResult CheckDiskResult) (*AllocationPlan, error) {
	spec := vc.TypedSpec()
	sim := s.topology[diskPath]

	identity := identifyDisk(sim.spec)

	allocation := &AllocationPlan{
		Disk:           identity,
		DiskVersion:    s.diskVersions[diskPath],
		GeometryDigest: s.geometryDigest(diskPath),
		Kind:           spec.Type,
		HasGPT:         checkResult.HasGPT,
		SectorSize:     uint(sim.spec.SectorSize),
	}

	switch spec.Type { //nolint:exhaustive
	case block.VolumeTypeDisk:
		allocation.Size = sim.spec.Size

		return allocation, nil

	case block.VolumeTypePartition:
		partitionIdx, partitionEntry, size, available, err := sim.simulatePartitionAllocation(spec.Provisioning.PartitionSpec)
		if err != nil {
			return nil, err
		}

		allocation.AvailableBytes = available
		allocation.PartitionIndex = partitionIdx
		allocation.FirstLBA = partitionEntry.FirstLBA
		allocation.LastLBA = partitionEntry.LastLBA
		allocation.Size = size

		alignmentBytes, ok := checkedMul(sim.alignmentLBA, sim.sectorSize)
		if !ok {
			return nil, fmt.Errorf("%w: alignment size overflow for disk %q", errUnknownGeometry, diskPath)
		}

		allocation.AlignmentBytes = alignmentBytes

		return allocation, nil

	default:
		return nil, fmt.Errorf("unexpected volume type for allocation: %s", spec.Type)
	}
}

// simulatePartitionAllocation replays CreatePartition's size resolution and the real
// gpt.AllocatePartition against the in-memory table, returning the predicted entry.
//
// Allocating the first partition flips the simulated disk to the GPT state, mirroring the
// partition table that CreatePartition would write on an empty disk.
func (s *simulatedDisk) simulatePartitionAllocation(partSpec block.PartitionSpec) (int, gpt.Partition, uint64, uint64, error) {
	// expected to exist: the snapshot checker only returns CanProvision with a usable table
	if s.table == nil {
		return 0, gpt.Partition{}, 0, 0, errors.New("no simulated GPT table")
	}

	available := s.table.LargestContiguousAllocatable()

	// replay CreatePartition's size resolution verbatim
	size := partSpec.MinSize

	maxSize, err := partSpec.ResolveMaxSize(available)
	if err != nil {
		return 0, gpt.Partition{}, 0, available, fmt.Errorf("error resolving max size: %w", err)
	}

	if available < size {
		// mirrors the defensive check in CreatePartition
		return 0, gpt.Partition{}, 0, available, errors.New("not enough space on disk")
	}

	if maxSize == 0 || maxSize >= available {
		size = available
	} else {
		size = maxSize
	}

	typeUUID, err := uuid.Parse(partSpec.TypeUUID)
	if err != nil {
		return 0, gpt.Partition{}, 0, available, fmt.Errorf("error parsing type UUID: %w", err)
	}

	partitionIdx, partitionEntry, err := s.table.AllocatePartition(size, partSpec.Label, typeUUID)
	if err != nil {
		return 0, gpt.Partition{}, 0, available, fmt.Errorf("error allocating partition: %w", err)
	}

	// writing the first partition on an empty disk creates its GPT: subsequent volume
	// checks must take the GPT/free-space branch, exactly as on a real device
	if !s.hasGPT {
		s.hasGPT = true
		s.tableName = gptSignatureName
	}

	return partitionIdx, partitionEntry, size, available, nil
}

func (s *Snapshot) geometryDigest(devPath string) string {
	summary, ok := s.GeometryOf(devPath)
	if !ok {
		return ""
	}

	return summary.Digest()
}

// PlanStaleError aggregates the reasons why a plan can no longer be applied to the current
// topology.
type PlanStaleError struct {
	Reasons []string
}

// Error implements error.
func (e *PlanStaleError) Error() string {
	return fmt.Sprintf("allocation plan is stale: %s", strings.Join(e.Reasons, "; "))
}

// Validate re-checks the plan against a fresh snapshot before Apply.
//
// It compares bound resource versions, stable disk identities and geometry digests, and it
// verifies that reuse targets still exist. The check is read-only and purely comparative:
// even a successful validation cannot rule out concurrent external changes after the fresh
// snapshot was taken.
func (p *VolumeAllocationPlan) Validate(fresh *Snapshot) error {
	if fresh == nil {
		return errors.New("validation requires a fresh snapshot")
	}

	var stale PlanStaleError

	for _, volumePlan := range p.Volumes {
		freshCfg := fresh.volumeConfig(volumePlan.VolumeID)
		if freshCfg == nil {
			stale.Reasons = append(stale.Reasons, fmt.Sprintf("volume %q is no longer present", volumePlan.VolumeID))

			continue
		}

		if !freshCfg.Metadata().Version().Equal(volumePlan.VolumeConfigVersion) {
			stale.Reasons = append(stale.Reasons,
				fmt.Sprintf("volume %q config changed (version %s -> %s)",
					volumePlan.VolumeID, volumePlan.VolumeConfigVersion, freshCfg.Metadata().Version()))
		}

		switch {
		case volumePlan.Reuse != nil:
			if !fresh.discoveredVolumeStillPresent(volumePlan.Reuse) {
				stale.Reasons = append(stale.Reasons,
					fmt.Sprintf("volume %q reuse target %q is no longer discovered", volumePlan.VolumeID, volumePlan.Reuse.DevPath))
			}
		case volumePlan.Allocation != nil:
			reason := fresh.validateAllocation(volumePlan.Allocation)
			if reason != "" {
				stale.Reasons = append(stale.Reasons, fmt.Sprintf("volume %q: %s", volumePlan.VolumeID, reason))
			}
		}
	}

	if len(stale.Reasons) > 0 {
		return &stale
	}

	return nil
}

func (s *Snapshot) volumeConfig(id string) *block.VolumeConfig {
	for _, vc := range s.volumeConfigs {
		if vc.Metadata().ID() == id {
			return vc
		}
	}

	return nil
}

func (s *Snapshot) discoveredVolumeStillPresent(reuse *ReusePlan) bool {
	for _, dv := range s.discovered {
		if dv.DevPath != reuse.DevPath {
			continue
		}

		if reuse.PartitionUUID != "" && dv.PartitionUuid != reuse.PartitionUUID {
			continue
		}

		return true
	}

	return false
}

func (s *Snapshot) validateAllocation(allocation *AllocationPlan) string {
	sim, ok := s.topology[allocation.Disk.DevPath]
	if !ok {
		return fmt.Sprintf("disk %q is no longer available or its geometry is unknown", allocation.Disk.DevPath)
	}

	identity := identifyDisk(sim.spec)
	if identity.Digest != allocation.Disk.Digest || identity.Kind != allocation.Disk.Kind {
		return fmt.Sprintf("stable identity of disk %q changed", allocation.Disk.DevPath)
	}

	if boundVersion, known := s.diskVersions[allocation.Disk.DevPath]; known && !boundVersion.Equal(allocation.DiskVersion) {
		return fmt.Sprintf("disk %q resource version changed (%s -> %s)",
			allocation.Disk.DevPath, allocation.DiskVersion, boundVersion)
	}

	if digest := s.geometryDigest(allocation.Disk.DevPath); digest != allocation.GeometryDigest {
		return fmt.Sprintf("geometry of disk %q changed (digest %s -> %s)",
			allocation.Disk.DevPath, allocation.GeometryDigest, digest)
	}

	return ""
}
