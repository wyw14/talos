// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package volumes_test

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/siderolabs/talos/internal/app/machined/pkg/controllers/block/internal/volumes"
	blockpb "github.com/siderolabs/talos/pkg/machinery/api/resource/definitions/block"
	taloscel "github.com/siderolabs/talos/pkg/machinery/cel"
	"github.com/siderolabs/talos/pkg/machinery/cel/celenv"
	"github.com/siderolabs/talos/pkg/machinery/resources/block"
)

// Test planning constants.
const (
	pTenGiB = 10 * 1024 * 1024 * 1024
	pOneGiB = 1024 * 1024 * 1024
	pOneMiB = 1024 * 1024
)

func pDiskCEL(expr string) taloscel.Expression {
	return taloscel.MustExpression(taloscel.ParseBooleanExpression(expr, celenv.DiskLocator()))
}

func pVolumeCEL(expr string) taloscel.Expression {
	return taloscel.MustExpression(taloscel.ParseBooleanExpression(expr, celenv.VolumeLocator()))
}

func pDisk(dev string, size, sectorSize uint64, opts ...func(*blockpb.DiskSpec)) volumes.DiskContext {
	d := &blockpb.DiskSpec{DevPath: dev, Size: size, SectorSize: sectorSize, IoSize: sectorSize}
	for _, opt := range opts {
		opt(d)
	}

	return volumes.DiskContext{Disk: d}
}

func pDV(dev, parent string, size uint64, opts ...func(*blockpb.DiscoveredVolumeSpec)) *blockpb.DiscoveredVolumeSpec {
	v := &blockpb.DiscoveredVolumeSpec{DevPath: dev, ParentDevPath: parent, Size: size, SectorSize: 512}
	for _, opt := range opts {
		opt(v)
	}

	return v
}

func pWholeDisk(dev string, size uint64, name string) *blockpb.DiscoveredVolumeSpec {
	return pDV(dev, "", size, func(v *blockpb.DiscoveredVolumeSpec) { v.Name = name })
}

func pPartitionVC(id string, minSize, maxSize uint64, selector string) *block.VolumeConfig {
	vc := block.NewVolumeConfig(block.NamespaceName, id)
	vc.TypedSpec().Type = block.VolumeTypePartition
	vc.TypedSpec().Locator.Match = pVolumeCEL("false")
	vc.TypedSpec().Provisioning.DiskSelector.Match = pDiskCEL(selector)
	vc.TypedSpec().Provisioning.PartitionSpec.MinSize = minSize
	vc.TypedSpec().Provisioning.PartitionSpec.MaxSize = maxSize
	vc.TypedSpec().Provisioning.PartitionSpec.Label = id
	vc.TypedSpec().Provisioning.PartitionSpec.TypeUUID = "c12a7328-f81f-11d2-ba4b-00a0c93ec93b"

	return vc
}

func pDiskVC(id, selector string) *block.VolumeConfig {
	vc := block.NewVolumeConfig(block.NamespaceName, id)
	vc.TypedSpec().Type = block.VolumeTypeDisk
	vc.TypedSpec().Locator.Match = pVolumeCEL("false")
	vc.TypedSpec().Provisioning.DiskSelector.Match = pDiskCEL(selector)

	return vc
}

func pSnapshot(t *testing.T, input volumes.PlanInput) *volumes.Snapshot {
	t.Helper()

	snapshot, err := volumes.NewSnapshot(input)
	require.NoError(t, err)

	return snapshot
}

func pPlan(t *testing.T, snapshot *volumes.Snapshot) *volumes.VolumeAllocationPlan {
	t.Helper()

	plan, err := volumes.PlanAllocation(context.Background(), zaptest.NewLogger(t), snapshot)
	require.NoError(t, err)

	return plan
}

func pFind(plan *volumes.VolumeAllocationPlan, id string) *volumes.VolumePlan {
	for i := range plan.Volumes {
		if plan.Volumes[i].VolumeID == id {
			return &plan.Volumes[i]
		}
	}

	return nil
}

func pOffset(off uint64) func(*blockpb.DiscoveredVolumeSpec) {
	return func(v *blockpb.DiscoveredVolumeSpec) { v.Offset = off }
}

func pIndex(idx uint64) func(*blockpb.DiscoveredVolumeSpec) {
	return func(v *blockpb.DiscoveredVolumeSpec) { v.PartitionIndex = idx }
}

func pPartLabel(label string) func(*blockpb.DiscoveredVolumeSpec) {
	return func(v *blockpb.DiscoveredVolumeSpec) { v.PartitionLabel = label }
}

func pPartUUID(uuid string) func(*blockpb.DiscoveredVolumeSpec) {
	return func(v *blockpb.DiscoveredVolumeSpec) { v.PartitionUuid = uuid }
}

func pPartType() func(*blockpb.DiscoveredVolumeSpec) {
	return func(v *blockpb.DiscoveredVolumeSpec) { v.PartitionType = testTypeUUID }
}

func pSerial(serial string) func(*blockpb.DiskSpec) {
	return func(d *blockpb.DiskSpec) { d.Serial = serial }
}

func pWWID(wwid string) func(*blockpb.DiskSpec) {
	return func(d *blockpb.DiskSpec) { d.Wwid = wwid }
}

func pReadonly(d *blockpb.DiskSpec) { d.Readonly = true }

func TestPlanPartitionAlignment512e(t *testing.T) {
	t.Parallel()

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs:     []*block.VolumeConfig{pPartitionVC("EPHEMERAL", pOneMiB, pOneMiB, "!disk.readonly")},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, "")},
		Disks:             []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512)},
		DevicesReady:      true,
	})

	plan := pPlan(t, snapshot)
	volume := pFind(plan, "EPHEMERAL")
	require.NotNil(t, volume)

	require.Equal(t, volumes.DecisionAllocateNew, volume.Decision)
	require.NotNil(t, volume.Allocation)

	assert.False(t, volume.Allocation.HasGPT)
	assert.Equal(t, 1, volume.Allocation.PartitionIndex)
	assert.Equal(t, uint64(2048), volume.Allocation.FirstLBA)
	assert.Equal(t, uint64(4095), volume.Allocation.LastLBA)
	assert.Equal(t, uint64(pOneMiB), volume.Allocation.Size)
	assert.Equal(t, uint64(pOneMiB), volume.Allocation.AlignmentBytes)
	assert.Equal(t, uint(512), volume.Allocation.SectorSize)

	require.Len(t, volume.Candidates, 1)
	assert.True(t, volume.Candidates[0].Alignment.Aligned)
	assert.Equal(t, uint64(2048), volume.Candidates[0].Alignment.PredictedFirstLBA)
}

func TestPlanPartitionAlignment4Kn(t *testing.T) {
	t.Parallel()

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs:     []*block.VolumeConfig{pPartitionVC("EPHEMERAL", pOneMiB, pOneMiB, "!disk.readonly")},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, "")},
		Disks:             []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 4096)},
		DevicesReady:      true,
	})

	plan := pPlan(t, snapshot)
	volume := pFind(plan, "EPHEMERAL")
	require.NotNil(t, volume)
	require.NotNil(t, volume.Allocation)

	// 1MiB alignment on a 4KiB sector disk is LBA 256; a 1MiB partition spans 256 sectors
	assert.Equal(t, uint64(256), volume.Allocation.FirstLBA)
	assert.Equal(t, uint64(511), volume.Allocation.LastLBA)
	assert.Equal(t, uint(4096), volume.Allocation.SectorSize)
	assert.Equal(t, uint64(pOneMiB), volume.Allocation.AlignmentBytes)
}

func TestPlanGrowToAvailableSpace(t *testing.T) {
	t.Parallel()

	vc := pPartitionVC("BIG", pOneMiB, 0, "!disk.readonly") // no max size: grows to fill

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs:     []*block.VolumeConfig{vc},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, "")},
		Disks:             []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512)},
		DevicesReady:      true,
	})

	plan := pPlan(t, snapshot)
	volume := pFind(plan, "BIG")
	require.NotNil(t, volume)
	require.NotNil(t, volume.Allocation)

	assert.Equal(t, volume.Candidates[0].AvailableBytes, volume.Allocation.Size)
	assert.Greater(t, volume.Allocation.Size, uint64(pOneMiB))
}

func TestPlanFragmentationPicksSmallestFittingRange(t *testing.T) {
	t.Parallel()

	parts := []*blockpb.DiscoveredVolumeSpec{
		// partition 1: LBA 2048, 1GiB; partition 2 starts after a 1GiB gap
		pDV("/dev/sda1", "/dev/sda", pOneGiB, pOffset(2048*512), pIndex(1), pPartLabel("P1"), pPartType()),
		pDV("/dev/sda2", "/dev/sda", pOneGiB, pOffset(4196352*512), pIndex(2), pPartLabel("P2"), pPartType()),
	}

	build := func(t *testing.T, minSize, maxSize uint64) *volumes.VolumePlan {
		snapshot := pSnapshot(t, volumes.PlanInput{
			VolumeConfigs:     []*block.VolumeConfig{pPartitionVC("HOLE", minSize, maxSize, "!disk.readonly")},
			DiscoveredVolumes: append([]*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, "gpt")}, parts...),
			Disks:             []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512)},
			DevicesReady:      true,
		})

		return pFind(pPlan(t, snapshot), "HOLE")
	}

	t.Run("small request lands in the 1GiB gap", func(t *testing.T) {
		t.Parallel()

		volume := build(t, pOneMiB/2, pOneMiB/2)
		require.Equal(t, volumes.DecisionAllocateNew, volume.Decision)
		assert.Equal(t, uint64(2099200), volume.Allocation.FirstLBA)
		assert.True(t, volume.Allocation.HasGPT)
		assert.Equal(t, 3, volume.Allocation.PartitionIndex)

		// two free ranges are reported: the gap and the tail
		assert.Len(t, volume.Candidates[0].FreeRanges, 2)
	})

	t.Run("request larger than the gap lands in the tail", func(t *testing.T) {
		t.Parallel()

		volume := build(t, 2*pOneGiB, 2*pOneGiB)
		require.Equal(t, volumes.DecisionAllocateNew, volume.Decision)
		assert.Equal(t, uint64(6293504), volume.Allocation.FirstLBA)
	})
}

func TestPlanIdenticalCandidatesKeepDeterministicOrder(t *testing.T) {
	t.Parallel()

	disks := []volumes.DiskContext{
		pDisk("/dev/sda", pTenGiB, 512),
		pDisk("/dev/sdb", pTenGiB, 512),
	}

	for range 3 {
		snapshot := pSnapshot(t, volumes.PlanInput{
			VolumeConfigs:     []*block.VolumeConfig{pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")},
			DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, ""), pWholeDisk("/dev/sdb", pTenGiB, "")},
			Disks:             disks,
			DevicesReady:      true,
		})

		plan := pPlan(t, snapshot)
		volume := pFind(plan, "V")
		require.NotNil(t, volume)
		require.NotNil(t, volume.Allocation)
		assert.Equal(t, "/dev/sda", volume.Allocation.Disk.DevPath)

		require.Len(t, volume.Candidates, 2)
		assert.False(t, volume.Candidates[0].Rejected)
		assert.False(t, volume.Candidates[1].Rejected)
	}
}

func TestPlanReusesExistingVolume(t *testing.T) {
	t.Parallel()

	vc := block.NewVolumeConfig(block.NamespaceName, "STATE")
	vc.TypedSpec().Type = block.VolumeTypePartition
	vc.TypedSpec().Locator.Match = pVolumeCEL(`volume.partition_label == "STATE"`)

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs: []*block.VolumeConfig{vc},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{
			pWholeDisk("/dev/sda", pTenGiB, "gpt"),
			pDV("/dev/sda1", "/dev/sda", pOneGiB, pOffset(2048*512), pIndex(1), pPartLabel("STATE"), pPartUUID("part-uuid-1"), pPartType()),
		},
		Disks:        []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512)},
		DevicesReady: true,
	})

	plan := pPlan(t, snapshot)
	volume := pFind(plan, "STATE")
	require.NotNil(t, volume)

	assert.Equal(t, volumes.DecisionReuseExisting, volume.Decision)
	require.NotNil(t, volume.Reuse)
	assert.Nil(t, volume.Allocation)
	assert.Equal(t, "/dev/sda1", volume.Reuse.DevPath)
	assert.Equal(t, "/dev/sda", volume.Reuse.ParentDevPath)
	assert.Equal(t, 1, volume.Reuse.PartitionIndex)
	assert.Equal(t, "part-uuid-1", volume.Reuse.PartitionUUID)
	assert.Empty(t, volume.Candidates)
}

func TestPlanDiskMatchMultipleDisksIsError(t *testing.T) {
	t.Parallel()

	vc := block.NewVolumeConfig(block.NamespaceName, "D")
	vc.TypedSpec().Type = block.VolumeTypeDisk
	vc.TypedSpec().Locator.DiskMatch = pDiskCEL("!disk.readonly")

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs: []*block.VolumeConfig{vc},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{
			pWholeDisk("/dev/sda", pTenGiB, "gpt"),
			pWholeDisk("/dev/sdb", pTenGiB, "gpt"),
		},
		Disks: []volumes.DiskContext{
			pDisk("/dev/sda", pTenGiB, 512),
			pDisk("/dev/sdb", pTenGiB, 512),
		},
		DevicesReady: true,
	})

	volume := pFind(pPlan(t, snapshot), "D")
	require.NotNil(t, volume)
	assert.Equal(t, volumes.DecisionError, volume.Decision)
	assert.Contains(t, volume.ErrorMessage, "multiple disks matched")
}

func TestPlanIncompleteDiscoveryWaitsAndDoesNotImplyAbsence(t *testing.T) {
	t.Parallel()

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs:     []*block.VolumeConfig{pPartitionVC("V", pOneMiB, pOneMiB, `disk.serial == "FUTURE-DISK"`)},
		DiscoveredVolumes: nil,
		Disks:             []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512, pSerial("PRESENT"))},
		DevicesReady:      false,
	})

	volume := pFind(pPlan(t, snapshot), "V")
	require.NotNil(t, volume)
	assert.Equal(t, volumes.DecisionWaiting, volume.Decision)
	require.NotEmpty(t, volume.UnknownReasons)
	assert.Contains(t, volume.UnknownReasons[0], "not considered absent")
}

func TestPlanUnknownGeometryMarkedUnknown(t *testing.T) {
	t.Parallel()

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs:     []*block.VolumeConfig{pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, "gpt")},
		Disks:             []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 0)},
		DevicesReady:      true,
	})

	volume := pFind(pPlan(t, snapshot), "V")
	require.NotNil(t, volume)
	assert.Equal(t, volumes.DecisionUnknown, volume.Decision)
	require.Len(t, volume.Candidates, 1)
	assert.False(t, volume.Candidates[0].GeometryKnown)
	assert.NotEmpty(t, volume.Candidates[0].Exclusion)
}

func TestPlanOverflowingPartitionIsUnknown(t *testing.T) {
	t.Parallel()

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs: []*block.VolumeConfig{pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{
			pWholeDisk("/dev/sda", pTenGiB, "gpt"),
			pDV("/dev/sda1", "/dev/sda", math.MaxUint64-511, pOffset(2048*512), pIndex(1)),
		},
		Disks:        []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512)},
		DevicesReady: true,
	})

	volume := pFind(pPlan(t, snapshot), "V")
	require.NotNil(t, volume)
	assert.Equal(t, volumes.DecisionUnknown, volume.Decision)
}

func TestPlanNoCandidateNotEnoughSpace(t *testing.T) {
	t.Parallel()

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs:     []*block.VolumeConfig{pPartitionVC("V", pOneGiB, pOneGiB, "!disk.readonly")},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", 100*pOneMiB, "")},
		Disks:             []volumes.DiskContext{pDisk("/dev/sda", 100*pOneMiB, 512)},
		DevicesReady:      true,
	})

	volume := pFind(pPlan(t, snapshot), "V")
	require.NotNil(t, volume)
	assert.Equal(t, volumes.DecisionNoCandidate, volume.Decision)
	require.Len(t, volume.Candidates, 1)
	assert.True(t, volume.Candidates[0].Rejected)
	assert.Equal(t, volumes.NotEnoughSpace, volume.Candidates[0].RejectedReason)
	assert.Contains(t, volume.Candidates[0].Exclusion, "minimum size")
}

func TestPlanSharedSpaceSimulatedInDeterministicOrder(t *testing.T) {
	t.Parallel()

	// feed the configs in reverse ID order; the planner must sort them itself
	configs := []*block.VolumeConfig{
		pPartitionVC("ZVOL", 2*pOneGiB, 2*pOneGiB, "!disk.readonly"),
		pPartitionVC("AVOL", 2*pOneGiB, 2*pOneGiB, "!disk.readonly"),
		pPartitionVC("TOOLARGE", 9*pOneGiB, 9*pOneGiB, "!disk.readonly"),
	}

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs:     configs,
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, "")},
		Disks:             []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512)},
		DevicesReady:      true,
	})

	plan := pPlan(t, snapshot)

	avol := pFind(plan, "AVOL")
	require.NotNil(t, avol)
	require.NotNil(t, avol.Allocation)
	assert.Equal(t, 1, avol.Allocation.PartitionIndex)
	assert.Equal(t, uint64(2048), avol.Allocation.FirstLBA)

	zvol := pFind(plan, "ZVOL")
	require.NotNil(t, zvol)
	require.NotNil(t, zvol.Allocation)
	assert.Equal(t, 2, zvol.Allocation.PartitionIndex)
	assert.Equal(t, uint64(4196352), zvol.Allocation.FirstLBA)
	assert.Less(t, zvol.Candidates[0].AvailableBytes, avol.Candidates[0].AvailableBytes)

	tooLarge := pFind(plan, "TOOLARGE")
	require.NotNil(t, tooLarge)
	assert.Equal(t, volumes.DecisionNoCandidate, tooLarge.Decision)
}

func TestPlanWaveGating(t *testing.T) {
	t.Parallel()

	blocked := pPartitionVC("BLOCKED", pOneMiB, pOneMiB, `disk.serial == "MISSING"`)
	blocked.TypedSpec().Provisioning.Wave = 0

	next := pPartitionVC("NEXT", pOneMiB, pOneMiB, "!disk.readonly")
	next.TypedSpec().Provisioning.Wave = 1

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs:     []*block.VolumeConfig{blocked, next},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, "")},
		Disks:             []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512)},
		DevicesReady:      true,
	})

	plan := pPlan(t, snapshot)
	assert.Equal(t, volumes.DecisionNoCandidate, pFind(plan, "BLOCKED").Decision)

	nextPlan := pFind(plan, "NEXT")
	require.NotNil(t, nextPlan)
	assert.Equal(t, volumes.DecisionWaiting, nextPlan.Decision)
	require.NotEmpty(t, nextPlan.UnknownReasons)
	assert.Contains(t, nextPlan.UnknownReasons[0], "wave 1")
}

func TestPlanLimitsTruncate(t *testing.T) {
	t.Parallel()

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs: []*block.VolumeConfig{pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{
			pWholeDisk("/dev/sda", pTenGiB, ""),
			pWholeDisk("/dev/sdb", pTenGiB, ""),
		},
		Disks: []volumes.DiskContext{
			pDisk("/dev/sda", pTenGiB, 512),
			pDisk("/dev/sdb", pTenGiB, 512),
		},
		DevicesReady: true,
		Limits:       volumes.PlanLimits{MaxDisks: 1, MaxVolumes: 8, MaxRanges: 4},
	})

	assert.True(t, snapshot.Truncated())
	require.NotEmpty(t, snapshot.Warnings())

	plan := pPlan(t, snapshot)
	assert.True(t, plan.Truncated)
	require.Len(t, pFind(plan, "V").Candidates, 1)
}

func TestPlanSerialSummarizedAndSecretsNeverExposed(t *testing.T) {
	t.Parallel()

	vc := pPartitionVC("SECRET", pOneMiB, pOneMiB, "!disk.readonly")
	vc.TypedSpec().Encryption.Provider = block.EncryptionProviderLUKS2
	vc.TypedSpec().Encryption.Keys = []block.EncryptionKey{
		{
			Type:             block.EncryptionKeyStatic,
			StaticPassphrase: []byte("topsecret-passphrase"),
		},
		{
			Type:        block.EncryptionKeyKMS,
			KMSEndpoint: "https://kms.example.invalid/root-key",
		},
	}

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs:     []*block.VolumeConfig{vc},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, "")},
		Disks: []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512,
			pSerial("SECRETSERIAL9999"), pWWID("very-sensitive-wwid-value"))},
		DevicesReady: true,
	})

	plan := pPlan(t, snapshot)
	volume := pFind(plan, "SECRET")
	require.NotNil(t, volume)
	assert.True(t, volume.EncryptionEnabled)

	identity, ok := snapshot.DiskIdentity("/dev/sda")
	require.True(t, ok)
	assert.True(t, identity.Stable)
	assert.Equal(t, volumes.IdentityKindWWID, identity.Kind)
	assert.Equal(t, "alue", identity.Suffix)
	assert.Len(t, identity.Digest, 16)

	raw, err := json.Marshal(plan)
	require.NoError(t, err)

	rendered := string(raw)
	assert.NotContains(t, rendered, "topsecret-passphrase")
	assert.NotContains(t, rendered, "kms.example.invalid")
	assert.NotContains(t, rendered, "SECRETSERIAL9999")
	assert.NotContains(t, rendered, "very-sensitive-wwid-value")
	assert.Contains(t, rendered, identity.Digest)
}

func TestPlanValidation(t *testing.T) {
	t.Parallel()

	buildInput := func(size uint64, includePartition bool, vc *block.VolumeConfig) volumes.PlanInput {
		dvs := []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", size, "gpt")}
		if includePartition {
			dvs = append(dvs, pDV("/dev/sda1", "/dev/sda", pOneGiB, pOffset(2048*512), pIndex(1), pPartLabel("STATE"), pPartUUID("part-uuid-1"), pPartType()))
		}

		return volumes.PlanInput{
			VolumeConfigs:     []*block.VolumeConfig{vc},
			DiscoveredVolumes: dvs,
			Disks:             []volumes.DiskContext{pDisk("/dev/sda", size, 512, pWWID("stable-wwid"))},
			DevicesReady:      true,
			DiskVersions:      map[string]resource.Version{"/dev/sda": resource.VersionUndefined.Next()},
		}
	}

	t.Run("unchanged snapshot validates", func(t *testing.T) {
		t.Parallel()

		vc := pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")
		plan := pPlan(t, pSnapshot(t, buildInput(pTenGiB, true, vc)))

		assert.NoError(t, plan.Validate(pSnapshot(t, buildInput(pTenGiB, true, pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")))))
	})

	t.Run("geometry change invalidates", func(t *testing.T) {
		t.Parallel()

		vc := pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")
		plan := pPlan(t, pSnapshot(t, buildInput(pTenGiB, true, vc)))

		err := plan.Validate(pSnapshot(t, buildInput(pTenGiB-pOneGiB, true, pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly"))))
		require.Error(t, err)

		var stale *volumes.PlanStaleError
		require.ErrorAs(t, err, &stale)
		assert.Contains(t, err.Error(), "geometry")
	})

	t.Run("disk removal invalidates", func(t *testing.T) {
		t.Parallel()

		vc := pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")
		plan := pPlan(t, pSnapshot(t, buildInput(pTenGiB, true, vc)))

		fresh := pSnapshot(t, volumes.PlanInput{
			VolumeConfigs: []*block.VolumeConfig{pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")},
			Disks:         nil,
			DevicesReady:  true,
		})

		err := plan.Validate(fresh)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no longer available")
	})

	t.Run("volume config version change invalidates", func(t *testing.T) {
		t.Parallel()

		vc := pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")
		plan := pPlan(t, pSnapshot(t, buildInput(pTenGiB, true, vc)))

		freshVC := pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")
		freshVC.Metadata().SetVersion(freshVC.Metadata().Version().Next())

		err := plan.Validate(pSnapshot(t, buildInput(pTenGiB, true, freshVC)))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "config changed")
	})

	t.Run("reuse target removal invalidates", func(t *testing.T) {
		t.Parallel()

		reuseVC := block.NewVolumeConfig(block.NamespaceName, "STATE")
		reuseVC.TypedSpec().Type = block.VolumeTypePartition
		reuseVC.TypedSpec().Locator.Match = pVolumeCEL(`volume.partition_label == "STATE"`)

		input := buildInput(pTenGiB, true, reuseVC)
		plan := pPlan(t, pSnapshot(t, input))

		freshInput := buildInput(pTenGiB, false, reuseVC)
		err := plan.Validate(pSnapshot(t, freshInput))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reuse target")
	})
}

func TestPlanSimpleTypeNotApplicable(t *testing.T) {
	t.Parallel()

	vc := block.NewVolumeConfig(block.NamespaceName, "TMP")
	vc.TypedSpec().Type = block.VolumeTypeTmpfs

	plan := pPlan(t, pSnapshot(t, volumes.PlanInput{
		VolumeConfigs: []*block.VolumeConfig{vc},
		DevicesReady:  true,
	}))

	assert.Equal(t, volumes.DecisionNotApplicable, pFind(plan, "TMP").Decision)
}

func TestPlanDiskTypeAllocation(t *testing.T) {
	t.Parallel()

	t.Run("whole empty disk", func(t *testing.T) {
		t.Parallel()

		snapshot := pSnapshot(t, volumes.PlanInput{
			VolumeConfigs:     []*block.VolumeConfig{pDiskVC("D", "!disk.readonly")},
			DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, "")},
			Disks:             []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512)},
			DevicesReady:      true,
		})

		volume := pFind(pPlan(t, snapshot), "D")
		require.NotNil(t, volume)
		require.NotNil(t, volume.Allocation)
		assert.Equal(t, block.VolumeTypeDisk, volume.Allocation.Kind)
		assert.Equal(t, uint64(pTenGiB), volume.Allocation.Size)
		assert.Equal(t, 0, volume.Allocation.PartitionIndex)
	})

	t.Run("already partitioned disk rejected", func(t *testing.T) {
		t.Parallel()

		snapshot := pSnapshot(t, volumes.PlanInput{
			VolumeConfigs:     []*block.VolumeConfig{pDiskVC("D", "!disk.readonly")},
			DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, "gpt")},
			Disks:             []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512)},
			DevicesReady:      true,
		})

		volume := pFind(pPlan(t, snapshot), "D")
		require.NotNil(t, volume)
		assert.Equal(t, volumes.DecisionNoCandidate, volume.Decision)
		assert.Equal(t, volumes.WrongFormat, volume.Candidates[0].RejectedReason)
	})
}

func TestPlanReadonlyDiskExcluded(t *testing.T) {
	t.Parallel()

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs:     []*block.VolumeConfig{pPartitionVC("V", pOneMiB, pOneMiB, "!disk.readonly")},
		DiscoveredVolumes: []*blockpb.DiscoveredVolumeSpec{pWholeDisk("/dev/sda", pTenGiB, "")},
		Disks:             []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512, pReadonly)},
		DevicesReady:      true,
	})

	volume := pFind(pPlan(t, snapshot), "V")
	require.NotNil(t, volume)
	assert.Equal(t, volumes.DecisionNoCandidate, volume.Decision)
	require.Len(t, volume.Candidates, 1)
	assert.True(t, volume.Candidates[0].Readonly)
	assert.Contains(t, volume.Candidates[0].Exclusion, "read-only")
}

func TestPlanMissingWithoutProvisioningSpec(t *testing.T) {
	t.Parallel()

	vc := block.NewVolumeConfig(block.NamespaceName, "M")
	vc.TypedSpec().Type = block.VolumeTypePartition
	vc.TypedSpec().Locator.Match = pVolumeCEL("false")

	snapshot := pSnapshot(t, volumes.PlanInput{
		VolumeConfigs: []*block.VolumeConfig{vc},
		Disks:         []volumes.DiskContext{pDisk("/dev/sda", pTenGiB, 512)},
		DevicesReady:  true,
	})

	assert.Equal(t, volumes.DecisionMissing, pFind(pPlan(t, snapshot), "M").Decision)
}
