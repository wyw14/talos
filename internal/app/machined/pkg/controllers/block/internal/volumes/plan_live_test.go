// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package volumes_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/siderolabs/gen/xerrors"
	"github.com/siderolabs/go-blockdevice/v2/blkid"
	blockdev "github.com/siderolabs/go-blockdevice/v2/block"
	"github.com/siderolabs/go-blockdevice/v2/partitioning/gpt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/siderolabs/talos/internal/app/machined/pkg/controllers/block/internal/volumes"
	blockpb "github.com/siderolabs/talos/pkg/machinery/api/resource/definitions/block"
	"github.com/siderolabs/talos/pkg/machinery/resources/block"
)

const testTypeUUID = "c12a7328-f81f-11d2-ba4b-00a0c93ec93b"

// snapshotFromRealDevice builds a planning snapshot from a read-only blkid probe of a real
// (loop) block device, mirroring the data the discovery controller would publish.
func snapshotFromRealDevice(t *testing.T, devPath string, configs ...*block.VolumeConfig) *volumes.Snapshot {
	t.Helper()

	info, err := blkid.ProbePath(devPath)
	require.NoError(t, err)

	disk := volumes.DiskContext{
		Disk: &blockpb.DiskSpec{
			DevPath:    devPath,
			Size:       info.Size,
			SectorSize: uint64(info.SectorSize),
			IoSize:     uint64(info.IOSize),
		},
	}

	discovered := make([]*blockpb.DiscoveredVolumeSpec, 0, 1+len(info.Parts))
	discovered = append(discovered, &blockpb.DiscoveredVolumeSpec{
		DevPath:    devPath,
		Name:       info.Name,
		Size:       info.Size,
		SectorSize: uint64(info.SectorSize),
		IoSize:     uint64(info.IOSize),
	})

	for _, part := range info.Parts {
		dv := &blockpb.DiscoveredVolumeSpec{
			Type:           "partition",
			ParentDevPath:  devPath,
			DevPath:        devPath,
			Offset:         part.PartitionOffset,
			Size:           part.PartitionSize,
			SectorSize:     uint64(info.SectorSize),
			PartitionIndex: uint64(part.PartitionIndex),
		}

		if part.PartitionLabel != nil {
			dv.PartitionLabel = *part.PartitionLabel
		}

		if part.PartitionType != nil {
			dv.PartitionType = part.PartitionType.String()
		}

		if part.PartitionUUID != nil {
			dv.PartitionUuid = part.PartitionUUID.String()
		}

		discovered = append(discovered, dv)
	}

	return pSnapshot(t, volumes.PlanInput{
		VolumeConfigs:     configs,
		DiscoveredVolumes: discovered,
		Disks:             []volumes.DiskContext{disk},
		DevicesReady:      true,
	})
}

// assertStillUnpartitioned proves that planning did not write any partition table.
func assertStillUnpartitioned(t *testing.T, devPath string) {
	t.Helper()

	info, err := blkid.ProbePath(devPath)
	require.NoError(t, err)
	assert.Empty(t, info.Name, "planning wrote a partition signature to %q", devPath)
}

// gptPartitionCount reads the current number of GPT entries on the device.
func gptPartitionCount(t *testing.T, devPath string) int {
	t.Helper()

	bd, err := blockdev.NewFromPath(devPath)
	require.NoError(t, err)

	defer bd.Close() //nolint:errcheck

	gptdev, err := gpt.DeviceFromBlockDevice(bd)
	require.NoError(t, err)

	pt, err := gpt.Read(gptdev)
	require.NoError(t, err)

	count := 0

	for _, entry := range pt.Partitions() {
		if entry != nil {
			count++
		}
	}

	return count
}

// partitionByLabel reads the on-disk GPT and looks up an entry by label.
//
// CreatePartition is not idempotent: once the GPT write succeeded, a transient failure of the
// following wipe/device-open step must not be fixed by allocating another entry on retry.
func partitionByLabel(t *testing.T, devPath, label string) (volumes.CreatePartitionResult, bool) {
	t.Helper()

	bd, err := blockdev.NewFromPath(devPath)
	require.NoError(t, err)

	defer bd.Close() //nolint:errcheck

	gptdev, err := gpt.DeviceFromBlockDevice(bd)
	require.NoError(t, err)

	pt, err := gpt.Read(gptdev)
	require.NoError(t, err)

	for idx, entry := range pt.Partitions() {
		if entry == nil || entry.Name != label {
			continue
		}

		return volumes.CreatePartitionResult{
			PartitionIdx: idx + 1,
			Partition:    *entry,
			Size:         (entry.LastLBA - entry.FirstLBA + 1) * uint64(bd.GetSectorSize()),
		}, true
	}

	return volumes.CreatePartitionResult{}, false
}

// createPartitionWithRetry mirrors the retry behavior used by the existing tests, while
// recovering from a post-write transient failure by reading the already allocated entry back.
func createPartitionWithRetry(t *testing.T, devPath string, vc *block.VolumeConfig, hasPT bool) volumes.CreatePartitionResult {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	label := vc.TypedSpec().Provisioning.PartitionSpec.Label

	var err error

	for range 10 {
		result, createErr := volumes.CreatePartition(ctx, zaptest.NewLogger(t), devPath, vc, hasPT)
		if createErr == nil {
			return result
		}

		err = createErr

		if !xerrors.TagIs[volumes.Retryable](createErr) {
			break
		}

		// the GPT entry may already be on disk even though the post-write step failed
		if existing, ok := partitionByLabel(t, devPath, label); ok {
			return existing
		}

		time.Sleep(10 * time.Millisecond)
	}

	require.NoError(t, err)

	return volumes.CreatePartitionResult{}
}

func TestPlanMatchesRealProvisioning(t *testing.T) {
	checkRequirements(t)

	t.Run("empty disk, single fixed partition", func(t *testing.T) {
		diskPath := prepareRawImage(t, 256<<20)

		vc := pPartitionVC("ONE", 8<<20, 8<<20, "!disk.readonly")

		snapshot := snapshotFromRealDevice(t, diskPath, vc)
		plan := pPlan(t, snapshot)

		volume := pFind(plan, "ONE")
		require.NotNil(t, volume)
		require.Equal(t, volumes.DecisionAllocateNew, volume.Decision)
		require.NotNil(t, volume.Allocation)

		// planning must not have touched the device at all
		assertStillUnpartitioned(t, diskPath)

		result := createPartitionWithRetry(t, diskPath, vc, false)

		assert.Equal(t, volume.Allocation.PartitionIndex, result.PartitionIdx)
		assert.Equal(t, volume.Allocation.Size, result.Size)
		assert.Equal(t, volume.Allocation.FirstLBA, result.Partition.FirstLBA)
		assert.Equal(t, volume.Allocation.LastLBA, result.Partition.LastLBA)
	})

	t.Run("partitioned GPT disk, new partition uses the free space", func(t *testing.T) {
		diskPath := prepareRawImage(t, 256<<20)

		prepareGPT(t, diskPath, func(pt *gpt.Table) {
			_, _, err := pt.AllocatePartition(4<<20, "EXISTING", uuid.MustParse(testTypeUUID))
			require.NoError(t, err)
		})

		vc := pPartitionVC("TWO", 8<<20, 8<<20, "!disk.readonly")

		snapshot := snapshotFromRealDevice(t, diskPath, vc)
		plan := pPlan(t, snapshot)

		volume := pFind(plan, "TWO")
		require.NotNil(t, volume)
		require.Equal(t, volumes.DecisionAllocateNew, volume.Decision)
		require.NotNil(t, volume.Allocation)
		assert.True(t, volume.Allocation.HasGPT)
		assert.Equal(t, 2, volume.Allocation.PartitionIndex)

		assert.Equal(t, 1, gptPartitionCount(t, diskPath), "planning must not create GPT entries")

		result := createPartitionWithRetry(t, diskPath, vc, true)

		assert.Equal(t, 2, result.PartitionIdx)
		assert.Equal(t, volume.Allocation.Size, result.Size)
		assert.Equal(t, volume.Allocation.FirstLBA, result.Partition.FirstLBA)
		assert.Equal(t, volume.Allocation.LastLBA, result.Partition.LastLBA)
	})

	t.Run("two volumes sharing one disk are simulated in provisioning order", func(t *testing.T) {
		diskPath := prepareRawImage(t, 512<<20)

		// reversed order on purpose: the planner must sort them deterministically
		first := pPartitionVC("AVOL", 16<<20, 16<<20, "!disk.readonly")
		second := pPartitionVC("ZVOL", 16<<20, 16<<20, "!disk.readonly")

		snapshot := snapshotFromRealDevice(t, diskPath, second, first)
		plan := pPlan(t, snapshot)

		plannedFirst := pFind(plan, "AVOL")
		require.NotNil(t, plannedFirst)

		plannedSecond := pFind(plan, "ZVOL")
		require.NotNil(t, plannedSecond)

		assertStillUnpartitioned(t, diskPath)

		realFirst := createPartitionWithRetry(t, diskPath, first, false)
		realSecond := createPartitionWithRetry(t, diskPath, second, true)

		require.NotNil(t, plannedFirst.Allocation)
		require.NotNil(t, plannedSecond.Allocation)

		assert.Equal(t, realFirst.PartitionIdx, plannedFirst.Allocation.PartitionIndex)
		assert.Equal(t, realFirst.Partition.FirstLBA, plannedFirst.Allocation.FirstLBA)
		assert.Equal(t, realFirst.Partition.LastLBA, plannedFirst.Allocation.LastLBA)
		assert.Equal(t, realFirst.Size, plannedFirst.Allocation.Size)

		assert.Equal(t, realSecond.PartitionIdx, plannedSecond.Allocation.PartitionIndex)
		assert.Equal(t, realSecond.Partition.FirstLBA, plannedSecond.Allocation.FirstLBA)
		assert.Equal(t, realSecond.Partition.LastLBA, plannedSecond.Allocation.LastLBA)
		assert.Equal(t, realSecond.Size, plannedSecond.Allocation.Size)
	})
}
