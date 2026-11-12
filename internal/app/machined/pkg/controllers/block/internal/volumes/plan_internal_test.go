// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package volumes

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	blockpb "github.com/siderolabs/talos/pkg/machinery/api/resource/definitions/block"
	taloscel "github.com/siderolabs/talos/pkg/machinery/cel"
	"github.com/siderolabs/talos/pkg/machinery/cel/celenv"
	"github.com/siderolabs/talos/pkg/machinery/resources/block"
)

// partitionConfig builds a partition-type volume config valid for planning tests.
func partitionConfig(id string, minSize, maxSize uint64, selector string) *block.VolumeConfig {
	vc := block.NewVolumeConfig(block.NamespaceName, id)
	vc.TypedSpec().Type = block.VolumeTypePartition
	vc.TypedSpec().Locator.Match = taloscel.MustExpression(
		taloscel.ParseBooleanExpression("false", celenv.VolumeLocator()))
	vc.TypedSpec().Provisioning.DiskSelector.Match = taloscel.MustExpression(
		taloscel.ParseBooleanExpression(selector, celenv.DiskLocator()))
	vc.TypedSpec().Provisioning.PartitionSpec.MinSize = minSize
	vc.TypedSpec().Provisioning.PartitionSpec.MaxSize = maxSize
	vc.TypedSpec().Provisioning.PartitionSpec.Label = id
	vc.TypedSpec().Provisioning.PartitionSpec.TypeUUID = "c12a7328-f81f-11d2-ba4b-00a0c93ec93b"

	return vc
}

const (
	tenGiB = 10 * 1024 * 1024 * 1024
	oneGiB = 1024 * 1024 * 1024
	oneMiB = 1024 * 1024
)

func internalDisk(size, sectorSize uint64) *blockpb.DiskSpec {
	return &blockpb.DiskSpec{
		DevPath:    "/dev/sda",
		Size:       size,
		SectorSize: sectorSize,
		IoSize:     sectorSize,
	}
}

func internalWholeDV(devPath string, size, sectorSize uint64, name string) *blockpb.DiscoveredVolumeSpec {
	return &blockpb.DiscoveredVolumeSpec{
		DevPath:    devPath,
		Name:       name,
		Size:       size,
		SectorSize: sectorSize,
	}
}

func internalPartition(devPath string, index, firstLBA, sectors, sectorSize uint64) *blockpb.DiscoveredVolumeSpec {
	return &blockpb.DiscoveredVolumeSpec{
		DevPath:        devPath,
		ParentDevPath:  "/dev/sda",
		PartitionIndex: index,
		PartitionType:  "c12a7328-f81f-11d2-ba4b-00a0c93ec93b",
		Offset:         firstLBA * sectorSize,
		Size:           sectors * sectorSize,
		SectorSize:     sectorSize,
	}
}

func TestCheckedArithmetic(t *testing.T) {
	t.Parallel()

	v, ok := checkedMul(math.MaxUint64, 2)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), v)

	v, ok = checkedMul(math.MaxUint64/2, 2)
	assert.True(t, ok)
	assert.Equal(t, uint64(math.MaxUint64-1), v)

	v, ok = checkedAdd(math.MaxUint64, 1)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), v)

	v, ok = checkedSub(10, 11)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), v)
}

func TestMemoryDeviceWriteSpy(t *testing.T) {
	t.Parallel()

	dev := newMemoryDevice(4096, 512, 512)

	n, err := dev.WriteAt(make([]byte, 512), 0)
	require.NoError(t, err)
	assert.Equal(t, 512, n)
	assert.Equal(t, 1, dev.WriteCalls())
	assert.Equal(t, 1, dev.MutatingCalls())

	require.NoError(t, dev.Sync())
	assert.Equal(t, 2, dev.MutatingCalls())

	dev.ResetWriteSpy()
	assert.Equal(t, 0, dev.MutatingCalls())
	assert.Equal(t, uint64(4096), dev.GetSize())
	assert.Equal(t, uint(512), dev.GetSectorSize())

	ioSize, err := dev.GetIOSize()
	require.NoError(t, err)
	assert.Equal(t, uint(512), ioSize)

	// out of range writes are rejected and still recorded
	_, err = dev.WriteAt(make([]byte, 512), 4096)
	assert.Error(t, err)
	assert.Equal(t, 1, dev.WriteCalls())
}

func TestCheckDiskFromProbe(t *testing.T) {
	t.Parallel()

	cfg := func(volumeType block.VolumeType, minSize uint64) *block.VolumeConfig {
		vc := block.NewVolumeConfig(block.NamespaceName, "TEST")
		vc.TypedSpec().Type = volumeType
		vc.TypedSpec().Provisioning.PartitionSpec.MinSize = minSize

		return vc
	}

	t.Run("disk volume requires empty signature", func(t *testing.T) {
		t.Parallel()

		res := checkDiskFromProbe(cfg(block.VolumeTypeDisk, 0), diskProbeInfo{Name: "", Size: 100}, 0)
		assert.True(t, res.CanProvision)
		assert.Equal(t, uint64(100), res.DiskSize)

		res = checkDiskFromProbe(cfg(block.VolumeTypeDisk, 0), diskProbeInfo{Name: gptSignatureName, Size: 100}, 0)
		assert.False(t, res.CanProvision)
		assert.Equal(t, WrongFormat, res.RejectedReason)
	})

	t.Run("partition on empty disk reserves GPT overhead", func(t *testing.T) {
		t.Parallel()

		// exactly min + 67 sectors fits
		res := checkDiskFromProbe(
			cfg(block.VolumeTypePartition, 100*512),
			diskProbeInfo{Name: "", Size: 167 * 512, SectorSize: 512}, 0)
		assert.True(t, res.CanProvision)

		res = checkDiskFromProbe(
			cfg(block.VolumeTypePartition, 100*512),
			diskProbeInfo{Name: "", Size: 166 * 512, SectorSize: 512}, 0)
		assert.False(t, res.CanProvision)
		assert.Equal(t, NotEnoughSpace, res.RejectedReason)
	})

	t.Run("partition on foreign signature is rejected", func(t *testing.T) {
		t.Parallel()

		res := checkDiskFromProbe(
			cfg(block.VolumeTypePartition, oneMiB),
			diskProbeInfo{Name: "xfs", Size: tenGiB, SectorSize: 512}, 0)
		assert.False(t, res.CanProvision)
		assert.Equal(t, WrongFormat, res.RejectedReason)
	})

	t.Run("partition on gpt uses largest allocatable", func(t *testing.T) {
		t.Parallel()

		res := checkDiskFromProbe(
			cfg(block.VolumeTypePartition, 2*oneGiB),
			diskProbeInfo{Name: gptSignatureName, Size: tenGiB, SectorSize: 512}, oneGiB)
		assert.False(t, res.CanProvision)
		assert.True(t, res.HasGPT)
		assert.Equal(t, NotEnoughSpace, res.RejectedReason)

		res = checkDiskFromProbe(
			cfg(block.VolumeTypePartition, 2*oneGiB),
			diskProbeInfo{Name: gptSignatureName, Size: tenGiB, SectorSize: 512}, 2*oneGiB)
		assert.True(t, res.CanProvision)
	})
}

func TestSyntheticGPTRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		sectorSize uint64
		parts      []*blockpb.DiscoveredVolumeSpec
	}{
		{
			name:       "empty 512e disk",
			sectorSize: 512,
		},
		{
			name:       "empty 4Kn disk",
			sectorSize: 4096,
		},
		{
			name:       "fragmented 512e GPT",
			sectorSize: 512,
			parts: []*blockpb.DiscoveredVolumeSpec{
				internalPartition("/dev/sda1", 1, 2048, oneGiB/512, 512),
				// deliberate 1GiB gap, then another partition
				internalPartition("/dev/sda2", 2, 2048+2*(oneGiB/512), oneGiB/512, 512),
			},
		},
		{
			name:       "partitioned 4Kn GPT",
			sectorSize: 4096,
			parts: []*blockpb.DiscoveredVolumeSpec{
				internalPartition("/dev/sda1", 1, 256, oneGiB/4096, 4096),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			disk := internalDisk(tenGiB, tc.sectorSize)
			whole := internalWholeDV("/dev/sda", tenGiB, tc.sectorSize, "gpt")

			if len(tc.parts) == 0 {
				whole.Name = ""
			}

			sim, err := buildSimulatedDisk(disk, whole, tc.parts)
			require.NoError(t, err)
			require.NotNil(t, sim.table)

			// the entries parsed by the real gpt.Read must match the snapshot layout
			entries := sim.table.Partitions()
			require.Len(t, entries, len(tc.parts))

			for i, part := range tc.parts {
				require.NotNil(t, entries[i])
				assert.Equal(t, part.Offset/tc.sectorSize, entries[i].FirstLBA)
				assert.Equal(t, part.Offset/tc.sectorSize+part.Size/tc.sectorSize-1, entries[i].LastLBA)
			}

			// the reported free ranges must agree with the gpt package's own computation
			ranges, _ := sim.freeRanges(defaultMaxRanges)

			var largest uint64

			for _, freeRange := range ranges {
				assert.Equal(t, uint64(0), freeRange.StartLBA%sim.alignmentLBA, "range starts unaligned")
				largest = max(largest, freeRange.Size)
			}

			assert.Equal(t, sim.table.LargestContiguousAllocatable(), largest)
		})
	}
}

func TestSyntheticGPTUnknownGeometry(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		disk *blockpb.DiskSpec
		part *blockpb.DiscoveredVolumeSpec
	}{
		{
			name: "zero sector size",
			disk: internalDisk(tenGiB, 0),
		},
		{
			name: "zero size",
			disk: internalDisk(0, 512),
		},
		{
			name: "disk too small",
			disk: internalDisk(32*512, 512),
		},
		{
			name: "unaligned partition offset",
			disk: internalDisk(tenGiB, 512),
			part: &blockpb.DiscoveredVolumeSpec{
				ParentDevPath:  "/dev/sda",
				PartitionIndex: 1,
				Offset:         123,
				Size:           oneMiB,
				SectorSize:     512,
			},
		},
		{
			name: "overflowing partition size",
			disk: internalDisk(tenGiB, 512),
			part: &blockpb.DiscoveredVolumeSpec{
				ParentDevPath:  "/dev/sda",
				PartitionIndex: 1,
				Offset:         2048 * 512,
				Size:           math.MaxUint64 - 511,
				SectorSize:     512,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			whole := internalWholeDV("/dev/sda", tc.disk.Size, tc.disk.SectorSize, "gpt")

			var parts []*blockpb.DiscoveredVolumeSpec
			if tc.part != nil {
				parts = []*blockpb.DiscoveredVolumeSpec{tc.part}
			}

			_, err := buildSimulatedDisk(tc.disk, whole, parts)
			assert.ErrorIs(t, err, errUnknownGeometry)
		})
	}
}

func TestPlanningPerformsNoDeviceMutations(t *testing.T) {
	t.Parallel()

	disk := DiskContext{Disk: internalDisk(tenGiB, 512)}
	whole := internalWholeDV("/dev/sda", tenGiB, 512, "gpt")
	parts := []*blockpb.DiscoveredVolumeSpec{
		internalPartition("/dev/sda1", 1, 2048, oneGiB/512, 512),
	}

	snapshot, err := NewSnapshot(PlanInput{
		VolumeConfigs: []*block.VolumeConfig{
			partitionConfig("PLAN", 2*oneMiB, 2*oneMiB, "!disk.readonly"),
		},
		DiscoveredVolumes: append([]*blockpb.DiscoveredVolumeSpec{whole}, parts...),
		Disks:             []DiskContext{disk},
		DevicesReady:      true,
	})
	require.NoError(t, err)

	plan, err := PlanAllocation(context.Background(), zaptest.NewLogger(t), snapshot)
	require.NoError(t, err)
	require.Len(t, plan.Volumes, 1)
	assert.Equal(t, DecisionAllocateNew, plan.Volumes[0].Decision)

	for devPath, sim := range snapshot.topology {
		assert.Equal(t, 0, sim.mem.MutatingCalls(), "simulated disk %q received mutating calls during planning", devPath)
		assert.Equal(t, 0, sim.mem.WriteCalls())
	}
}
