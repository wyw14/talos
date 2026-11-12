// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package volumes

import (
	"github.com/siderolabs/go-blockdevice/v2/blkid"
	blockdev "github.com/siderolabs/go-blockdevice/v2/block"
	"github.com/siderolabs/go-blockdevice/v2/partitioning/gpt"
	"go.uber.org/zap"

	"github.com/siderolabs/talos/pkg/machinery/resources/block"
)

// DiskRejectedReason is the reason why a disk cannot be used for provisioning.
type DiskRejectedReason int

// Possible reasons why a disk cannot be used for provisioning.
const (
	GeneralError DiskRejectedReason = iota
	NotEnoughSpace
	WrongFormat
)

// CheckDiskResult is the result of checking a disk for provisioning.
type CheckDiskResult struct {
	// CanProvision indicates if the disk can be used for provisioning.
	CanProvision bool
	// HasGPT indicates if the disk has a GPT partition table.
	HasGPT bool
	// DiskSize is the size of the disk.
	DiskSize uint64
	// RejectedReason is the reason why the disk cannot be used for provisioning (if CanProvision is false).
	RejectedReason DiskRejectedReason
}

// diskProbeInfo is the subset of the blkid probe information required to take provisioning decisions.
//
// It is a pure value, so the same decisions can be replayed against an in-memory snapshot
// without touching the underlying device.
type diskProbeInfo struct {
	// Name is the probed signature name: an empty string means the disk is not partitioned,
	// "gpt" means it carries a GPT partition table, anything else is an incompatible format.
	Name string
	// Size is the overall size of the disk in bytes.
	Size uint64
	// SectorSize is the logical sector size of the disk in bytes.
	SectorSize uint
}

// gptOverheadSectors is the number of sectors reserved by a freshly created GPT (primary header,
// partition entry array and the secondary table copy), matching the historical estimate used
// when probing an unpartitioned disk.
const gptOverheadSectors = 67

// gptSignatureName is the blkid probe name reported by a disk carrying a GPT partition table.
const gptSignatureName = "gpt"

// checkDiskFromProbe is the pure decision logic behind CheckDiskForProvisioning.
//
// It performs no I/O: the probe information and, for GPT disks, the largest contiguous
// allocatable space are supplied by the caller (either gathered from a real device or
// simulated from a read-only snapshot). Keeping this side-effect free allows the read-only
// allocation planner to run the exact same checks as the real provisioning path.
func checkDiskFromProbe(volumeCfg *block.VolumeConfig, info diskProbeInfo, largestAllocatable uint64) CheckDiskResult {
	switch volumeCfg.TypedSpec().Type { //nolint:exhaustive
	case block.VolumeTypeDisk:
		return CheckDiskResult{
			CanProvision:   info.Name == "",
			DiskSize:       info.Size,
			RejectedReason: WrongFormat,
		}
	case block.VolumeTypePartition:
		if info.Name == "" {
			// if the disk is not partitioned, it can be used for partitioning, but we need to check the size
			overhead := uint64(info.SectorSize) * gptOverheadSectors // GPT + MBR

			return CheckDiskResult{
				CanProvision:   info.Size >= volumeCfg.TypedSpec().Provisioning.PartitionSpec.MinSize+overhead,
				DiskSize:       info.Size,
				RejectedReason: NotEnoughSpace,
			}
		}

		if info.Name != gptSignatureName {
			// not empty, and not gpt => can't be used for partitioning
			return CheckDiskResult{
				RejectedReason: WrongFormat,
			}
		}
	default:
		panic("unexpected volume type")
	}

	// the rest for partition type volumes with existing GPT partition table
	return CheckDiskResult{
		CanProvision:   largestAllocatable >= volumeCfg.TypedSpec().Provisioning.PartitionSpec.MinSize,
		HasGPT:         true,
		DiskSize:       info.Size,
		RejectedReason: NotEnoughSpace,
	}
}

// CheckDiskForProvisioning checks if the disk can be used for provisioning for the given volume configuration.
func CheckDiskForProvisioning(logger *zap.Logger, diskPath string, volumeCfg *block.VolumeConfig) CheckDiskResult {
	info, err := blkid.ProbePath(diskPath)
	if err != nil {
		logger.Error("error probing disk", zap.String("disk", diskPath), zap.Error(err))

		return CheckDiskResult{}
	}

	probe := diskProbeInfo{
		Name:       info.Name,
		Size:       info.Size,
		SectorSize: info.SectorSize,
	}

	if info.Name != gptSignatureName || volumeCfg.TypedSpec().Type != block.VolumeTypePartition {
		return checkDiskFromProbe(volumeCfg, probe, 0)
	}

	dev, err := blockdev.NewFromPath(diskPath)
	if err != nil {
		logger.Error("error opening disk", zap.String("disk", diskPath), zap.Error(err))

		return CheckDiskResult{}
	}

	defer dev.Close() //nolint:errcheck

	if err = dev.TryLock(false); err != nil {
		logger.Error("error locking disk", zap.String("disk", diskPath), zap.Error(err))

		return CheckDiskResult{}
	}

	defer dev.Unlock() //nolint:errcheck

	gptdev, err := gpt.DeviceFromBlockDevice(dev)
	if err != nil {
		logger.Error("error getting GPT device", zap.String("disk", diskPath), zap.Error(err))

		return CheckDiskResult{}
	}

	pt, err := gpt.Read(gptdev)
	if err != nil {
		logger.Error("error reading GPT", zap.String("disk", diskPath), zap.Error(err))

		return CheckDiskResult{}
	}

	available := pt.LargestContiguousAllocatable()

	logger.Debug("checking disk for provisioning", zap.String("disk", diskPath), zap.Uint64("available", available))

	return checkDiskFromProbe(volumeCfg, probe, available)
}
