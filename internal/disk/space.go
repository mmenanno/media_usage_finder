package disk

import (
	"fmt"
	"syscall"
)

// SpaceInfo contains disk space statistics
type SpaceInfo struct {
	TotalBytes  int64   // Total disk capacity
	FreeBytes   int64   // Free space available
	UsedBytes   int64   // Used space
	UsedPercent float64 // Percentage used
}

// GetDiskSpace queries filesystem statistics for a path using syscall.Statfs
// This works from within Docker containers without special privileges
func GetDiskSpace(path string) (*SpaceInfo, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	if err != nil {
		return nil, fmt.Errorf("statfs failed for %s: %w", path, err)
	}

	// Calculate space statistics
	// Note: Available blocks vs Free blocks
	// - Bfree: Free blocks including reserved blocks (root-only)
	// - Bavail: Free blocks available to non-root users
	// We use Bavail for user-accessible free space

	total := stat.Blocks * uint64(stat.Bsize)
	available := stat.Bavail * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)

	// Calculate used space
	// Used = Total - Free (including reserved)
	used := total - free

	// Calculate percentage used
	var usedPercent float64
	if total > 0 {
		usedPercent = float64(used) / float64(total) * 100
	}

	// Debug logging to diagnose incorrect percentages
	fmt.Printf("DEBUG GetDiskSpace(%s):\n", path)
	fmt.Printf("  Raw syscall values: Blocks=%d, Bfree=%d, Bavail=%d, Bsize=%d\n",
		stat.Blocks, stat.Bfree, stat.Bavail, stat.Bsize)
	fmt.Printf("  Calculated: total=%d, free=%d, available=%d, used=%d\n",
		total, free, available, used)
	fmt.Printf("  Result: %.2f TB total, %.2f TB used, %.1f%% full\n",
		float64(total)/1024/1024/1024/1024, float64(used)/1024/1024/1024/1024, usedPercent)

	return &SpaceInfo{
		TotalBytes:  int64(total),
		FreeBytes:   int64(available), // Use available (not free) for user-accessible space
		UsedBytes:   int64(used),
		UsedPercent: usedPercent,
	}, nil
}

// FormatBytes converts bytes to human-readable format
func FormatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
		PB = TB * 1024
	)

	absBytes := bytes
	if bytes < 0 {
		absBytes = -bytes
	}

	switch {
	case absBytes >= PB:
		return fmt.Sprintf("%.2f PB", float64(bytes)/float64(PB))
	case absBytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/float64(TB))
	case absBytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case absBytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case absBytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// GetDiskUsageSummary returns a summary string for a disk
func GetDiskUsageSummary(info *SpaceInfo) string {
	return fmt.Sprintf("%s used / %s total (%.1f%% full)",
		FormatBytes(info.UsedBytes),
		FormatBytes(info.TotalBytes),
		info.UsedPercent)
}

// GetDiskFreeSpace returns a summary of free space
func GetDiskFreeSpace(info *SpaceInfo) string {
	return fmt.Sprintf("%s free (%.1f%% available)",
		FormatBytes(info.FreeBytes),
		100.0-info.UsedPercent)
}

// CompareDiskUsage compares two disks and returns which is fuller
// Returns:
//
//	-1 if disk1 is less full than disk2
//	 0 if disks are equally full
//	 1 if disk1 is more full than disk2
func CompareDiskUsage(disk1, disk2 *DiskInfo) int {
	if disk1.UsedPercent < disk2.UsedPercent {
		return -1
	} else if disk1.UsedPercent > disk2.UsedPercent {
		return 1
	}
	return 0
}

// FindLeastFullDisk returns the disk with the lowest usage percentage
func FindLeastFullDisk(disks []*DiskInfo) *DiskInfo {
	if len(disks) == 0 {
		return nil
	}

	leastFull := disks[0]
	for _, disk := range disks[1:] {
		if disk.UsedPercent < leastFull.UsedPercent {
			leastFull = disk
		}
	}

	return leastFull
}

// FindMostFullDisk returns the disk with the highest usage percentage
func FindMostFullDisk(disks []*DiskInfo) *DiskInfo {
	if len(disks) == 0 {
		return nil
	}

	mostFull := disks[0]
	for _, disk := range disks[1:] {
		if disk.UsedPercent > mostFull.UsedPercent {
			mostFull = disk
		}
	}

	return mostFull
}

// CalculatePotentialSavings calculates space that could be freed by deleting a file
func CalculatePotentialSavings(fileSize int64, numCopies int) int64 {
	if numCopies <= 1 {
		return 0
	}
	// Savings = size * (copies - 1)
	// Keep one copy, delete the rest
	return fileSize * int64(numCopies-1)
}
