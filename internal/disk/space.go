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

// ParseSize converts human-readable size strings to bytes
// Supports formats: "4MB", "10GB", "512KB", "2TB", or plain numbers (bytes)
// Case-insensitive. Returns error for invalid formats.
func ParseSize(sizeStr string) (int64, error) {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
		PB = TB * 1024
	)

	if sizeStr == "" {
		return 0, fmt.Errorf("size string cannot be empty")
	}

	// Try parsing as plain number first (bytes)
	var value float64
	var unit string
	n, err := fmt.Sscanf(sizeStr, "%f%s", &value, &unit)

	if err != nil || n == 0 {
		return 0, fmt.Errorf("invalid size format: %s (expected format like '4MB', '10GB', or '1024')", sizeStr)
	}

	if value < 0 {
		return 0, fmt.Errorf("size cannot be negative: %s", sizeStr)
	}

	// If no unit specified, treat as bytes
	if n == 1 {
		return int64(value), nil
	}

	// Convert unit to uppercase for case-insensitive matching
	unit = fmt.Sprintf("%s", sizeStr[len(sizeStr)-len(unit):])
	unitUpper := ""
	for _, r := range unit {
		if r >= 'a' && r <= 'z' {
			unitUpper += string(r - 32)
		} else {
			unitUpper += string(r)
		}
	}

	// Match unit and calculate bytes
	switch unitUpper {
	case "B":
		return int64(value), nil
	case "KB":
		return int64(value * KB), nil
	case "MB":
		return int64(value * MB), nil
	case "GB":
		return int64(value * GB), nil
	case "TB":
		return int64(value * TB), nil
	case "PB":
		return int64(value * PB), nil
	default:
		return 0, fmt.Errorf("unsupported unit: %s (supported: B, KB, MB, GB, TB, PB)", unitUpper)
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
