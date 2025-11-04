package disk

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// UnraidStatsPath is the default path where Unraid stats are mounted in the container
const UnraidStatsPath = "/unraid/stats"

// UnraidDiskStats contains Unraid-specific disk statistics from disks.ini
type UnraidDiskStats struct {
	Name       string // e.g., "disk1", "disk2"
	FSSize     int64  // Filesystem size in KB
	FSFree     int64  // Free space in KB
	FSUsed     int64  // Used space in KB
	FSType     string // e.g., "zfs", "xfs"
	FSStatus   string // e.g., "Mounted"
	DeviceName string // e.g., "sdi", "sdj"
}

// UnraidStatsReader parses Unraid's disks.ini file
type UnraidStatsReader struct {
	statsPath string
	disks     map[string]*UnraidDiskStats // Map of disk name -> stats
}

// NewUnraidStatsReader creates a new Unraid stats reader
func NewUnraidStatsReader(statsPath string) *UnraidStatsReader {
	if statsPath == "" {
		statsPath = UnraidStatsPath
	}
	return &UnraidStatsReader{
		statsPath: statsPath,
		disks:     make(map[string]*UnraidDiskStats),
	}
}

// IsUnraidStatsAvailable checks if Unraid stats directory is mounted
func IsUnraidStatsAvailable(statsPath string) bool {
	if statsPath == "" {
		statsPath = UnraidStatsPath
	}
	disksIniPath := filepath.Join(statsPath, "disks.ini")
	_, err := os.Stat(disksIniPath)
	return err == nil
}

// ParseDisksINI reads and parses the Unraid disks.ini file
func (r *UnraidStatsReader) ParseDisksINI() error {
	disksIniPath := filepath.Join(r.statsPath, "disks.ini")

	file, err := os.Open(disksIniPath)
	if err != nil {
		return fmt.Errorf("failed to open disks.ini: %w", err)
	}
	defer file.Close()

	r.disks = make(map[string]*UnraidDiskStats)

	var currentDisk *UnraidDiskStats
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Check for section header ["diskN"] or [diskN]
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			// Remove brackets and quotes
			diskName := strings.Trim(line, `[]"`)
			// Only process data disks (disk1, disk2, etc.), not parity or cache
			if strings.HasPrefix(diskName, "disk") && len(diskName) > 4 {
				currentDisk = &UnraidDiskStats{Name: diskName}
				r.disks[diskName] = currentDisk
			}
		} else if currentDisk != nil && strings.Contains(line, "=") {
			// Parse key=value pairs
			parts := strings.SplitN(line, "=", 2)
			if len(parts) != 2 {
				continue
			}
			key := parts[0]
			value := strings.Trim(parts[1], `"`)

			switch key {
			case "device":
				currentDisk.DeviceName = value
			case "fsType":
				currentDisk.FSType = value
			case "fsStatus":
				currentDisk.FSStatus = value
			case "fsSize":
				if val, err := strconv.ParseInt(value, 10, 64); err == nil {
					currentDisk.FSSize = val
				}
			case "fsFree":
				if val, err := strconv.ParseInt(value, 10, 64); err == nil {
					currentDisk.FSFree = val
				}
			case "fsUsed":
				if val, err := strconv.ParseInt(value, 10, 64); err == nil {
					currentDisk.FSUsed = val
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading disks.ini: %w", err)
	}

	fmt.Printf("Parsed Unraid disks.ini: found %d data disk(s)\n", len(r.disks))
	return nil
}

// GetDiskStats returns stats for a specific disk by name
func (r *UnraidStatsReader) GetDiskStats(diskName string) (*UnraidDiskStats, bool) {
	stats, ok := r.disks[diskName]
	return stats, ok
}

// GetAllDiskStats returns all parsed disk stats
func (r *UnraidStatsReader) GetAllDiskStats() map[string]*UnraidDiskStats {
	return r.disks
}

// ExtractDiskNameFromPath extracts disk name (e.g., "disk1") from mount path (e.g., "/disk1")
func ExtractDiskNameFromPath(mountPath string) string {
	// Remove leading/trailing slashes and return base name
	base := filepath.Base(strings.TrimSuffix(mountPath, "/"))

	// Only return if it matches disk pattern (disk1, disk2, etc.)
	if strings.HasPrefix(base, "disk") && len(base) > 4 {
		return base
	}

	return ""
}

// ConvertKBToBytes converts kilobytes to bytes
func ConvertKBToBytes(kb int64) int64 {
	return kb * 1024
}

// GetSpaceInfoFromUnraid converts Unraid disk stats to SpaceInfo
func GetSpaceInfoFromUnraid(stats *UnraidDiskStats) *SpaceInfo {
	totalBytes := ConvertKBToBytes(stats.FSSize)
	usedBytes := ConvertKBToBytes(stats.FSUsed)
	freeBytes := ConvertKBToBytes(stats.FSFree)

	var usedPercent float64
	if totalBytes > 0 {
		usedPercent = float64(usedBytes) / float64(totalBytes) * 100
	}

	return &SpaceInfo{
		TotalBytes:  totalBytes,
		FreeBytes:   freeBytes,
		UsedBytes:   usedBytes,
		UsedPercent: usedPercent,
	}
}
