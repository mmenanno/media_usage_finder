package disk

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/config"
)

// DiskInfo contains information about a mounted disk
type DiskInfo struct {
	Name        string    // User-friendly name (e.g., "Disk 1")
	MountPath   string    // Container mount path (e.g., "/disk1")
	DeviceID    int64     // Filesystem device ID from stat
	TotalBytes  int64     // Total disk capacity
	FreeBytes   int64     // Free space available
	UsedBytes   int64     // Used space
	UsedPercent float64   // Percentage used
	LastUpdated time.Time // When space info was last refreshed
}

// Detector manages disk detection and information
type Detector struct {
	config  []config.DiskConfig
	diskMap map[int64]*DiskInfo // Map of device_id -> DiskInfo
	mu      sync.RWMutex        // Protects diskMap
}

// NewDetector creates a new disk detector
func NewDetector(diskConfigs []config.DiskConfig) *Detector {
	return &Detector{
		config:  diskConfigs,
		diskMap: make(map[int64]*DiskInfo),
	}
}

// DetectDisks stats each configured mount point and builds disk info map
func (d *Detector) DetectDisks() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Clear existing map
	d.diskMap = make(map[int64]*DiskInfo)

	// Check if Unraid stats are available
	var unraidReader *UnraidStatsReader
	if IsUnraidStatsAvailable("") {
		fmt.Printf("Unraid stats detected, using /unraid/stats for accurate disk usage\n")
		unraidReader = NewUnraidStatsReader("")
		if err := unraidReader.ParseDisksINI(); err != nil {
			fmt.Printf("Warning: Failed to parse Unraid disks.ini: %v (falling back to statfs)\n", err)
			unraidReader = nil
		}
	}

	for _, diskConfig := range d.config {
		// Stat the mount point to get device ID (always needed)
		var stat syscall.Stat_t
		if err := syscall.Stat(diskConfig.MountPath, &stat); err != nil {
			// If mount doesn't exist, log warning but continue
			fmt.Printf("Warning: Could not stat disk mount %s: %v\n", diskConfig.MountPath, err)
			continue
		}

		deviceID := int64(stat.Dev)

		// Try to get disk space from Unraid stats first
		var spaceInfo *SpaceInfo
		if unraidReader != nil {
			diskName := ExtractDiskNameFromPath(diskConfig.MountPath)
			if diskName != "" {
				if unraidStats, ok := unraidReader.GetDiskStats(diskName); ok {
					spaceInfo = GetSpaceInfoFromUnraid(unraidStats)
					fmt.Printf("Using Unraid stats for %s (%s): %.2f TB total, %.2f TB used, %.1f%% full\n",
						diskConfig.Name, diskName,
						float64(spaceInfo.TotalBytes)/1024/1024/1024/1024,
						float64(spaceInfo.UsedBytes)/1024/1024/1024/1024,
						spaceInfo.UsedPercent)
				} else {
					fmt.Printf("Warning: No Unraid stats found for %s, falling back to statfs\n", diskName)
				}
			}
		}

		// Fall back to statfs if Unraid stats not available
		if spaceInfo == nil {
			var err error
			spaceInfo, err = GetDiskSpace(diskConfig.MountPath)
			if err != nil {
				fmt.Printf("Warning: Could not get disk space for %s: %v\n", diskConfig.MountPath, err)
				continue
			}
		}

		// Create DiskInfo
		diskInfo := &DiskInfo{
			Name:        diskConfig.Name,
			MountPath:   diskConfig.MountPath,
			DeviceID:    deviceID,
			TotalBytes:  spaceInfo.TotalBytes,
			FreeBytes:   spaceInfo.FreeBytes,
			UsedBytes:   spaceInfo.UsedBytes,
			UsedPercent: spaceInfo.UsedPercent,
			LastUpdated: time.Now(),
		}

		// Store in map
		d.diskMap[deviceID] = diskInfo

		fmt.Printf("Detected disk: %s (device_id=%d, mount=%s, used=%.1f%%)\n",
			diskInfo.Name, deviceID, diskInfo.MountPath, diskInfo.UsedPercent)
	}

	if len(d.diskMap) == 0 {
		return fmt.Errorf("no disks detected - check your disk configuration and Docker mounts")
	}

	fmt.Printf("Successfully detected %d disk(s)\n", len(d.diskMap))
	return nil
}

// GetDiskForFile returns disk info for a given device_id
func (d *Detector) GetDiskForFile(deviceID int64) (*DiskInfo, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	diskInfo, ok := d.diskMap[deviceID]
	if !ok {
		return nil, fmt.Errorf("no disk found for device_id %d", deviceID)
	}

	return diskInfo, nil
}

// GetAllDisks returns all detected disks in configuration order
func (d *Detector) GetAllDisks() []*DiskInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Build result in config order to maintain consistent ordering
	disks := make([]*DiskInfo, 0, len(d.diskMap))
	for _, diskConfig := range d.config {
		// Find disk in map by matching mount path
		for _, disk := range d.diskMap {
			if disk.MountPath == diskConfig.MountPath {
				disks = append(disks, disk)
				break
			}
		}
	}

	return disks
}

// RefreshDiskSpace updates space information for all disks
func (d *Detector) RefreshDiskSpace() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for deviceID, diskInfo := range d.diskMap {
		spaceInfo, err := GetDiskSpace(diskInfo.MountPath)
		if err != nil {
			fmt.Printf("Warning: Could not refresh disk space for %s: %v\n", diskInfo.Name, err)
			continue
		}

		// Update space information
		diskInfo.TotalBytes = spaceInfo.TotalBytes
		diskInfo.FreeBytes = spaceInfo.FreeBytes
		diskInfo.UsedBytes = spaceInfo.UsedBytes
		diskInfo.UsedPercent = spaceInfo.UsedPercent
		diskInfo.LastUpdated = time.Now()

		d.diskMap[deviceID] = diskInfo
	}

	return nil
}

// GetDiskCount returns the number of detected disks
func (d *Detector) GetDiskCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return len(d.diskMap)
}

// IsDiskMounted checks if a specific mount path is accessible
func IsDiskMounted(mountPath string) bool {
	_, err := os.Stat(mountPath)
	return err == nil
}

// GetDeviceIDForPath returns the device ID for a given path
func GetDeviceIDForPath(path string) (int64, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return 0, fmt.Errorf("could not stat path %s: %w", path, err)
	}

	return int64(stat.Dev), nil
}
