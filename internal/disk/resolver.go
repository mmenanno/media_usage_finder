package disk

import "fmt"

// DeviceResolver resolves device IDs to friendly names for UI display
type DeviceResolver struct {
	disks map[int64]string // device_id → disk name
	fuse  int64             // FUSE device_id (if detected)
}

// NewDeviceResolver creates a new device resolver from detected disks
func NewDeviceResolver(disks []*DiskInfo) *DeviceResolver {
	resolver := &DeviceResolver{
		disks: make(map[int64]string),
		fuse:  -1, // Initialize to invalid value
	}

	for _, disk := range disks {
		resolver.disks[disk.DeviceID] = disk.Name
	}

	return resolver
}

// SetFUSEDeviceID sets the device ID for the FUSE layer
// This is typically detected during the first scan
func (r *DeviceResolver) SetFUSEDeviceID(deviceID int64) {
	r.fuse = deviceID
}

// ResolveName returns the friendly name for a device ID
// Returns "Disk 1", "FUSE", or "Unknown"
func (r *DeviceResolver) ResolveName(deviceID int64) string {
	if name, ok := r.disks[deviceID]; ok {
		return name
	}
	if deviceID == r.fuse && r.fuse != -1 {
		return "FUSE"
	}
	return "Unknown"
}

// ResolveDisplayName returns the display name with device ID
// Returns "Disk 1 (44)", "FUSE (143)", or "Unknown (99)"
func (r *DeviceResolver) ResolveDisplayName(deviceID int64) string {
	name := r.ResolveName(deviceID)
	return fmt.Sprintf("%s (%d)", name, deviceID)
}

// ResolveColor returns the badge color for a device ID
// Blue for physical disks, purple for FUSE, gray for unknown
func (r *DeviceResolver) ResolveColor(deviceID int64) string {
	if _, ok := r.disks[deviceID]; ok {
		return "blue"
	}
	if deviceID == r.fuse && r.fuse != -1 {
		return "purple"
	}
	return "gray"
}

// GetAllDeviceIDs returns all known device IDs (disks + FUSE)
func (r *DeviceResolver) GetAllDeviceIDs() []int64 {
	ids := make([]int64, 0, len(r.disks)+1)
	for id := range r.disks {
		ids = append(ids, id)
	}
	if r.fuse != -1 {
		ids = append(ids, r.fuse)
	}
	return ids
}

// IsDiskDeviceID checks if a device ID belongs to a configured disk
func (r *DeviceResolver) IsDiskDeviceID(deviceID int64) bool {
	_, ok := r.disks[deviceID]
	return ok
}

// IsFUSEDeviceID checks if a device ID is the FUSE layer
func (r *DeviceResolver) IsFUSEDeviceID(deviceID int64) bool {
	return deviceID == r.fuse && r.fuse != -1
}
