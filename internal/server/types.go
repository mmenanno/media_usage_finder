package server

import (
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
	"github.com/mmenanno/media-usage-finder/internal/duplicates"
	"github.com/mmenanno/media-usage-finder/internal/stats"
)

// API Response Types - Typed structs instead of map[string]interface{}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status  string                 `json:"status"`
	Version string                 `json:"version"`
	Checks  map[string]interface{} `json:"checks"`
}

// ServiceHealthCheck represents a service health check result
type ServiceHealthCheck struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// ScannerHealthCheck represents scanner health check result
type ScannerHealthCheck struct {
	Status   string  `json:"status"`
	Running  bool    `json:"running"`
	Progress float64 `json:"progress,omitempty"`
	Phase    string  `json:"phase,omitempty"`
}

// ScanStartResponse represents the response when starting a scan
type ScanStartResponse struct {
	Status      string `json:"status"`
	Message     string `json:"message"`
	Incremental bool   `json:"incremental"`
}

// ScanProgressResponse represents scan progress information
type ScanProgressResponse struct {
	Running bool `json:"running"`
	// If running, include snapshot fields from scanner.ProgressSnapshot
	TotalFiles      int64   `json:"total_files,omitempty"`
	ProcessedFiles  int64   `json:"processed_files,omitempty"`
	PercentComplete float64 `json:"percent_complete,omitempty"`
	CurrentPhase    string  `json:"current_phase,omitempty"`
	ETA             string  `json:"eta,omitempty"`
}

// FileDetailsResponse represents detailed file information
type FileDetailsResponse struct {
	ID            int64                        `json:"id"`
	Path          string                       `json:"path"`
	Size          int64                        `json:"size"`
	Inode         int64                        `json:"inode"`
	DeviceID      int64                        `json:"device_id"`
	DeviceName    string                       `json:"device_name,omitempty"`    // Friendly device name (e.g., "Disk 1 (44)")
	DeviceColor   string                       `json:"device_color,omitempty"`   // Badge color for device
	ModifiedTime  int64                        `json:"modified_time"`
	LastVerified  int64                        `json:"last_verified"`
	IsOrphaned    bool                         `json:"is_orphaned"`
	CreatedAt     int64                        `json:"created_at"`
	Usage         []*database.Usage            `json:"usage"`
	Hardlinks     []string                     `json:"hardlinks,omitempty"`
	DiskLocations []*database.FileDiskLocation `json:"disk_locations,omitempty"` // Disk-specific locations
}

// BulkDeleteResponse represents the result of a bulk deletion
type BulkDeleteResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Deleted int    `json:"deleted"`
	Errors  int    `json:"errors,omitempty"`
}

// BulkRescanResponse represents the result of a bulk rescan marking
type BulkRescanResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Count   int64  `json:"count"`
}

// ConfigSaveResponse represents the result of saving configuration
type ConfigSaveResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// TestServiceResponse represents service connection test result
type TestServiceResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Service string `json:"service,omitempty"`
}

// Template data structures for rendering HTML pages

// DashboardData represents data for the dashboard template
type DashboardData struct {
	Stats                *stats.Stats
	Title                string
	Version              string // Application version
	HasActiveScan        bool
	HasInterruptedScan   bool
	InterruptedScanID    int64
	InterruptedScanPhase string
	Disks                []*disk.DiskInfo // Detected disks with usage information
}

// FilesData represents data for the files list template
type FilesData struct {
	Files             interface{}
	Total             int64
	Page              int64
	Limit             int
	TotalPages        int
	Title             string
	Orphaned          bool
	Hardlinks         bool
	Service           string   // Deprecated: Use Services instead (kept for backward compatibility)
	Services          []string
	ServiceFilterMode string
	Search            string
	OrderBy           string
	Direction         string
	Extensions        []string
	Devices                  []string             // Selected device names for filtering
	AvailableDisks           []*disk.DiskInfo     // Available disks for filter dropdown
	DiskResolver             *disk.DeviceResolver // For resolving device IDs to friendly names
	HasDiskLocations         bool                 // True if disk location tracking is enabled
	Version                  string               // Application version
	DeleteFilesFromFilesystem bool                // Config setting for filesystem deletion
}

// ConfigData represents data for the configuration template
type ConfigData struct {
	Config             interface{}
	Title              string
	Version            string // Application version
	CPUCores           int    // Number of CPU cores on the server
	RecommendedWorkers int    // Recommended number of hash workers
}

// StatsData represents data for the statistics template
type StatsData struct {
	Stats                  *stats.Stats
	Title                  string
	Disks                  []*disk.DiskInfo
	CrossDiskDuplicates    int64 // Count of files on multiple disks
	SameDiskDuplicates     int64 // Count of duplicate file groups on same disk
	HasDiskLocations       bool  // True if disk location tracking is enabled
	Version                string // Application version
}

// HardlinksData represents data for the hardlinks template
type HardlinksData struct {
	Groups     interface{}
	Total      int64
	Showing    int    // Number of groups shown on current page
	Page       int64
	TotalPages int
	Title      string
	Search     string // Search query for filtering by path
	OrderBy    string // Sort field (link_count, space_saved, first_path)
	Direction  string // Sort direction (asc, desc)
	Version    string // Application version
}

// ScansData represents data for the scans history template
type ScansData struct {
	Scans      interface{}
	Total      int64
	Page       int64
	TotalPages int
	Title      string
	Version    string // Application version
}

// AdvancedData represents data for the advanced settings template
type AdvancedData struct {
	Stats   interface{}
	Title   string
	Version string // Application version
}

// DuplicatesData represents data for the duplicates page template
type DuplicatesData struct {
	Title                  string
	Version                string
	ActiveTab              string
	CrossDiskGroups        []*duplicates.ConsolidationPlan
	SameDiskGroups         []*duplicates.ConsolidationPlan
	CrossDiskCount         int64
	SameDiskCount          int64
	TotalSavings           int64
	CrossDiskSavings       int64
	SameDiskSavings        int64
	CrossDiskFilesToDelete int
	SameDiskFilesToLink    int
	HashScanningEnabled    bool
	DisplayLimit           int
	ShowingCrossDisk       int
	ShowingSameDisk        int
	// Pagination fields
	Page       int
	TotalPages int
	Total      int64
	Limit      int
	// Filter fields
	Filters database.DuplicateFilters
	// Disk location tracking
	FilesMissingDiskLocations int64 // Count of files missing disk location data
	SameDiskGroupsSkipped     int   // Count of same-disk groups skipped due to missing disk locations
}
