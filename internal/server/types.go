package server

import "github.com/mmenanno/media-usage-finder/internal/database"

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
	ID           int64             `json:"id"`
	Path         string            `json:"path"`
	Size         int64             `json:"size"`
	Inode        int64             `json:"inode"`
	DeviceID     int64             `json:"device_id"`
	ModifiedTime int64             `json:"modified_time"`
	LastVerified int64             `json:"last_verified"`
	IsOrphaned   bool              `json:"is_orphaned"`
	CreatedAt    int64             `json:"created_at"`
	Usage        []*database.Usage `json:"usage"`
	Hardlinks    []string          `json:"hardlinks,omitempty"`
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
	Stats interface{}
	Title string
}

// FilesData represents data for the files list template
type FilesData struct {
	Files      interface{}
	Total      int
	Page       int
	Limit      int
	TotalPages int
	Title      string
	Orphaned   bool
	Hardlinks  bool
	Service    string
	Search     string
}

// ConfigData represents data for the configuration template
type ConfigData struct {
	Config interface{}
	Title  string
}

// StatsData represents data for the statistics template
type StatsData struct {
	Stats interface{}
	Title string
}

// HardlinksData represents data for the hardlinks template
type HardlinksData struct {
	Groups     interface{}
	Total      int
	Page       int
	TotalPages int
	Title      string
}

// ScansData represents data for the scans history template
type ScansData struct {
	Scans      interface{}
	Total      int
	Page       int
	TotalPages int
	Title      string
}
