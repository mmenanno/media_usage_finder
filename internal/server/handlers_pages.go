package server

import (
	"log"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/constants"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
	"github.com/mmenanno/media-usage-finder/internal/stats"
)

func (s *Server) HandleIndex(w http.ResponseWriter, r *http.Request) {
	// Trigger stale scan cleanup by calling GetCurrentScan
	// This will mark any stale running scans as interrupted
	_, _ = s.db.GetCurrentScan()

	statistics := s.getStats()
	if statistics == nil {
		log.Println("ERROR: Failed to calculate dashboard stats")
		respondError(w, http.StatusInternalServerError, "Failed to calculate statistics. Database may be unavailable", "stats_calculation_failed")
		return
	}

	// Check if there's an active scan to conditionally render SSE connection
	hasActiveScan := s.scanner.GetProgress() != nil

	// Check if there's an interrupted scan that can be resumed
	var hasInterruptedScan bool
	var interruptedScanID int64
	var interruptedScanPhase string

	interruptedScan, err := s.db.GetLastInterruptedScan()
	if err == nil && interruptedScan != nil {
		hasInterruptedScan = true
		interruptedScanID = interruptedScan.ID
		if interruptedScan.CurrentPhase != nil {
			interruptedScanPhase = *interruptedScan.CurrentPhase
		}
	}

	// Get disk information if disk detector is configured
	var disks []*disk.DiskInfo
	if s.diskDetector != nil {
		// Refresh disk space information (uses 5-minute cache)
		if err := s.diskDetector.RefreshDiskSpace(); err != nil {
			log.Printf("Warning: Failed to refresh disk space: %v", err)
		}
		disks = s.diskDetector.GetAllDisks()
	}

	// Duplicate statistics are now included in cached stats (statistics.DuplicateStats)
	// No need for separate GetDuplicateStats() call

	data := map[string]interface{}{
		"Stats":                statistics,
		"Title":                "Dashboard",
		"Version":              s.version,
		"HasActiveScan":        hasActiveScan,
		"HasInterruptedScan":   hasInterruptedScan,
		"InterruptedScanID":    interruptedScanID,
		"InterruptedScanPhase": interruptedScanPhase,
		"Disks":                disks,
		"DuplicateStats":       statistics.DuplicateStats,
	}

	s.renderTemplate(w, "dashboard.html", data)
}

// getStats returns stats via the SWR cache. Callers receive an
// immediate response: a fresh value when the cache is hot, the most
// recent stale value when a background refresh is in flight, or a
// freshly-computed value on cold start / hard expiry. Errors are
// logged; callers get nil only when no stale value exists either.
func (s *Server) getStats() *stats.Stats {
	statistics, err := s.statsCache.Get(func() (*stats.Stats, error) {
		return stats.NewCalculator(s.db).Calculate()
	})
	if err != nil {
		log.Printf("Failed to calculate stats: %v (returning %s value)",
			err, ifNil(statistics, "no", "stale"))
	}
	return statistics
}

// ifNil returns whenNil if v is nil, otherwise whenNotNil. Tiny helper
// for the log line above so we don't shadow the actual return value.
func ifNil[T any](v *T, whenNil, whenNotNil string) string {
	if v == nil {
		return whenNil
	}
	return whenNotNil
}

// getDatabaseStats retrieves database stats from cache or calculates fresh
// Uses 60-second cache since database stats change infrequently
func (s *Server) getDatabaseStats() *database.DatabaseStats {
	const cacheTTL = 60 * time.Second

	// Try cache first
	s.dbStatsCacheMutex.RLock()
	if s.dbStatsCache != nil && time.Since(s.dbStatsCachedAt) < cacheTTL {
		cached := s.dbStatsCache
		s.dbStatsCacheMutex.RUnlock()
		return cached
	}
	s.dbStatsCacheMutex.RUnlock()

	// Calculate fresh stats
	dbStats, err := s.db.GetDatabaseStats()
	if err != nil {
		log.Printf("Failed to get database stats: %v", err)
		return &database.DatabaseStats{} // Return empty stats on error
	}

	// Cache for next time
	s.dbStatsCacheMutex.Lock()
	s.dbStatsCache = dbStats
	s.dbStatsCachedAt = time.Now()
	s.dbStatsCacheMutex.Unlock()

	return dbStats
}

// HandleFiles serves the files page
func (s *Server) HandleConfig(w http.ResponseWriter, r *http.Request) {
	cpuCores := runtime.NumCPU()
	recommendedWorkers := cpuCores
	if recommendedWorkers > 16 {
		recommendedWorkers = 16 // Cap at 16 for diminishing returns
	}
	if recommendedWorkers < 4 {
		recommendedWorkers = 4 // Minimum of 4
	}

	data := ConfigData{
		Config:             s.config,
		Title:              "Configuration",
		Version:            s.version,
		CPUCores:           cpuCores,
		RecommendedWorkers: recommendedWorkers,
	}

	s.renderTemplate(w, "config.html", data)
}

// HandleStats serves the statistics page
func (s *Server) HandleStats(w http.ResponseWriter, r *http.Request) {
	statistics := s.getStats()
	if statistics == nil {
		log.Println("ERROR: Failed to calculate statistics page stats")
		respondError(w, http.StatusInternalServerError, "Failed to calculate statistics. Database may be unavailable", "stats_calculation_failed")
		return
	}

	// Get disk information if disk detector is configured
	var disks []*disk.DiskInfo
	hasDiskLocations := len(s.config.Disks) > 0

	if s.diskDetector != nil {
		// Refresh disk space information (uses 5-minute cache)
		if err := s.diskDetector.RefreshDiskSpace(); err != nil {
			log.Printf("Warning: Failed to refresh disk space: %v", err)
		}
		disks = s.diskDetector.GetAllDisks()
	}

	// Cross-disk and same-disk duplicates counts are now in cached stats
	var crossDiskDuplicates, sameDiskDuplicates int64
	if statistics.DuplicateStats != nil {
		crossDiskDuplicates = statistics.DuplicateStats.CrossDiskGroups
		sameDiskDuplicates = statistics.DuplicateStats.SameDiskGroups
	}

	data := StatsData{
		Stats:               statistics,
		Title:               "Statistics",
		Disks:               disks,
		CrossDiskDuplicates: crossDiskDuplicates,
		SameDiskDuplicates:  sameDiskDuplicates,
		HasDiskLocations:    hasDiskLocations,
		Version:             s.version,
	}

	s.renderTemplate(w, "stats.html", data)
}

// HardlinkGroup represents a group of hardlinked files
type HardlinkGroup struct {
	Key       string
	Files     []*database.File
	LinkCount int   // Number of linked files in the group
	Size      int64 // Space saved by hardlinks
}

// ScanDisplay represents a scan with additional computed fields for display
type ScanDisplay struct {
	*database.Scan
	ActualFileCount int // Actual count of files with this scan_id
}

// HandleHardlinks serves the hardlinks page with pagination, search, and sorting
func (s *Server) HandleHardlinks(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	page = ValidatePage(page)

	search := r.URL.Query().Get("search")
	orderBy := r.URL.Query().Get("order")
	if orderBy == "" {
		orderBy = "space_saved" // default sort
	}
	direction := r.URL.Query().Get("direction")
	if direction == "" {
		direction = "desc" // default direction
	}

	limit := constants.DefaultHardlinkGroupsPerPage
	offset := (page - 1) * limit

	// Get filtered and sorted groups from database
	groupsMap, total, err := s.db.GetHardlinkGroupsFiltered(search, orderBy, direction, limit, offset)
	if err != nil {
		log.Printf("ERROR: Failed to get hardlink groups: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to retrieve hardlink groups. Database error occurred", "database_error")
		return
	}

	// Convert map to slice for display
	groups := make([]HardlinkGroup, 0, len(groupsMap))
	for key, files := range groupsMap {
		if len(files) > 0 {
			// Use first file size as baseline
			baseSize := files[0].Size

			// Check for size inconsistencies (possible corruption)
			for _, f := range files[1:] {
				if f.Size != baseSize {
					log.Printf("WARNING: Hardlink group %s has files with different sizes (%d vs %d). This may indicate filesystem corruption",
						key, baseSize, f.Size)
					// Use minimum size for conservative calculation
					if f.Size < baseSize {
						baseSize = f.Size
					}
				}
			}

			groups = append(groups, HardlinkGroup{
				Key:       key,
				Files:     files,
				LinkCount: len(files),
				Size:      baseSize * int64(len(files)-1), // Space saved
			})
		}
	}

	data := HardlinksData{
		Groups:     groups,
		Total:      int64(total),
		Showing:    len(groups),
		Page:       int64(page),
		TotalPages: CalculateTotalPages(total, limit),
		Title:      "Hardlink Groups",
		Search:     search,
		OrderBy:    orderBy,
		Direction:  direction,
		Version:    s.version,
	}

	s.renderTemplate(w, "hardlinks.html", data)
}

// HandleScans serves the scan history page
func (s *Server) HandleScans(w http.ResponseWriter, r *http.Request) {
	// Trigger stale scan cleanup by calling GetCurrentScan
	// This will mark any stale running scans as interrupted
	_, _ = s.db.GetCurrentScan()

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	page = ValidatePage(page)

	limit := constants.DefaultScansPerPage
	offset := (page - 1) * limit

	scans, total, err := s.db.ListScans(limit, offset)
	if err != nil {
		log.Printf("ERROR: Failed to list scans: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to retrieve scan history. Database error occurred", "database_error")
		return
	}

	// Enhance scans with actual file counts
	// Prefer the FilesScanned field from the scan record (populated by our persistence logic)
	// Only query the database as a fallback for old scans that don't have it
	scanDisplays := make([]*ScanDisplay, 0, len(scans))
	for _, scan := range scans {
		actualCount := int(scan.FilesScanned)

		// Fallback: Query database if FilesScanned is 0 (old scans before our persistence fix)
		if actualCount == 0 {
			count, err := s.db.GetScanFileCount(scan.ID)
			if err != nil {
				log.Printf("WARNING: Failed to get file count for scan %d: %v", scan.ID, err)
			} else {
				actualCount = count
			}
		}

		scanDisplays = append(scanDisplays, &ScanDisplay{
			Scan:            scan,
			ActualFileCount: actualCount,
		})
	}

	data := ScansData{
		Scans:      scanDisplays,
		Total:      int64(total),
		Page:       int64(page),
		TotalPages: CalculateTotalPages(total, limit),
		Title:      "Scan History",
		Version:    s.version,
	}

	s.renderTemplate(w, "scans.html", data)
}

// HandleScanLogsPage serves the scan logs page
func (s *Server) HandleScanLogsPage(w http.ResponseWriter, r *http.Request) {
	// Get all scans for the filter dropdown
	scans, _, err := s.db.ListScans(100, 0) // Get recent 100 scans
	if err != nil {
		log.Printf("ERROR: Failed to list scans: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to retrieve scans", "database_error")
		return
	}

	// Get tab parameter (default to "scan")
	tab := r.URL.Query().Get("tab")
	if tab == "" {
		tab = "scan"
	}

	// Get scan_id parameter for deep linking
	var scanID *int64
	if scanIDStr := r.URL.Query().Get("scan_id"); scanIDStr != "" {
		if id, err := strconv.ParseInt(scanIDStr, 10, 64); err == nil {
			scanID = &id
		}
	}

	data := map[string]interface{}{
		"Title":   "Scan Logs",
		"Version": s.version,
		"Scans":   scans,
		"Tab":     tab,
		"ScanID":  scanID,
	}

	s.renderTemplate(w, "logs.html", data)
}

// HandleGetScanLogs serves the scan logs API endpoint with filtering and pagination
func (s *Server) HandleAdvanced(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	// Get database stats (uses 60-second cache)
	dbStats := s.getDatabaseStats()

	data := AdvancedData{
		Title:   "Advanced Settings",
		Stats:   dbStats,
		Version: s.version,
	}

	s.renderTemplate(w, "advanced.html", data)
}

// HandleAdminClearFiles handles clearing all or orphaned files
