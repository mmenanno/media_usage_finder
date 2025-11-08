package server

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/api"
	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/constants"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
	"github.com/mmenanno/media-usage-finder/internal/duplicates"
	"github.com/mmenanno/media-usage-finder/internal/scanner"
	"github.com/mmenanno/media-usage-finder/internal/stats"
)

// Server holds the application state
type Server struct {
	db                *database.DB
	config            *config.Config
	scanner           *scanner.Scanner
	hashScanner       *scanner.HashScanner          // Hash scanner for duplicate detection
	templates         map[string]*template.Template // Map of template name to parsed template
	statsCache        *stats.Cache
	dbStatsCache      *database.DatabaseStats // Database stats cache
	dbStatsCachedAt   time.Time               // When database stats were cached
	dbStatsCacheMutex sync.RWMutex            // Mutex for database stats cache
	templateFuncs     template.FuncMap        // Cached template functions
	version           string                  // Application version
	clientFactory     *api.ClientFactory      // Factory for creating service clients
	diskDetector      *disk.Detector          // Disk detector for cross-disk duplicate detection
	diskResolver      *disk.DeviceResolver    // Device resolver for friendly disk names in UI
}

// NewServer creates a new server instance
func NewServer(db *database.DB, cfg *config.Config, version string) *Server {
	cacheTTL := cfg.StatsCacheTTL
	if cacheTTL == 0 {
		cacheTTL = 30 * time.Second // Default fallback
	}

	srv := &Server{
		db:            db,
		config:        cfg,
		statsCache:    stats.NewCache(cacheTTL),
		version:       version,
		clientFactory: api.NewClientFactory(cfg),
	}

	// Initialize cached template functions
	srv.templateFuncs = srv.createTemplateFuncs()

	srv.scanner = scanner.NewScanner(db, cfg)

	// Initialize hash scanner if duplicate detection is enabled
	if cfg.DuplicateDetection.Enabled {
		srv.hashScanner = scanner.NewHashScanner(db, &cfg.DuplicateDetection)
		log.Printf("Hash scanner initialized with algorithm: %s", cfg.DuplicateDetection.HashAlgorithm)
	} else {
		log.Printf("Duplicate detection disabled in configuration")
	}

	// Initialize disk detector if disks are configured
	if len(cfg.Disks) > 0 {
		srv.diskDetector = disk.NewDetector(cfg.Disks)
		if err := srv.diskDetector.DetectDisks(); err != nil {
			log.Printf("Warning: Failed to detect disks: %v", err)
			log.Printf("Duplicate detection features will be limited without disk information")
		} else {
			log.Printf("Successfully detected %d disk(s)", srv.diskDetector.GetDiskCount())

			// Initialize disk resolver with detected disks for UI display
			srv.diskResolver = disk.NewDeviceResolver(srv.diskDetector.GetAllDisks())
		}
	} else {
		log.Printf("No disks configured - cross-disk duplicate detection disabled")
		log.Printf("To enable: configure 'disks' in config.yaml and mount disks in docker-compose.yml")
	}

	// Invalidate stats cache when scan completes
	srv.scanner.SetOnScanComplete(func() {
		srv.statsCache.Invalidate()
	})

	return srv
}

// LoadTemplates loads HTML templates
// Each page template is parsed separately to avoid block name collisions
func (s *Server) LoadTemplates(pattern string) error {
	s.templates = make(map[string]*template.Template)

	// Extract base directory from pattern (e.g., "web/templates/*.html" -> "web/templates")
	baseDir := "web/templates"
	if idx := strings.LastIndex(pattern, "/"); idx > 0 {
		baseDir = pattern[:idx]
	}

	// List of page templates that need to be loaded
	pages := []string{
		"dashboard.html",
		"files.html",
		"duplicates.html",
		"hardlinks.html",
		"scans.html",
		"logs.html",
		"stats.html",
		"config.html",
		"advanced.html",
	}

	layoutPath := baseDir + "/layout.html"

	// Parse each page template with layout.html to avoid block name collisions
	// This ensures each page gets its own "content" block without conflicts
	for _, page := range pages {
		fullPath := baseDir + "/" + page

		tmpl, err := template.New("").Funcs(s.templateFuncs).ParseFiles(
			layoutPath,
			fullPath,
		)
		if err != nil {
			return fmt.Errorf("failed to parse %s: %w", page, err)
		}

		s.templates[page] = tmpl
	}

	// Load partial templates (used for HTMX responses)
	partials := []string{
		"partials/validation-errors.html",
		"logs_table.html",
		"duplicates_table.html",
	}

	for _, partial := range partials {
		// Create template set without a named root to avoid conflicts
		// ParseFiles will add the file content as a named template
		tmpl, err := template.New("").Funcs(s.templateFuncs).ParseFiles(
			baseDir + "/" + partial,
		)
		if err != nil {
			return fmt.Errorf("failed to parse %s: %w", partial, err)
		}
		s.templates[partial] = tmpl
	}

	return nil
}

// HandleIndex serves the dashboard page
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

// getStats retrieves stats from cache or calculates fresh
func (s *Server) getStats() *stats.Stats {
	// Try cache first
	if cached := s.statsCache.Get(); cached != nil {
		return cached
	}

	// Calculate fresh stats
	calculator := stats.NewCalculator(s.db)
	statistics, err := calculator.Calculate()
	if err != nil {
		log.Printf("Failed to calculate stats: %v", err)
		return nil
	}

	// Cache for next time
	s.statsCache.Set(statistics)
	return statistics
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
func (s *Server) HandleFiles(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	page = ValidatePage(page)

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	limit = ValidateLimit(limit)

	offset := (page - 1) * limit

	orphanedOnly := r.URL.Query().Get("orphaned") == "true"
	hardlinksOnly := r.URL.Query().Get("hardlink") == "true"
	search := r.URL.Query().Get("search")
	orderBy := r.URL.Query().Get("order")
	direction := r.URL.Query().Get("direction")

	// Parse services filter (can be comma-separated or multiple params)
	var services []string
	if servicesParam := r.URL.Query().Get("services"); servicesParam != "" {
		for _, svc := range strings.Split(servicesParam, ",") {
			svc = strings.TrimSpace(strings.ToLower(svc))
			if svc != "" {
				services = append(services, svc)
			}
		}
	}
	// Support legacy single service parameter for backward compatibility
	if len(services) == 0 {
		if service := r.URL.Query().Get("service"); service != "" {
			services = []string{service}
		}
	}

	// Parse service filter mode: "any", "all", or "exact"
	serviceFilterMode := r.URL.Query().Get("service_filter_mode")
	if serviceFilterMode == "" {
		serviceFilterMode = "any" // default
	}

	// Parse extensions filter (can be comma-separated or multiple params)
	var extensions []string
	if extParam := r.URL.Query().Get("extensions"); extParam != "" {
		for _, ext := range strings.Split(extParam, ",") {
			ext = strings.TrimSpace(strings.ToLower(ext))
			if ext != "" {
				extensions = append(extensions, ext)
			}
		}
	}

	// Parse device names filter and convert to device IDs
	var deviceIDs []int64
	var deviceNames []string
	if devicesParam := r.URL.Query().Get("devices"); devicesParam != "" && s.diskDetector != nil {
		for _, devName := range strings.Split(devicesParam, ",") {
			devName = strings.TrimSpace(devName)
			if devName == "" {
				continue
			}
			deviceNames = append(deviceNames, devName)
			// Find the disk by name and get its device ID
			for _, disk := range s.diskDetector.GetAllDisks() {
				if disk.Name == devName {
					deviceIDs = append(deviceIDs, disk.DeviceID)
					break
				}
			}
		}
	}

	var files []*database.File
	var total int
	var err error

	if search != "" {
		files, total, err = s.db.SearchFiles(search, limit, offset)
	} else {
		files, total, err = s.db.ListFiles(orphanedOnly, services, serviceFilterMode, hardlinksOnly, extensions, deviceIDs, limit, offset, orderBy, direction)
	}

	if err != nil {
		log.Printf("ERROR: Failed to list files: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to list files. The database may be locked or experiencing issues", "database_error")
		return
	}

	// Batch load usage for all files (fixes N+1 query problem)
	fileIDs := make([]int64, len(files))
	for i, file := range files {
		fileIDs[i] = file.ID
	}

	usageMap, err := s.db.GetUsageByFileIDs(fileIDs)
	if err != nil {
		// Log error but continue with empty usage
		usageMap = make(map[int64][]*database.Usage)
	}

	// Batch load disk locations for all files (if disk scanning is configured)
	diskLocationsMap := make(map[int64][]*database.FileDiskLocation)
	if len(s.config.Disks) > 0 {
		diskLocationsMap, err = s.db.GetDiskLocationsByFileIDs(fileIDs)
		if err != nil {
			// Log error but continue with empty disk locations
			diskLocationsMap = make(map[int64][]*database.FileDiskLocation)
		}
	}

	filesWithUsage := make([]map[string]interface{}, 0, len(files))
	for _, file := range files {
		filesWithUsage = append(filesWithUsage, map[string]interface{}{
			"File":          file,
			"Usage":         usageMap[file.ID],
			"DiskLocations": diskLocationsMap[file.ID],
		})
	}

	// For backward compatibility, set Service if there's exactly one service
	legacyService := ""
	if len(services) == 1 {
		legacyService = services[0]
	}

	// Get available disks for filter dropdown
	var availableDisks []*disk.DiskInfo
	if s.diskDetector != nil {
		availableDisks = s.diskDetector.GetAllDisks()
	}

	data := FilesData{
		Files:             filesWithUsage,
		Total:             total,
		Page:              int64(page),
		Limit:             limit,
		TotalPages:        CalculateTotalPages(total, limit),
		Title:             "Files",
		Orphaned:          orphanedOnly,
		Hardlinks:         hardlinksOnly,
		Service:           legacyService,
		Services:          services,
		ServiceFilterMode: serviceFilterMode,
		Search:            search,
		OrderBy:           orderBy,
		Direction:         direction,
		Extensions:        extensions,
		Devices:           deviceNames,
		AvailableDisks:    availableDisks,
		DiskResolver:      s.diskResolver,
		HasDiskLocations:  len(s.config.Disks) > 0,
		Version:           s.version,
	}

	s.renderTemplate(w, "files.html", data)
}

// HandleGetFileExtensions returns a JSON list of distinct file extensions
func (s *Server) HandleGetFileExtensions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse filter parameters
	orphanedOnly := r.URL.Query().Get("orphaned") == "true"
	service := r.URL.Query().Get("service")

	extensions, err := s.db.GetFileExtensions(orphanedOnly, service)
	if err != nil {
		log.Printf("ERROR: Failed to get file extensions: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to get file extensions", "database_error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"extensions": extensions,
	})
}

// HandleConfig serves the configuration page
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
		Total:      total,
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
		Total:      total,
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

	data := map[string]interface{}{
		"Title":   "Scan Logs",
		"Version": s.version,
		"Scans":   scans,
	}

	s.renderTemplate(w, "logs.html", data)
}

// HandleGetScanLogs serves the scan logs API endpoint with filtering and pagination
func (s *Server) HandleGetScanLogs(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for filtering
	filters := database.LogFilters{
		Level:      r.URL.Query().Get("level"),
		Phase:      r.URL.Query().Get("phase"),
		SearchText: r.URL.Query().Get("search"),
		Limit:      100, // Default limit
		Offset:     0,
	}

	// Parse scan_id filter
	if scanIDStr := r.URL.Query().Get("scan_id"); scanIDStr != "" && scanIDStr != "all" {
		scanID, err := strconv.ParseInt(scanIDStr, 10, 64)
		if err == nil {
			filters.ScanID = &scanID
		}
	}

	// Parse pagination
	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		page, _ := strconv.Atoi(pageStr)
		if page > 0 {
			filters.Offset = (page - 1) * filters.Limit
		}
	}

	// Parse limit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		limit, _ := strconv.Atoi(limitStr)
		if limit > 0 && limit <= 1000 {
			filters.Limit = limit
		}
	}

	// Parse date range filters
	if startTimeStr := r.URL.Query().Get("start_time"); startTimeStr != "" {
		if startTime, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
			filters.StartTime = &startTime
		}
	}

	if endTimeStr := r.URL.Query().Get("end_time"); endTimeStr != "" {
		if endTime, err := time.Parse(time.RFC3339, endTimeStr); err == nil {
			filters.EndTime = &endTime
		}
	}

	// Get logs with filters
	logs, err := s.db.GetScanLogs(filters)
	if err != nil {
		log.Printf("ERROR: Failed to get scan logs: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to retrieve logs", "database_error")
		return
	}

	// Get total count for pagination
	total, err := s.db.GetScanLogCount(filters)
	if err != nil {
		log.Printf("ERROR: Failed to get log count: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to count logs", "database_error")
		return
	}

	// Calculate pagination info
	currentPage := (filters.Offset / filters.Limit) + 1
	totalPages := (total + filters.Limit - 1) / filters.Limit

	// Check if this is an HTMX request
	isHTMX := r.Header.Get("HX-Request") == "true"

	data := map[string]interface{}{
		"Logs":       logs,
		"Total":      total,
		"Page":       currentPage,
		"TotalPages": totalPages,
		"Filters":    filters,
		"IsHTMX":     isHTMX,
	}

	if isHTMX {
		// Return just the logs table fragment for HTMX updates
		tmplSet, ok := s.templates["logs_table.html"]
		if !ok {
			http.Error(w, "logs_table template not found", http.StatusInternalServerError)
			return
		}

		if err := tmplSet.ExecuteTemplate(w, "logs_table.html", data); err != nil {
			log.Printf("ERROR: Failed to execute template: %v", err)
			http.Error(w, "Failed to render logs", http.StatusInternalServerError)
			return
		}
	} else {
		// Return JSON for API requests
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	}
}

// HandleHealth serves the health check endpoint with detailed status
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	health := HealthResponse{
		Status:  "healthy",
		Version: s.version,
		Checks:  make(map[string]interface{}),
	}

	// Check database
	dbHealth := ServiceHealthCheck{Status: "ok"}
	if err := s.db.Ping(); err != nil {
		dbHealth.Status = "error"
		dbHealth.Error = err.Error()
		health.Status = "degraded"
	}
	health.Checks["database"] = dbHealth

	// Check if scan is running
	scanHealth := ScannerHealthCheck{Status: "ok", Running: false}
	if progress := s.scanner.GetProgress(); progress != nil {
		snapshot := progress.GetSnapshot()
		scanHealth.Running = true
		scanHealth.Progress = snapshot.PercentComplete
		scanHealth.Phase = snapshot.CurrentPhase
	}
	health.Checks["scanner"] = scanHealth

	// Optionally check external services (quick timeout)
	if r.URL.Query().Get("detailed") == "true" {
		health.Checks["services"] = s.checkExternalServices()
	}

	// Set status code based on health
	statusCode := http.StatusOK
	if health.Status == "degraded" {
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(health)
}

// checkExternalServices checks connectivity to external services concurrently
func (s *Server) checkExternalServices() map[string]interface{} {
	type serviceCheck struct {
		name   string
		result map[string]string
	}

	serviceNames := []string{"plex", "sonarr", "radarr", "qbittorrent", "stash"}
	results := make(chan serviceCheck, len(serviceNames))
	timeout := 2 * time.Second
	var wg sync.WaitGroup

	// Check all configured services concurrently
	for _, serviceName := range serviceNames {
		if !s.clientFactory.IsServiceConfigured(serviceName) {
			continue
		}

		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			client, err := s.clientFactory.CreateClient(name, timeout)
			if err != nil {
				results <- serviceCheck{name, map[string]string{"status": "error", "error": err.Error()}}
				return
			}

			if err := client.Test(); err != nil {
				results <- serviceCheck{name, map[string]string{"status": "error", "error": err.Error()}}
			} else {
				results <- serviceCheck{name, map[string]string{"status": "ok"}}
			}
		}(serviceName)
	}

	// Close results channel when all checks complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	services := make(map[string]interface{})
	for check := range results {
		services[check.name] = check.result
	}

	return services
}

// HandleStartScan starts a new scan
func (s *Server) HandleStartScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "A scan is already running", "scan_already_running")
		return
	}

	incremental := r.URL.Query().Get("incremental") == "true"

	// Create context with timeout for scan operation
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
		defer cancel()

		if err := s.scanner.Scan(ctx, incremental); err != nil {
			log.Printf("ERROR: Scan failed: %v", err)
			// Scan error will be recorded in database by scanner itself
		} else {
			log.Printf("INFO: Scan completed successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Scan started successfully")
	w.Header().Set("X-Toast-Type", "info")

	response := ScanStartResponse{
		Status:      "success",
		Message:     "Scan started",
		Incremental: incremental,
	}
	respondJSON(w, http.StatusOK, response)
}

// HandleCancelScan gracefully cancels the current scan
func (s *Server) HandleCancelScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if s.scanner.Cancel() {
		w.Header().Set("X-Toast-Message", "Scan cancelled successfully")
		w.Header().Set("X-Toast-Type", "info")
		respondSuccess(w, "Scan cancelled", nil)
	} else {
		respondError(w, http.StatusConflict, "No scan is currently running", "no_scan_running")
	}
}

// HandleForceStopScan immediately terminates the current scan
func (s *Server) HandleForceStopScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if s.scanner.ForceStop() {
		w.Header().Set("X-Toast-Message", "Scan force stopped")
		w.Header().Set("X-Toast-Type", "warning")
		respondSuccess(w, "Scan force stopped", nil)
	} else {
		respondError(w, http.StatusConflict, "No scan is currently running", "no_scan_running")
	}
}

// HandleResumeScan resumes an interrupted scan from where it left off
func (s *Server) HandleResumeScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "A scan is already running", "scan_already_running")
		return
	}

	// Check if there's an interrupted scan to resume
	interruptedScan, err := s.db.GetLastInterruptedScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check for interrupted scans", "check_failed")
		return
	}

	if interruptedScan == nil {
		respondError(w, http.StatusNotFound, "No interrupted scan found to resume", "no_interrupted_scan")
		return
	}

	// Run resume scan in background
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
		defer cancel()

		if err := s.scanner.ResumeScan(ctx); err != nil {
			log.Printf("ERROR: Resume scan failed: %v", err)
		} else {
			log.Printf("INFO: Resume scan completed successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Resuming scan from checkpoint...")
	w.Header().Set("X-Toast-Type", "info")

	response := ScanStartResponse{
		Status:  "success",
		Message: fmt.Sprintf("Resuming scan #%d", interruptedScan.ID),
	}
	respondJSON(w, http.StatusOK, response)
}

// HandleUpdateAllServices manually updates all service usage information
func (s *Server) HandleUpdateAllServices(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "Cannot update services while a scan is running", "scan_running")
		return
	}

	// Run service updates in background
	go func() {
		if err := s.scanner.UpdateAllServices(); err != nil {
			log.Printf("ERROR: Failed to update all services: %v", err)
		} else {
			log.Printf("INFO: All services updated successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Updating all services...")
	w.Header().Set("X-Toast-Type", "info")

	respondSuccess(w, "Service update started", nil)
}

// HandleUpdateSingleService manually updates a specific service's usage information
func (s *Server) HandleUpdateSingleService(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Get service name from query parameter
	serviceName := r.URL.Query().Get("service")
	if serviceName == "" {
		respondError(w, http.StatusBadRequest, "Service name is required", "missing_service")
		return
	}

	// Validate service name
	validServices := map[string]bool{
		"plex":        true,
		"sonarr":      true,
		"radarr":      true,
		"qbittorrent": true,
		"stash":       true,
	}
	if !validServices[serviceName] {
		respondError(w, http.StatusBadRequest, "Invalid service name", "invalid_service")
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "Cannot update service while a scan is running", "scan_running")
		return
	}

	// Run service update in background
	go func() {
		if err := s.scanner.UpdateSingleService(serviceName); err != nil {
			log.Printf("ERROR: Failed to update %s: %v", serviceName, err)
		} else {
			log.Printf("INFO: %s updated successfully", serviceName)
		}
	}()

	w.Header().Set("X-Toast-Message", fmt.Sprintf("Updating %s...", serviceName))
	w.Header().Set("X-Toast-Type", "info")

	respondSuccess(w, fmt.Sprintf("%s update started", serviceName), nil)
}

// HandleRecalculateOrphaned manually recalculates which files are orphaned
func (s *Server) HandleRecalculateOrphaned(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "Cannot recalculate while a scan is running", "scan_running")
		return
	}

	// Run recalculation in background
	go func() {
		if err := s.scanner.RecalculateOrphanedStatus(); err != nil {
			log.Printf("ERROR: Failed to recalculate orphaned status: %v", err)
		} else {
			log.Printf("INFO: Orphaned status recalculated successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Recalculating orphaned status...")
	w.Header().Set("X-Toast-Type", "info")

	respondSuccess(w, "Recalculation started", nil)
}

// HandleCleanupScan runs a manual cleanup scan to remove database entries for missing files
func (s *Server) HandleCleanupScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "Cannot start cleanup while a scan is running", "scan_running")
		return
	}

	// Run cleanup scan in background
	go func() {
		if err := s.scanner.RunCleanupScan(); err != nil {
			log.Printf("ERROR: Failed to run cleanup scan: %v", err)
		} else {
			log.Printf("INFO: Cleanup scan completed successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Starting cleanup scan...")
	w.Header().Set("X-Toast-Type", "info")

	respondSuccess(w, "Cleanup scan started", nil)
}

// HandleScanProgress returns the current scan progress
func (s *Server) HandleScanProgress(w http.ResponseWriter, r *http.Request) {
	progress := s.scanner.GetProgress()

	response := ScanProgressResponse{Running: false}

	if progress != nil {
		snapshot := progress.GetSnapshot()
		response.Running = true
		response.TotalFiles = snapshot.TotalFiles
		response.ProcessedFiles = snapshot.ProcessedFiles
		response.PercentComplete = snapshot.PercentComplete
		response.CurrentPhase = snapshot.CurrentPhase
		response.ETA = stats.FormatDuration(snapshot.ETA)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleScanProgressHTML returns HTML for scan progress (HTMX endpoint)
func (s *Server) HandleScanProgressHTML(w http.ResponseWriter, r *http.Request) {
	// Check for regular scan progress first, then disk scan progress
	progress := s.scanner.GetProgress()
	if progress == nil {
		progress = s.scanner.GetDiskScanProgress()
	}

	if progress == nil {
		w.Write([]byte(`<div class="text-gray-400">No scan running</div>`))
		return
	}

	snapshot := progress.GetSnapshot()

	// Calculate files per second
	elapsed := time.Since(snapshot.StartTime)
	var filesPerSec float64
	if elapsed.Seconds() > 0 && snapshot.ProcessedFiles > 0 {
		filesPerSec = float64(snapshot.ProcessedFiles) / elapsed.Seconds()
	}

	// Check if scan is completed - show final summary without polling
	if snapshot.CurrentPhase == "Completed" {
		completedHTML := fmt.Sprintf(`
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<div class="flex items-center text-sm text-gray-300">
					<svg class="w-5 h-5 inline-block mr-2 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
					<span class="font-medium">Completed</span>
				</div>
				<span class="text-lg font-bold text-green-400">100.0%%</span>
			</div>

			<div class="w-full bg-gray-700 rounded-full h-3 overflow-hidden">
				<div class="bg-gradient-to-r from-green-500 to-green-600 h-3 rounded-full shadow-lg" style="width: 100%%"></div>
			</div>

			<div class="grid grid-cols-2 gap-4 text-sm">
				<div>
					<div class="text-gray-500 text-xs">Files Processed</div>
					<div class="text-gray-200 font-medium">%d</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">Average Speed</div>
					<div class="text-gray-200 font-medium">%.1f files/sec</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">Total Time</div>
					<div class="text-gray-200 font-medium">%s</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">Status</div>
					<div class="text-green-400 font-medium">✓ Complete</div>
				</div>
			</div>
		</div>
		`,
			snapshot.ProcessedFiles,
			filesPerSec,
			stats.FormatDuration(elapsed),
		)

		// Set HX-Trigger header to stop polling
		w.Header().Set("HX-Trigger", "scanCompleted")
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(completedHTML))
		return
	}

	// Phase icons
	phaseIcons := map[string]string{
		"Initializing":             `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>`,
		"Counting files":           `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 20l4-16m2 16l4-16M6 9h14M4 15h14"></path></svg>`,
		"Scanning filesystem":      `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path></svg>`,
		"Loading File Cache":       `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"></path></svg>`,
		"Checking Plex":            `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 4v16M17 4v16M3 8h4m10 0h4M3 12h18M3 16h4m10 0h4M4 20h16a1 1 0 001-1V5a1 1 0 00-1-1H4a1 1 0 00-1 1v14a1 1 0 001 1z"></path></svg>`,
		"Checking Sonarr":          `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"></path></svg>`,
		"Checking Radarr":          `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 4v16M17 4v16M3 8h4m10 0h4M3 12h18M3 16h4m10 0h4M4 20h16a1 1 0 001-1V5a1 1 0 00-1-1H4a1 1 0 00-1 1v14a1 1 0 001 1z"></path></svg>`,
		"Checking qBittorrent":     `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"></path></svg>`,
		"Checking Stash":           `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 19a2 2 0 01-2-2V7a2 2 0 012-2h4l2 2h4a2 2 0 012 2v1M5 19h14a2 2 0 002-2v-5a2 2 0 00-2-2H9a2 2 0 00-2 2v5a2 2 0 01-2 2z"></path></svg>`,
		"Updating orphaned status": `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>`,
		"Completed":                `<svg class="w-5 h-5 inline-block mr-2 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>`,
	}

	icon := phaseIcons[snapshot.CurrentPhase]
	if icon == "" {
		// Add disk scanning phase icons (match any phase starting with "Scanning Disk")
		if strings.HasPrefix(snapshot.CurrentPhase, "Scanning Disk") {
			icon = `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"></path></svg>`
		} else {
			icon = phaseIcons["Initializing"]
		}
	}

	// Check if we're in a service update phase (starts with "Checking")
	// Exclude "Updating orphaned status" which is a post-service phase
	isServicePhase := strings.HasPrefix(snapshot.CurrentPhase, "Checking ")

	var html string
	if isServicePhase {
		// Service update phase: show progress with service count
		// Calculate service progress percentage
		var serviceProgressPercent float64
		var serviceProgressDisplay string
		if snapshot.TotalServices > 0 && snapshot.CurrentService > 0 {
			serviceProgressPercent = (float64(snapshot.CurrentService) / float64(snapshot.TotalServices)) * 100
			serviceProgressDisplay = fmt.Sprintf("Service %d of %d", snapshot.CurrentService, snapshot.TotalServices)
		} else {
			// Fallback if service tracking not available
			serviceProgressPercent = 100 // Show full bar with pulse
			serviceProgressDisplay = "Querying API..."
		}

		html = fmt.Sprintf(`
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<div class="flex items-center text-sm text-gray-300">
					%s
					<span class="font-medium">%s</span>
				</div>
				<span class="text-lg font-bold text-purple-400">%s</span>
			</div>

			<div class="w-full bg-gray-700 rounded-full h-3 overflow-hidden">
				<div class="bg-gradient-to-r from-purple-500 via-purple-400 to-purple-500 h-3 rounded-full shadow-lg transition-all duration-300" style="width: %.1f%%; animation: shimmer 2s ease-in-out infinite; background-size: 200%% 100%%"></div>
			</div>
			<style>
				@keyframes shimmer {
					0%% { background-position: -200%% 0; }
					100%% { background-position: 200%% 0; }
				}
			</style>

			<div class="grid grid-cols-2 gap-4 text-sm">
				<div>
					<div class="text-gray-500 text-xs">Filesystem Scan</div>
					<div class="text-gray-200 font-medium">%d files scanned</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">Service Progress</div>
					<div class="text-gray-200 font-medium">%s</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">Elapsed Time</div>
					<div class="text-gray-200 font-medium">%s</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">Status</div>
					<div class="text-gray-200 font-medium">In Progress</div>
				</div>
			</div>

			<div class="flex justify-end space-x-2 pt-2 border-t border-gray-700">
				<button
					hx-post="/api/scan/cancel"
					hx-swap="none"
					hx-confirm="Cancel the current scan gracefully? The current service update will complete before stopping."
					class="px-3 py-1 bg-yellow-600 hover:bg-yellow-700 rounded text-sm transition">
					Cancel Scan
				</button>
				<button
					hx-post="/api/scan/force-stop"
					hx-swap="none"
					hx-confirm="Force stop the scan immediately? This may leave the database in an inconsistent state."
					class="px-3 py-1 bg-red-600 hover:bg-red-700 rounded text-sm transition">
					Force Stop
				</button>
			</div>
		</div>
	`,
			icon,
			snapshot.CurrentPhase,
			serviceProgressDisplay,
			serviceProgressPercent,
			snapshot.ProcessedFiles,
			serviceProgressDisplay,
			stats.FormatDuration(elapsed),
		)
	} else {
		// Normal filesystem scanning phase: show percentage and file progress
		// Handle three cases:
		// 1. First scan (no estimate): Show animated gradient
		// 2. Estimated progress: Show ~X%
		// 3. Normal progress: Show X%

		var percentDisplay string
		var progressBarHTML string

		if snapshot.TotalFiles == 0 {
			// First scan ever - no estimate available
			percentDisplay = `<span class="text-sm text-gray-400">First scan</span>`
			progressBarHTML = `<div class="bg-gradient-to-r from-blue-500 via-purple-500 to-blue-500 h-3 rounded-full animate-pulse bg-[length:200%_100%]" style="animation: gradient 3s ease infinite;"></div>
			<style>
				@keyframes gradient {
					0% { background-position: 0% 50%; }
					50% { background-position: 100% 50%; }
					100% { background-position: 0% 50%; }
				}
			</style>`
		} else if snapshot.IsEstimated {
			// Using estimate from previous scan
			percentDisplay = fmt.Sprintf(`<span class="text-lg font-bold text-blue-400">~%.1f%%</span>`, snapshot.PercentComplete)
			progressBarHTML = fmt.Sprintf(`<div class="bg-gradient-to-r from-blue-500 to-blue-600 h-3 rounded-full transition-all duration-300 shadow-lg" style="width: %.1f%%"></div>`, snapshot.PercentComplete)
		} else {
			// Normal progress with actual total
			percentDisplay = fmt.Sprintf(`<span class="text-lg font-bold text-blue-400">%.1f%%</span>`, snapshot.PercentComplete)
			progressBarHTML = fmt.Sprintf(`<div class="bg-gradient-to-r from-blue-500 to-blue-600 h-3 rounded-full transition-all duration-300 shadow-lg" style="width: %.1f%%"></div>`, snapshot.PercentComplete)
		}

		// Format total files display
		var totalFilesDisplay string
		if snapshot.TotalFiles == 0 {
			totalFilesDisplay = fmt.Sprintf("%d", snapshot.ProcessedFiles)
		} else if snapshot.IsEstimated {
			totalFilesDisplay = fmt.Sprintf("%d / ~%d", snapshot.ProcessedFiles, snapshot.TotalFiles)
		} else {
			totalFilesDisplay = fmt.Sprintf("%d / %d", snapshot.ProcessedFiles, snapshot.TotalFiles)
		}

		// Format ETA display
		var etaDisplay string
		if snapshot.TotalFiles == 0 {
			etaDisplay = "calculating..."
		} else {
			etaDisplay = stats.FormatDuration(snapshot.ETA)
		}

		html = fmt.Sprintf(`
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<div class="flex items-center text-sm text-gray-300">
					%s
					<span class="font-medium">%s</span>
				</div>
				%s
			</div>

			<div class="w-full bg-gray-700 rounded-full h-3 overflow-hidden">
				%s
			</div>

			<div class="grid grid-cols-2 gap-4 text-sm">
				<div>
					<div class="text-gray-500 text-xs">Files Processed</div>
					<div class="text-gray-200 font-medium">%s</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">Speed</div>
					<div class="text-gray-200 font-medium">%.1f files/sec</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">Elapsed Time</div>
					<div class="text-gray-200 font-medium">%s</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">ETA</div>
					<div class="text-gray-200 font-medium">%s</div>
				</div>
			</div>

			<div class="flex justify-end space-x-2 pt-2 border-t border-gray-700">
				<button
					hx-post="/api/scan/cancel"
					hx-swap="none"
					hx-confirm="Cancel the current scan gracefully? The scan will finish processing the current file before stopping."
					class="px-3 py-1 bg-yellow-600 hover:bg-yellow-700 rounded text-sm transition">
					Cancel Scan
				</button>
				<button
					hx-post="/api/scan/force-stop"
					hx-swap="none"
					hx-confirm="Force stop the scan immediately? This may leave the database in an inconsistent state."
					class="px-3 py-1 bg-red-600 hover:bg-red-700 rounded text-sm transition">
					Force Stop
				</button>
			</div>
		</div>
	`,
			icon,
			snapshot.CurrentPhase,
			percentDisplay,
			progressBarHTML,
			totalFilesDisplay,
			filesPerSec,
			stats.FormatDuration(elapsed),
			etaDisplay,
		)
	}

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
}

// HandleScanLogs streams scan logs via SSE
func (s *Server) HandleScanLogs(w http.ResponseWriter, r *http.Request) {
	// Check if scanner exists
	if s.scanner == nil {
		log.Printf("ERROR: Scanner not initialized for SSE logs endpoint")
		http.Error(w, "Scanner not initialized", http.StatusInternalServerError)
		return
	}

	// Check if streaming is supported BEFORE writing any headers
	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Printf("ERROR: Streaming not supported for SSE logs endpoint")
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	// Now it's safe to write headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable buffering for nginx

	// Send initial connection message
	fmt.Fprintf(w, "data: <div class=\"text-gray-400\">Connected to log stream</div>\n\n")
	flusher.Flush()

	progress := s.scanner.GetProgress()
	if progress == nil {
		// No scan running - keep connection open but send status updates
		fmt.Fprintf(w, "data: <div class=\"text-gray-500\">No scan currently running</div>\n\n")
		flusher.Flush()

		// Keep connection alive with periodic pings
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				fmt.Fprintf(w, ": keep-alive\n\n")
				flusher.Flush()

				// Check if a scan has started
				if s.scanner.GetProgress() != nil {
					fmt.Fprintf(w, "data: <div class=\"text-green-400\">Scan started, reconnect to see logs</div>\n\n")
					flusher.Flush()
					return
				}
			}
		}
	}

	// Subscribe to log messages
	logChan := progress.Subscribe()
	if logChan == nil {
		fmt.Fprintf(w, "data: <div class=\"text-red-400\">Failed to subscribe to scan logs</div>\n\n")
		flusher.Flush()
		return
	}
	defer progress.Unsubscribe(logChan)

	// Create ticker for keep-alive heartbeat (every 30 seconds to keep connection alive)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Stream log messages
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			// Send keep-alive comment (ignored by SSE clients)
			fmt.Fprintf(w, ": keep-alive\n\n")
			flusher.Flush()
		case msg, ok := <-logChan:
			if !ok {
				// Channel closed, scan finished
				fmt.Fprintf(w, "data: <div class=\"text-green-400\">Scan completed</div>\n\n")
				flusher.Flush()
				return
			}
			fmt.Fprintf(w, "data: <div class=\"text-gray-300\">%s</div>\n\n", msg)
			flusher.Flush()
		}
	}
}

// HandleSaveConfig saves configuration
func (s *Server) HandleSaveConfig(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Parse form data
	if err := r.ParseForm(); err != nil {
		respondError(w, http.StatusBadRequest, "Failed to parse form data", "parse_error")
		return
	}

	// Update all config fields from form
	s.config.DatabasePath = r.FormValue("database_path")

	if workers := r.FormValue("scan_workers"); workers != "" {
		if w, err := strconv.Atoi(workers); err == nil && w > 0 && w <= 100 {
			s.config.ScanWorkers = w
		}
	}

	if apiTimeout := r.FormValue("api_timeout"); apiTimeout != "" {
		if timeout, err := time.ParseDuration(apiTimeout); err == nil && timeout >= time.Second {
			s.config.APITimeout = timeout
		}
	}

	if retentionDays := r.FormValue("scan_log_retention_days"); retentionDays != "" {
		if days, err := strconv.Atoi(retentionDays); err == nil && days >= -1 {
			s.config.ScanLogRetentionDays = days
		}
	}

	// Note: HTML checkboxes send "on" when checked, or nothing when unchecked
	s.config.AutoCleanupDeletedFiles = r.FormValue("auto_cleanup_deleted_files") != ""

	if cacheSize := r.FormValue("db_cache_size"); cacheSize != "" {
		if size, err := strconv.Atoi(cacheSize); err == nil && size > 0 {
			s.config.DBCacheSize = size
		}
	}

	// Collect all validation errors before updating config
	var validationErrors []string

	// Validate Plex config
	plexURL := r.FormValue("plex_url")
	if err := ValidateURL(plexURL); err != nil {
		validationErrors = append(validationErrors, fmt.Sprintf("Plex URL: %v", err))
	}

	// Validate Sonarr config
	sonarrURL := r.FormValue("sonarr_url")
	if err := ValidateURL(sonarrURL); err != nil {
		validationErrors = append(validationErrors, fmt.Sprintf("Sonarr URL: %v", err))
	}
	sonarrAPIKey := r.FormValue("sonarr_api_key")
	if err := ValidateAPIKey(sonarrAPIKey); err != nil {
		validationErrors = append(validationErrors, fmt.Sprintf("Sonarr API key: %v", err))
	}

	// Validate Radarr config
	radarrURL := r.FormValue("radarr_url")
	if err := ValidateURL(radarrURL); err != nil {
		validationErrors = append(validationErrors, fmt.Sprintf("Radarr URL: %v", err))
	}
	radarrAPIKey := r.FormValue("radarr_api_key")
	if err := ValidateAPIKey(radarrAPIKey); err != nil {
		validationErrors = append(validationErrors, fmt.Sprintf("Radarr API key: %v", err))
	}

	// Validate qBittorrent config
	qbURL := r.FormValue("qbittorrent_url")
	if err := ValidateURL(qbURL); err != nil {
		validationErrors = append(validationErrors, fmt.Sprintf("qBittorrent URL: %v", err))
	}
	qbProxyURL := r.FormValue("qbittorrent_qui_proxy_url")
	if err := ValidateURL(qbProxyURL); err != nil {
		validationErrors = append(validationErrors, fmt.Sprintf("qBittorrent proxy URL: %v", err))
	}

	// Validate Stash config
	stashURL := r.FormValue("stash_url")
	if err := ValidateURL(stashURL); err != nil {
		validationErrors = append(validationErrors, fmt.Sprintf("Stash URL: %v", err))
	}
	stashAPIKey := r.FormValue("stash_api_key")
	if err := ValidateAPIKey(stashAPIKey); err != nil {
		validationErrors = append(validationErrors, fmt.Sprintf("Stash API key: %v", err))
	}

	// If there are validation errors, show them in the error panel
	if len(validationErrors) > 0 {
		s.renderValidationErrors(w, "Configuration Validation Failed", validationErrors)
		return
	}

	// All validations passed, update config
	s.config.Services.Plex.URL = plexURL
	s.config.Services.Plex.Token = r.FormValue("plex_token")
	// Parse selected Plex libraries (multiple checkbox values)
	s.config.Services.Plex.Libraries = r.Form["plex_libraries"]

	s.config.Services.Sonarr.URL = sonarrURL
	s.config.Services.Sonarr.APIKey = sonarrAPIKey

	s.config.Services.Radarr.URL = radarrURL
	s.config.Services.Radarr.APIKey = radarrAPIKey

	s.config.Services.QBittorrent.URL = qbURL
	s.config.Services.QBittorrent.Username = r.FormValue("qbittorrent_username")
	s.config.Services.QBittorrent.Password = r.FormValue("qbittorrent_password")
	s.config.Services.QBittorrent.QuiProxyURL = qbProxyURL

	s.config.Services.Stash.URL = stashURL
	s.config.Services.Stash.APIKey = stashAPIKey

	// Parse scan paths (one per line)
	if scanPathsStr := r.FormValue("scan_paths"); scanPathsStr != "" {
		lines := strings.Split(scanPathsStr, "\n")
		s.config.ScanPaths = []string{}
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" {
				s.config.ScanPaths = append(s.config.ScanPaths, line)
			}
		}
	}

	// Parse local path mappings (format: service=local, one per line)
	if localMappingsStr := r.FormValue("local_path_mappings"); localMappingsStr != "" {
		lines := strings.Split(localMappingsStr, "\n")
		s.config.LocalPathMappings = []config.PathMapping{}
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				s.config.LocalPathMappings = append(s.config.LocalPathMappings, config.PathMapping{
					Service: strings.TrimSpace(parts[0]),
					Local:   strings.TrimSpace(parts[1]),
				})
			}
		}
	}

	// Parse service path mappings (format: servicename:service=local, one per line)
	if serviceMappingsStr := r.FormValue("service_path_mappings"); serviceMappingsStr != "" {
		lines := strings.Split(serviceMappingsStr, "\n")
		s.config.ServicePathMappings = make(map[string][]config.PathMapping)
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			// Split service:path=host
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}
			service := strings.TrimSpace(parts[0])
			pathParts := strings.SplitN(parts[1], "=", 2)
			if len(pathParts) != 2 {
				continue
			}
			mapping := config.PathMapping{
				Service: strings.TrimSpace(pathParts[0]),
				Local:   strings.TrimSpace(pathParts[1]),
			}
			s.config.ServicePathMappings[service] = append(s.config.ServicePathMappings[service], mapping)
		}
	}

	// Parse duplicate detection settings
	// Note: HTML checkboxes send "on" when checked, or nothing when unchecked
	s.config.DuplicateDetection.Enabled = r.FormValue("duplicate_detection_enabled") != ""
	s.config.DuplicateDetection.HashAlgorithm = r.FormValue("hash_algorithm")
	if s.config.DuplicateDetection.HashAlgorithm == "" {
		s.config.DuplicateDetection.HashAlgorithm = "sha256"
	}

	hashWorkers := r.FormValue("hash_workers")
	if hashWorkers != "" {
		if workers, err := strconv.Atoi(hashWorkers); err == nil && workers > 0 {
			s.config.DuplicateDetection.HashWorkers = workers
		}
	}

	s.config.DuplicateDetection.HashMode = r.FormValue("hash_mode")
	if s.config.DuplicateDetection.HashMode == "" {
		s.config.DuplicateDetection.HashMode = "quick_manual"
	}

	minFileSize := r.FormValue("min_file_size")
	if minFileSize != "" {
		if size, err := strconv.ParseInt(minFileSize, 10, 64); err == nil && size >= 0 {
			s.config.DuplicateDetection.MinFileSize = size * 1024 * 1024 // Convert MB to bytes
		}
	}

	maxFileSize := r.FormValue("max_file_size")
	if maxFileSize != "" {
		if size, err := strconv.ParseInt(maxFileSize, 10, 64); err == nil && size >= 0 {
			s.config.DuplicateDetection.MaxFileSize = size * 1024 * 1024 // Convert MB to bytes
		}
	}

	maxHashRate := r.FormValue("max_hash_rate")
	if maxHashRate != "" {
		if rate, err := strconv.Atoi(maxHashRate); err == nil && rate >= 0 {
			s.config.DuplicateDetection.MaxHashRateMB = rate
		}
	}

	hashBufferSize := r.FormValue("hash_buffer_size")
	if hashBufferSize != "" {
		// Validate buffer size using disk.ParseSize()
		if size, err := disk.ParseSize(hashBufferSize); err == nil {
			// Validate range: 512KB - 16MB
			if size >= 524288 && size <= 16777216 {
				s.config.DuplicateDetection.HashBufferSize = hashBufferSize
			}
		}
	}

	// Parse consolidation settings
	// Note: HTML checkboxes send "on" when checked, or nothing when unchecked
	s.config.DuplicateConsolidation.Enabled = r.FormValue("consolidation_enabled") != ""
	s.config.DuplicateConsolidation.DryRun = r.FormValue("dry_run") != ""
	s.config.DuplicateConsolidation.RequireManualApproval = r.FormValue("require_manual_approval") != ""
	s.config.DuplicateConsolidation.VerifyBeforeDelete = r.FormValue("verify_before_delete") != ""
	s.config.DuplicateConsolidation.Strategy = r.FormValue("consolidation_strategy")
	if s.config.DuplicateConsolidation.Strategy == "" {
		s.config.DuplicateConsolidation.Strategy = "least_full_disk"
	}

	// Parse disk configuration
	var disks []config.DiskConfig
	diskIndex := 0
	for {
		diskName := r.FormValue(fmt.Sprintf("disk_name_%d", diskIndex))
		diskMount := r.FormValue(fmt.Sprintf("disk_mount_%d", diskIndex))

		if diskName == "" && diskMount == "" {
			break
		}

		// Validate
		if diskName == "" {
			validationErrors = append(validationErrors, fmt.Sprintf("Disk %d: Name is required", diskIndex+1))
		}
		if diskMount == "" {
			validationErrors = append(validationErrors, fmt.Sprintf("Disk %d: Mount path is required", diskIndex+1))
		} else {
			if !filepath.IsAbs(diskMount) {
				validationErrors = append(validationErrors, fmt.Sprintf("Disk %d: Mount path must be absolute", diskIndex+1))
			}
			if strings.Contains(diskMount, "..") {
				validationErrors = append(validationErrors, fmt.Sprintf("Disk %d: Mount path cannot contain '..'", diskIndex+1))
			}
		}

		if diskName != "" && diskMount != "" {
			disks = append(disks, config.DiskConfig{
				Name:      strings.TrimSpace(diskName),
				MountPath: strings.TrimSpace(diskMount),
			})
		}
		diskIndex++
	}

	// Check for duplicate mount paths
	mountPaths := make(map[string]bool)
	for i, disk := range disks {
		if mountPaths[disk.MountPath] {
			validationErrors = append(validationErrors, fmt.Sprintf("Disk %d: Duplicate mount path '%s'", i+1, disk.MountPath))
		}
		mountPaths[disk.MountPath] = true
	}

	// Return early if there are validation errors
	if len(validationErrors) > 0 {
		s.renderValidationErrors(w, "Configuration Validation Failed", validationErrors)
		return
	}

	// Assign parsed disks
	s.config.Disks = disks

	// Clear path cache after updating mappings
	s.config.ClearPathCache()

	// Validate config before saving
	if err := s.config.Validate(); err != nil {
		s.renderValidationErrors(w, "Configuration Validation Failed", []string{err.Error()})
		return
	}

	// Save config to file
	if err := s.config.Save("/appdata/config/config.yaml"); err != nil {
		s.renderValidationErrors(w, "Failed to Save Configuration", []string{err.Error()})
		return
	}

	// Reinitialize hash scanner if duplicate detection was enabled/disabled
	if s.config.DuplicateDetection.Enabled && s.hashScanner == nil {
		// Duplicate detection was enabled - initialize hash scanner
		s.hashScanner = scanner.NewHashScanner(s.db, &s.config.DuplicateDetection)
		log.Printf("Hash scanner initialized with algorithm: %s", s.config.DuplicateDetection.HashAlgorithm)
	} else if !s.config.DuplicateDetection.Enabled && s.hashScanner != nil {
		// Duplicate detection was disabled - clear hash scanner
		s.hashScanner = nil
		log.Printf("Hash scanner disabled")
	}

	// Success - show toast and clear error panel
	w.Header().Set("X-Toast-Message", "Configuration saved successfully")
	w.Header().Set("X-Toast-Type", "success")
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	// Clear and hide the validation-errors div
	w.Write([]byte(`<script>document.getElementById('validation-errors').classList.add('hidden');</script>`))
}

// HandleDetectDisks tests disk detection with provided configuration
func (s *Server) HandleDetectDisks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Parse disk configs from form
	var diskConfigs []config.DiskConfig
	diskIndex := 0
	for {
		diskName := r.FormValue(fmt.Sprintf("disk_name_%d", diskIndex))
		diskMount := r.FormValue(fmt.Sprintf("disk_mount_%d", diskIndex))

		if diskName == "" && diskMount == "" {
			break
		}

		if diskName != "" && diskMount != "" {
			diskConfigs = append(diskConfigs, config.DiskConfig{
				Name:      strings.TrimSpace(diskName),
				MountPath: strings.TrimSpace(diskMount),
			})
		}
		diskIndex++
	}

	if len(diskConfigs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		http.Error(w, `{"error":"No disk configurations provided"}`, http.StatusBadRequest)
		return
	}

	// Create detector and test
	detector := disk.NewDetector(diskConfigs)
	if err := detector.DetectDisks(); err != nil {
		w.Header().Set("Content-Type", "application/json")
		http.Error(w, fmt.Sprintf(`{"error":"Disk detection failed: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// Build response
	disks := detector.GetAllDisks()
	type diskResponse struct {
		Name        string  `json:"name"`
		MountPath   string  `json:"mount_path"`
		TotalBytes  int64   `json:"total_bytes"`
		UsedBytes   int64   `json:"used_bytes"`
		FreeBytes   int64   `json:"free_bytes"`
		UsedPercent float64 `json:"used_percent"`
	}

	response := struct {
		Count int            `json:"count"`
		Disks []diskResponse `json:"disks"`
	}{
		Count: detector.GetDiskCount(),
		Disks: make([]diskResponse, 0, len(disks)),
	}

	for _, d := range disks {
		response.Disks = append(response.Disks, diskResponse{
			Name:        d.Name,
			MountPath:   d.MountPath,
			TotalBytes:  d.TotalBytes,
			UsedBytes:   d.UsedBytes,
			FreeBytes:   d.FreeBytes,
			UsedPercent: d.UsedPercent,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleScanDiskLocations starts a disk location scan to populate file_disk_locations table
func (s *Server) HandleScanDiskLocations(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Check if disks are configured
	if len(s.config.Disks) == 0 {
		respondError(w, http.StatusBadRequest, "No disks configured - disk scanning not available", "no_disks_configured")
		return
	}

	// Check if disk detector is available
	if s.diskDetector == nil {
		respondError(w, http.StatusInternalServerError, "Disk detector not initialized", "detector_unavailable")
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "Cannot run disk scan while a main scan is running", "scan_running")
		return
	}

	// Run disk scan in background
	go func() {
		log.Println("Starting disk location scan...")
		if err := s.scanner.ScanDiskLocations(s.diskDetector); err != nil {
			log.Printf("ERROR: Disk location scan failed: %v", err)
		} else {
			log.Println("INFO: Disk location scan completed successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Starting disk location scan...")
	w.Header().Set("X-Toast-Type", "info")

	respondSuccess(w, "Disk location scan started", nil)
}

// HandleDiskScanProgressHTML returns HTML for disk scan progress (HTMX endpoint)
func (s *Server) HandleDiskScanProgressHTML(w http.ResponseWriter, r *http.Request) {
	progress := s.scanner.GetDiskScanProgress()
	if progress == nil {
		w.Write([]byte(`<div class="text-gray-400">No disk scan running</div>`))
		return
	}

	snapshot := progress.GetSnapshot()

	// Calculate files per second
	elapsed := time.Since(snapshot.StartTime)
	var filesPerSec float64
	if elapsed.Seconds() > 0 && snapshot.ProcessedFiles > 0 {
		filesPerSec = float64(snapshot.ProcessedFiles) / elapsed.Seconds()
	}

	// Check if scan is completed - show final summary without polling
	if snapshot.CurrentPhase == "Completed" || snapshot.CurrentPhase == "Failed" {
		status := "Completed"
		statusColor := "green"
		statusIcon := `<svg class="w-5 h-5 inline-block mr-2 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>`

		if snapshot.CurrentPhase == "Failed" {
			status = "Failed"
			statusColor = "red"
			statusIcon = `<svg class="w-5 h-5 inline-block mr-2 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>`
		}

		completedHTML := fmt.Sprintf(`
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<div class="flex items-center text-sm text-gray-300">
					%s
					<span class="font-medium">%s</span>
				</div>
				<span class="text-lg font-bold text-%s-400">✓</span>
			</div>

			<div class="w-full bg-gray-700 rounded-full h-3 overflow-hidden">
				<div class="bg-gradient-to-r from-%s-500 to-%s-600 h-3 rounded-full shadow-lg" style="width: 100%%"></div>
			</div>

			<div class="grid grid-cols-2 gap-4 text-sm">
				<div>
					<div class="text-gray-500 text-xs">Files Processed</div>
					<div class="text-gray-200 font-medium">%d</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">Average Speed</div>
					<div class="text-gray-200 font-medium">%.1f files/sec</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">Total Time</div>
					<div class="text-gray-200 font-medium">%s</div>
				</div>
				<div>
					<div class="text-gray-500 text-xs">Status</div>
					<div class="text-%s-400 font-medium">%s</div>
				</div>
			</div>
		</div>
		`,
			statusIcon,
			status,
			statusColor,
			statusColor,
			statusColor,
			snapshot.ProcessedFiles,
			filesPerSec,
			stats.FormatDuration(elapsed),
			statusColor,
			status,
		)

		// Set HX-Trigger header to stop polling
		w.Header().Set("HX-Trigger", "diskScanCompleted")
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(completedHTML))
		return
	}

	// Phase-specific icon
	phaseIcon := `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path></svg>`

	// Running scan progress HTML
	percentDisplay := "Scanning..."
	if snapshot.TotalFiles > 0 {
		percentDisplay = fmt.Sprintf("%.1f%%", snapshot.PercentComplete)
	}

	html := fmt.Sprintf(`
	<div class="space-y-3" hx-get="/api/scan/disk-progress-html" hx-trigger="every 2s" hx-swap="outerHTML">
		<div class="flex items-center justify-between">
			<div class="flex items-center text-sm text-gray-300">
				%s
				<span class="font-medium">%s</span>
			</div>
			<span class="text-lg font-bold text-blue-400">%s</span>
		</div>

		<div class="w-full bg-gray-700 rounded-full h-3 overflow-hidden">
			<div class="bg-gradient-to-r from-blue-500 to-blue-600 h-3 rounded-full shadow-lg transition-all duration-300" style="width: %.1f%%"></div>
		</div>

		<div class="grid grid-cols-2 gap-4 text-sm">
			<div>
				<div class="text-gray-500 text-xs">Files Scanned</div>
				<div class="text-gray-200 font-medium">%d</div>
			</div>
			<div>
				<div class="text-gray-500 text-xs">Speed</div>
				<div class="text-gray-200 font-medium">%.1f files/sec</div>
			</div>
			<div>
				<div class="text-gray-500 text-xs">Elapsed</div>
				<div class="text-gray-200 font-medium">%s</div>
			</div>
			<div>
				<div class="text-gray-500 text-xs">Phase</div>
				<div class="text-blue-400 font-medium">%s</div>
			</div>
		</div>
	</div>
	`,
		phaseIcon,
		snapshot.CurrentPhase,
		percentDisplay,
		snapshot.PercentComplete,
		snapshot.ProcessedFiles,
		filesPerSec,
		stats.FormatDuration(elapsed),
		snapshot.CurrentPhase,
	)

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
}

// HandleDiskScanProgress streams disk scan logs via SSE
func (s *Server) HandleDiskScanProgress(w http.ResponseWriter, r *http.Request) {
	// Check if scanner exists
	if s.scanner == nil {
		log.Printf("ERROR: Scanner not initialized for disk scan SSE logs endpoint")
		http.Error(w, "Scanner not initialized", http.StatusInternalServerError)
		return
	}

	// Check if streaming is supported BEFORE writing any headers
	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Printf("ERROR: Streaming not supported for disk scan SSE logs endpoint")
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	// Now it's safe to write headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable buffering for nginx

	// Send initial connection message
	fmt.Fprintf(w, "data: <div class=\"text-gray-400\">Connected to disk scan log stream</div>\n\n")
	flusher.Flush()

	progress := s.scanner.GetDiskScanProgress()
	if progress == nil {
		// No scan running - keep connection open but send status updates
		fmt.Fprintf(w, "data: <div class=\"text-gray-500\">No disk scan currently running</div>\n\n")
		flusher.Flush()

		// Keep connection alive with periodic pings
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				fmt.Fprintf(w, ": keep-alive\n\n")
				flusher.Flush()

				// Check if a disk scan has started
				if s.scanner.GetDiskScanProgress() != nil {
					fmt.Fprintf(w, "data: <div class=\"text-green-400\">Disk scan started, reconnect to see logs</div>\n\n")
					flusher.Flush()
					return
				}
			}
		}
	}

	// Subscribe to log messages
	logChan := progress.Subscribe()
	if logChan == nil {
		fmt.Fprintf(w, "data: <div class=\"text-red-400\">Failed to subscribe to disk scan logs</div>\n\n")
		flusher.Flush()
		return
	}
	defer progress.Unsubscribe(logChan)

	// Create ticker for keep-alive heartbeat (every 30 seconds to keep connection alive)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Stream log messages
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			// Send keep-alive comment (ignored by SSE clients)
			fmt.Fprintf(w, ": keep-alive\n\n")
			flusher.Flush()
		case msg, ok := <-logChan:
			if !ok {
				// Channel closed, scan finished
				fmt.Fprintf(w, "data: <div class=\"text-green-400\">Disk scan completed</div>\n\n")
				flusher.Flush()
				return
			}
			fmt.Fprintf(w, "data: <div class=\"text-gray-300\">%s</div>\n\n", msg)
			flusher.Flush()
		}
	}
}

// HandleGetFileDiskLocations returns all disk locations for a specific file
func (s *Server) HandleGetFileDiskLocations(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	// Get file ID from query parameter
	fileIDStr := r.URL.Query().Get("id")
	if fileIDStr == "" {
		respondError(w, http.StatusBadRequest, "File ID is required", "missing_file_id")
		return
	}

	fileID, err := strconv.ParseInt(fileIDStr, 10, 64)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid file ID", "invalid_file_id")
		return
	}

	// Get disk locations from database
	locations, err := s.db.GetDiskLocationsForFile(fileID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to get disk locations", "db_error")
		return
	}

	// Build response with resolved disk names
	type locationResponse struct {
		ID           int64  `json:"id"`
		FileID       int64  `json:"file_id"`
		DiskName     string `json:"disk_name"`
		DeviceID     int64  `json:"device_id"`
		DeviceName   string `json:"device_name"`  // Friendly name from resolver
		DeviceColor  string `json:"device_color"` // Badge color
		DiskPath     string `json:"disk_path"`
		Size         int64  `json:"size"`
		Inode        int64  `json:"inode"`
		ModifiedTime int64  `json:"modified_time"`
		LastVerified int64  `json:"last_verified"`
	}

	response := make([]locationResponse, 0, len(locations))
	for _, loc := range locations {
		lr := locationResponse{
			ID:           loc.ID,
			FileID:       loc.FileID,
			DiskName:     loc.DiskName,
			DeviceID:     loc.DiskDeviceID,
			DiskPath:     loc.DiskPath,
			Size:         loc.Size,
			Inode:        loc.Inode,
			ModifiedTime: loc.ModifiedTime.Unix(),
			LastVerified: loc.LastVerified.Unix(),
		}

		// Resolve device name and color if resolver is available
		if s.diskResolver != nil {
			lr.DeviceName = s.diskResolver.ResolveDisplayName(loc.DiskDeviceID)
			lr.DeviceColor = s.diskResolver.ResolveColor(loc.DiskDeviceID)
		} else {
			lr.DeviceName = fmt.Sprintf("Device %d", loc.DiskDeviceID)
			lr.DeviceColor = "gray"
		}

		response = append(response, lr)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// getServiceDisplayName returns the properly capitalized display name for a service
func getServiceDisplayName(serviceName string) string {
	switch serviceName {
	case "plex":
		return "Plex"
	case "sonarr":
		return "Sonarr"
	case "radarr":
		return "Radarr"
	case "qbittorrent":
		return "qBittorrent"
	case "stash":
		return "Stash"
	default:
		return serviceName
	}
}

// HandleTestService tests connection to a service using current form values
func (s *Server) HandleTestService(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	serviceName := r.URL.Query().Get("service")
	displayName := getServiceDisplayName(serviceName)

	// Create a temporary config with form values for testing
	testConfig := *s.config // Copy current config

	// Read form values and validate based on service type
	var missingField string
	switch serviceName {
	case "plex":
		url := strings.TrimSpace(r.FormValue("plex_url"))
		token := strings.TrimSpace(r.FormValue("plex_token"))
		if url == "" {
			missingField = "Plex URL"
		} else if token == "" {
			missingField = "Plex Token"
		} else {
			testConfig.Services.Plex.URL = url
			testConfig.Services.Plex.Token = token
		}
	case "sonarr":
		url := strings.TrimSpace(r.FormValue("sonarr_url"))
		apiKey := strings.TrimSpace(r.FormValue("sonarr_api_key"))
		if url == "" {
			missingField = "Sonarr URL"
		} else if apiKey == "" {
			missingField = "Sonarr API Key"
		} else {
			testConfig.Services.Sonarr.URL = url
			testConfig.Services.Sonarr.APIKey = apiKey
		}
	case "radarr":
		url := strings.TrimSpace(r.FormValue("radarr_url"))
		apiKey := strings.TrimSpace(r.FormValue("radarr_api_key"))
		if url == "" {
			missingField = "Radarr URL"
		} else if apiKey == "" {
			missingField = "Radarr API Key"
		} else {
			testConfig.Services.Radarr.URL = url
			testConfig.Services.Radarr.APIKey = apiKey
		}
	case "qbittorrent":
		url := strings.TrimSpace(r.FormValue("qbittorrent_url"))
		if url == "" {
			missingField = "qBittorrent URL"
		} else {
			testConfig.Services.QBittorrent.URL = url
			testConfig.Services.QBittorrent.Username = strings.TrimSpace(r.FormValue("qbittorrent_username"))
			testConfig.Services.QBittorrent.Password = strings.TrimSpace(r.FormValue("qbittorrent_password"))
			testConfig.Services.QBittorrent.QuiProxyURL = strings.TrimSpace(r.FormValue("qbittorrent_qui_proxy_url"))
		}
	case "stash":
		url := strings.TrimSpace(r.FormValue("stash_url"))
		apiKey := strings.TrimSpace(r.FormValue("stash_api_key"))
		if url == "" {
			missingField = "Stash URL"
		} else if apiKey == "" {
			missingField = "Stash API Key"
		} else {
			testConfig.Services.Stash.URL = url
			testConfig.Services.Stash.APIKey = apiKey
		}
	}

	// If a required field is missing, return warning toast
	if missingField != "" {
		w.Header().Set("X-Toast-Message", fmt.Sprintf("No %s provided", missingField))
		w.Header().Set("X-Toast-Type", "warning")
		respondSuccess(w, "No configuration to test", nil)
		return
	}

	// Create a temporary client factory with the test config
	testFactory := api.NewClientFactory(&testConfig)

	// Use factory to create client with test config
	testClient, err := testFactory.CreateClient(serviceName, testConfig.APITimeout)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Unknown service", "unknown_service")
		return
	}

	// Test the connection
	if err := testClient.Test(); err != nil {
		w.Header().Set("X-Toast-Message", fmt.Sprintf("%s connection failed: %v", displayName, err))
		w.Header().Set("X-Toast-Type", "error")
		respondError(w, http.StatusBadRequest, err.Error(), "connection_failed")
		return
	}

	w.Header().Set("X-Toast-Message", fmt.Sprintf("%s connection successful", displayName))
	w.Header().Set("X-Toast-Type", "success")

	response := TestServiceResponse{
		Status:  "success",
		Message: "Connection successful",
		Service: serviceName,
	}
	respondJSON(w, http.StatusOK, response)
}

// HandleGetPlexLibraries fetches available Plex libraries for selection in UI
func (s *Server) HandleGetPlexLibraries(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	// Get Plex URL and token from query parameters
	plexURL := strings.TrimSpace(r.URL.Query().Get("url"))
	plexToken := strings.TrimSpace(r.URL.Query().Get("token"))

	if plexURL == "" {
		respondError(w, http.StatusBadRequest, "Plex URL is required", "missing_url")
		return
	}

	if plexToken == "" {
		respondError(w, http.StatusBadRequest, "Plex token is required", "missing_token")
		return
	}

	// Create a temporary Plex client
	client := api.NewPlexClient(plexURL, plexToken, s.config.APITimeout)

	// First test the connection
	if err := client.Test(); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Connection failed: %v", err), "connection_failed")
		return
	}

	// Fetch library sections
	ctx := r.Context()
	libraries, err := client.GetLibrarySections(ctx)
	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch libraries: %v", err), "fetch_failed")
		return
	}

	// Return libraries as JSON
	respondJSON(w, http.StatusOK, libraries)
}

// HandleTestScanPaths tests if configured scan paths exist and are accessible
func (s *Server) HandleTestScanPaths(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Parse scan paths from form data
	scanPathsStr := r.FormValue("scan_paths")
	if scanPathsStr == "" {
		w.Header().Set("X-Toast-Message", "No scan paths provided")
		w.Header().Set("X-Toast-Type", "warning")
		respondSuccess(w, "No paths to test", nil)
		return
	}

	lines := strings.Split(scanPathsStr, "\n")
	var paths []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			paths = append(paths, line)
		}
	}

	if len(paths) == 0 {
		w.Header().Set("X-Toast-Message", "No valid scan paths found")
		w.Header().Set("X-Toast-Type", "warning")
		respondSuccess(w, "No paths to test", nil)
		return
	}

	// Test each path
	var results []string
	var errors []string
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				errors = append(errors, fmt.Sprintf("%s: does not exist", path))
			} else if os.IsPermission(err) {
				errors = append(errors, fmt.Sprintf("%s: permission denied", path))
			} else {
				errors = append(errors, fmt.Sprintf("%s: %v", path, err))
			}
		} else if !info.IsDir() {
			errors = append(errors, fmt.Sprintf("%s: not a directory", path))
		} else {
			results = append(results, fmt.Sprintf("%s: OK (accessible directory)", path))
		}
	}

	// Send response
	if len(errors) > 0 {
		// Render validation errors panel
		s.renderValidationErrors(w, "Scan Path Validation Failed", errors)
		return
	}

	// Success - show toast and clear error panel
	w.Header().Set("X-Toast-Message", fmt.Sprintf("All %d scan path(s) validated successfully", len(results)))
	w.Header().Set("X-Toast-Type", "success")
	w.WriteHeader(http.StatusOK)
	// Clear and hide the validation-errors div
	w.Write([]byte(`<script>document.getElementById('validation-errors').classList.add('hidden');</script>`))
}

// HandleTestPathMappings validates path mapping syntax and tests paths
func (s *Server) HandleTestPathMappings(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	localMappingsStr := r.FormValue("local_path_mappings")
	serviceMappingsStr := r.FormValue("service_path_mappings")

	if localMappingsStr == "" && serviceMappingsStr == "" {
		w.Header().Set("X-Toast-Message", "No path mappings provided")
		w.Header().Set("X-Toast-Type", "warning")
		respondSuccess(w, "No mappings to test", nil)
		return
	}

	var errors []string
	var successes []string
	mappingCount := 0

	// Test local path mappings
	if localMappingsStr != "" {
		lines := strings.Split(localMappingsStr, "\n")
		for i, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			mappingCount++

			parts := strings.SplitN(line, "=", 2)
			if len(parts) != 2 {
				errors = append(errors, fmt.Sprintf("Line %d: invalid format (expected 'service=local')", i+1))
				continue
			}

			service := strings.TrimSpace(parts[0])
			local := strings.TrimSpace(parts[1])

			if service == "" || local == "" {
				errors = append(errors, fmt.Sprintf("Line %d: empty service or local path", i+1))
				continue
			}

			// Test service path (left side - what media-finder can see)
			if _, err := os.Stat(service); err != nil {
				errors = append(errors, fmt.Sprintf("%s=%s: service path error: %v", service, local, err))
			} else {
				successes = append(successes, fmt.Sprintf("%s=%s: OK (service path accessible)", service, local))
			}
		}
	}

	// Test service path mappings
	if serviceMappingsStr != "" {
		// First, collect service configurations from form values for intelligent testing
		serviceConfigs := s.collectServiceConfigsFromForm(r)

		lines := strings.Split(serviceMappingsStr, "\n")
		for i, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			mappingCount++

			// Split servicename:service=local
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				errors = append(errors, fmt.Sprintf("Service line %d: invalid format (expected 'servicename:service=local')", i+1))
				continue
			}

			serviceName := strings.TrimSpace(parts[0])
			pathParts := strings.SplitN(parts[1], "=", 2)
			if len(pathParts) != 2 {
				errors = append(errors, fmt.Sprintf("Service line %d: invalid format (expected 'servicename:service=local')", i+1))
				continue
			}

			servicePath := strings.TrimSpace(pathParts[0])
			localPath := strings.TrimSpace(pathParts[1])

			if serviceName == "" || servicePath == "" || localPath == "" {
				errors = append(errors, fmt.Sprintf("Service line %d: empty service name, service path, or local path", i+1))
				continue
			}

			// Test local path (right side - what media-finder can access)
			if _, err := os.Stat(localPath); err != nil {
				errors = append(errors, fmt.Sprintf("%s:%s=%s: local path error: %v", serviceName, servicePath, localPath, err))
				continue
			}

			// Intelligent validation: query service for actual file and test translation
			if cfg, hasConfig := serviceConfigs[serviceName]; hasConfig {
				if err := s.testServicePathMapping(serviceName, localPath, servicePath, cfg); err != nil {
					errors = append(errors, fmt.Sprintf("%s:%s=%s: mapping validation failed: %v", serviceName, servicePath, localPath, err))
				} else {
					successes = append(successes, fmt.Sprintf("%s:%s=%s: OK (local path accessible, mapping verified)", serviceName, servicePath, localPath))
				}
			} else {
				// No service config available, only basic validation
				successes = append(successes, fmt.Sprintf("%s:%s=%s: OK (local path accessible, no service config for intelligent test)", serviceName, servicePath, localPath))
			}
		}
	}

	// Send response
	if len(errors) > 0 {
		// Render validation errors panel
		s.renderValidationErrors(w, "Path Mapping Validation Failed", errors)
		return
	}

	if mappingCount == 0 {
		w.Header().Set("X-Toast-Message", "No valid mappings found")
		w.Header().Set("X-Toast-Type", "warning")
		w.WriteHeader(http.StatusOK)
		// Clear and hide the validation-errors div
		w.Write([]byte(`<script>document.getElementById('validation-errors').classList.add('hidden');</script>`))
		return
	}

	// Success - show toast and clear error panel
	w.Header().Set("X-Toast-Message", fmt.Sprintf("All %d path mapping(s) validated successfully", len(successes)))
	w.Header().Set("X-Toast-Type", "success")
	w.WriteHeader(http.StatusOK)
	// Clear and hide the validation-errors div
	w.Write([]byte(`<script>document.getElementById('validation-errors').classList.add('hidden');</script>`))
}

// collectServiceConfigsFromForm reads service configurations from form values
// Returns a map of service name to temporary config for testing
func (s *Server) collectServiceConfigsFromForm(r *http.Request) map[string]interface{} {
	configs := make(map[string]interface{})

	// Plex config
	if plexURL := strings.TrimSpace(r.FormValue("plex_url")); plexURL != "" {
		if plexToken := strings.TrimSpace(r.FormValue("plex_token")); plexToken != "" {
			configs["plex"] = map[string]string{
				"url":   plexURL,
				"token": plexToken,
			}
		}
	}

	// Sonarr config
	if sonarrURL := strings.TrimSpace(r.FormValue("sonarr_url")); sonarrURL != "" {
		if sonarrKey := strings.TrimSpace(r.FormValue("sonarr_api_key")); sonarrKey != "" {
			configs["sonarr"] = map[string]string{
				"url":     sonarrURL,
				"api_key": sonarrKey,
			}
		}
	}

	// Radarr config
	if radarrURL := strings.TrimSpace(r.FormValue("radarr_url")); radarrURL != "" {
		if radarrKey := strings.TrimSpace(r.FormValue("radarr_api_key")); radarrKey != "" {
			configs["radarr"] = map[string]string{
				"url":     radarrURL,
				"api_key": radarrKey,
			}
		}
	}

	// qBittorrent config
	if qbitURL := strings.TrimSpace(r.FormValue("qbittorrent_url")); qbitURL != "" {
		// qBittorrent requires username and password
		qbitUsername := strings.TrimSpace(r.FormValue("qbittorrent_username"))
		qbitPassword := strings.TrimSpace(r.FormValue("qbittorrent_password"))
		qbitQuiProxy := strings.TrimSpace(r.FormValue("qbittorrent_qui_proxy_url"))

		if qbitUsername != "" && qbitPassword != "" {
			configs["qbittorrent"] = map[string]string{
				"url":           qbitURL,
				"username":      qbitUsername,
				"password":      qbitPassword,
				"qui_proxy_url": qbitQuiProxy, // May be empty
			}
		}
	}

	// Stash config
	if stashURL := strings.TrimSpace(r.FormValue("stash_url")); stashURL != "" {
		if stashKey := strings.TrimSpace(r.FormValue("stash_api_key")); stashKey != "" {
			configs["stash"] = map[string]string{
				"url":     stashURL,
				"api_key": stashKey,
			}
		}
	}

	return configs
}

// testServicePathMapping validates a service path mapping by querying the service
// and testing if the path translation works correctly
func (s *Server) testServicePathMapping(serviceName, localPath, servicePath string, cfg interface{}) error {
	// Get a sample file path from the service
	var sampleFilePath string
	var err error

	switch serviceName {
	case "plex":
		configMap := cfg.(map[string]string)
		sampleFilePath, err = s.getSamplePlexFilePath(configMap["url"], configMap["token"], servicePath)
	case "sonarr":
		configMap := cfg.(map[string]string)
		sampleFilePath, err = s.getSampleArrFilePath(configMap["url"], configMap["api_key"], servicePath, "sonarr")
	case "radarr":
		configMap := cfg.(map[string]string)
		sampleFilePath, err = s.getSampleArrFilePath(configMap["url"], configMap["api_key"], servicePath, "radarr")
	case "qbittorrent":
		configMap := cfg.(map[string]string)
		sampleFilePath, err = s.getSampleQBittorrentFilePath(configMap["url"], configMap["username"], configMap["password"], configMap["qui_proxy_url"], servicePath)
	case "stash":
		configMap := cfg.(map[string]string)
		sampleFilePath, err = s.getSampleStashFilePath(configMap["url"], configMap["api_key"], servicePath)
	default:
		return fmt.Errorf("unsupported service: %s", serviceName)
	}

	if err != nil {
		// If we can't get a sample file, that's okay - service might be empty or not configured yet
		return nil
	}

	if sampleFilePath == "" {
		// No files found in service - can't validate mapping
		return nil
	}

	// Test if we can translate the service path to local path
	if !strings.HasPrefix(sampleFilePath, servicePath) {
		return fmt.Errorf("service file path '%s' doesn't start with expected service path '%s'", sampleFilePath, servicePath)
	}

	// Replace service path with local path
	translatedPath := strings.Replace(sampleFilePath, servicePath, localPath, 1)

	// Check if the translated path exists
	if _, err := os.Stat(translatedPath); err != nil {
		return fmt.Errorf("translated path '%s' doesn't exist (from service path '%s')", translatedPath, sampleFilePath)
	}

	return nil
}

// getSamplePlexFilePath gets a sample file path from Plex library
func (s *Server) getSamplePlexFilePath(url, token, pathPrefix string) (string, error) {
	// Create temporary config for testing
	testConfig := *s.config
	testConfig.Services.Plex.URL = url
	testConfig.Services.Plex.Token = token

	// Create client via factory
	factory := api.NewClientFactory(&testConfig)
	client, err := factory.CreateClient("plex", testConfig.APITimeout)
	if err != nil {
		return "", err
	}

	// Cast to PlexClient to access GetSampleFile
	plexClient, ok := client.(*api.PlexClient)
	if !ok {
		return "", fmt.Errorf("failed to cast to PlexClient")
	}

	// Get a sample file that matches the path prefix (optimized - stops at first match)
	return plexClient.GetSampleFile(pathPrefix)
}

// getSampleArrFilePath gets a sample file path from Sonarr/Radarr
func (s *Server) getSampleArrFilePath(url, apiKey, pathPrefix, serviceType string) (string, error) {
	// Create temporary config for testing
	testConfig := *s.config

	switch serviceType {
	case "sonarr":
		testConfig.Services.Sonarr.URL = url
		testConfig.Services.Sonarr.APIKey = apiKey
	case "radarr":
		testConfig.Services.Radarr.URL = url
		testConfig.Services.Radarr.APIKey = apiKey
	}

	// Create client via factory
	factory := api.NewClientFactory(&testConfig)
	client, err := factory.CreateClient(serviceType, testConfig.APITimeout)
	if err != nil {
		return "", err
	}

	// Cast to appropriate client type to access GetSampleFile
	switch serviceType {
	case "sonarr":
		sonarrClient, ok := client.(*api.SonarrClient)
		if !ok {
			return "", fmt.Errorf("failed to cast to SonarrClient")
		}

		// Get a sample file that matches the path prefix (optimized - stops at first match)
		return sonarrClient.GetSampleFile(pathPrefix)
	case "radarr":
		radarrClient, ok := client.(*api.RadarrClient)
		if !ok {
			return "", fmt.Errorf("failed to cast to RadarrClient")
		}

		// Get a sample file that matches the path prefix (optimized - stops at first match)
		return radarrClient.GetSampleFile(pathPrefix)
	}

	return "", nil
}

// getSampleQBittorrentFilePath gets a sample file path from qBittorrent
func (s *Server) getSampleQBittorrentFilePath(url, username, password, quiProxyURL, pathPrefix string) (string, error) {
	// Create temporary config for testing
	testConfig := *s.config
	testConfig.Services.QBittorrent.URL = url
	testConfig.Services.QBittorrent.Username = username
	testConfig.Services.QBittorrent.Password = password
	testConfig.Services.QBittorrent.QuiProxyURL = quiProxyURL

	// Create client via factory
	factory := api.NewClientFactory(&testConfig)
	client, err := factory.CreateClient("qbittorrent", testConfig.APITimeout)
	if err != nil {
		return "", err
	}

	// Cast to QBittorrentClient to access GetSampleFile
	qbitClient, ok := client.(*api.QBittorrentClient)
	if !ok {
		return "", fmt.Errorf("failed to cast to QBittorrentClient")
	}

	// Get a sample file that matches the path prefix (optimized - stops at first match)
	return qbitClient.GetSampleFile(pathPrefix)
}

// getSampleStashFilePath gets a sample file path from Stash
func (s *Server) getSampleStashFilePath(url, apiKey, pathPrefix string) (string, error) {
	// Create temporary config for testing
	testConfig := *s.config
	testConfig.Services.Stash.URL = url
	testConfig.Services.Stash.APIKey = apiKey

	// Create client via factory
	factory := api.NewClientFactory(&testConfig)
	client, err := factory.CreateClient("stash", testConfig.APITimeout)
	if err != nil {
		return "", err
	}

	// Cast to StashClient to access GetSampleFile
	stashClient, ok := client.(*api.StashClient)
	if !ok {
		return "", fmt.Errorf("failed to cast to StashClient")
	}

	// Get a sample file that matches the path prefix (optimized - stops at first match)
	return stashClient.GetSampleFile(pathPrefix)
}

// HandleExport exports files list using streaming for memory efficiency
func (s *Server) HandleExport(w http.ResponseWriter, r *http.Request) {
	format := r.URL.Query().Get("format")
	orphanedOnly := r.URL.Query().Get("orphaned") == "true"

	// Validate format before writing response
	if format != "json" && format != "csv" {
		http.Error(w, "Invalid format. Supported formats: json, csv", http.StatusBadRequest)
		return
	}

	// Stream files in batches to avoid loading everything into memory
	batchSize := constants.ExportBatchSize
	offset := 0

	switch format {
	case "json":
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=files.json")

		// Write opening bracket
		w.Write([]byte("[\n"))

		first := true
		for {
			files, _, err := s.db.ListFiles(orphanedOnly, nil, "any", false, nil, nil, batchSize, offset, "path", "asc")
			if err != nil {
				if offset == 0 {
					http.Error(w, "Failed to list files", http.StatusInternalServerError)
				}
				return
			}

			if len(files) == 0 {
				break
			}

			for _, file := range files {
				if !first {
					w.Write([]byte(",\n"))
				}
				first = false

				// Stream each file entry - marshal manually to avoid newline issues
				data, err := json.Marshal(file)
				if err != nil {
					log.Printf("Failed to marshal file: %v", err)
					continue
				}
				w.Write(data)
			}

			offset += batchSize

			// Flush to client
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}

		// Write closing bracket
		w.Write([]byte("\n]"))

	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=files.csv")

		// Create CSV writer for proper escaping
		csvWriter := csv.NewWriter(w)
		defer csvWriter.Flush()

		// Write CSV header
		if err := csvWriter.Write([]string{"path", "size", "is_orphaned"}); err != nil {
			http.Error(w, "Failed to write CSV header", http.StatusInternalServerError)
			return
		}

		for {
			files, _, err := s.db.ListFiles(orphanedOnly, nil, "any", false, nil, nil, batchSize, offset, "path", "asc")
			if err != nil {
				if offset == 0 {
					http.Error(w, "Failed to list files", http.StatusInternalServerError)
				}
				return
			}

			if len(files) == 0 {
				break
			}

			// Stream CSV rows in batches
			for _, file := range files {
				record := []string{
					file.Path,
					fmt.Sprintf("%d", file.Size),
					fmt.Sprintf("%v", file.IsOrphaned),
				}
				if err := csvWriter.Write(record); err != nil {
					log.Printf("Failed to write CSV record: %v", err)
					continue
				}
			}

			csvWriter.Flush()
			offset += batchSize

			// Flush to client
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}

	default:
		// This should never be reached due to validation at start
		http.Error(w, "Invalid format", http.StatusBadRequest)
	}
}

// HandleDeleteFile deletes a file or files
func (s *Server) HandleDeleteFile(w http.ResponseWriter, r *http.Request) {
	if !requireAnyMethod(w, r, http.MethodPost, http.MethodDelete) {
		return
	}

	fileID := r.URL.Query().Get("id")
	orphaned := r.URL.Query().Get("orphaned") == "true"

	// Single file deletion
	if fileID != "" {
		id, err := strconv.ParseInt(fileID, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, "Invalid file ID", "invalid_file_id")
			return
		}

		if err := s.db.DeleteFile(id, "UI deletion"); err != nil {
			respondError(w, http.StatusInternalServerError, "Failed to delete file", "delete_failed")
			return
		}

		w.Header().Set("X-Toast-Message", "File deleted successfully")
		w.Header().Set("X-Toast-Type", "success")
		respondSuccess(w, "File deleted", nil)
		return
	}

	// Bulk orphaned files deletion
	if orphaned {
		files, _, err := s.db.ListFiles(true, nil, "any", false, nil, nil, constants.MaxExportFiles, 0, "path", "asc")
		if err != nil {
			respondError(w, http.StatusInternalServerError, "Failed to list orphaned files", "list_failed")
			return
		}

		if len(files) == 0 {
			respondSuccess(w, "No orphaned files to delete", nil)
			return
		}

		deleted := 0
		errors := 0
		for _, file := range files {
			if err := s.db.DeleteFile(file.ID, "Bulk orphaned cleanup"); err != nil {
				errors++
				continue
			}
			deleted++
		}

		w.Header().Set("X-Toast-Message", fmt.Sprintf("Deleted %d files", deleted))
		w.Header().Set("X-Toast-Type", "success")

		response := BulkDeleteResponse{
			Status:  "success",
			Message: "Bulk deletion completed",
			Deleted: deleted,
			Errors:  errors,
		}
		respondJSON(w, http.StatusOK, response)
		return
	}

	respondError(w, http.StatusBadRequest, "Must specify file ID or orphaned flag", "missing_parameter")
}

// HandleFileDetails returns detailed information about a specific file
func (s *Server) HandleFileDetails(w http.ResponseWriter, r *http.Request) {
	fileIDStr := r.URL.Query().Get("id")
	if fileIDStr == "" {
		respondError(w, http.StatusBadRequest, "File ID is required", "missing_file_id")
		return
	}

	fileID, err := strconv.ParseInt(fileIDStr, 10, 64)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid file ID", "invalid_file_id")
		return
	}

	// Get file
	file, err := s.db.GetFileByID(fileID)
	if err != nil {
		respondError(w, http.StatusNotFound, "File not found", "file_not_found")
		return
	}

	// Get usage
	usage, err := s.db.GetUsageByFileID(fileID)
	if err != nil {
		usage = []*database.Usage{} // Empty array on error
	}

	// Get hardlinks if applicable (optimized query)
	var hardlinks []string
	if file.Inode != 0 && file.DeviceID != 0 {
		group, err := s.db.GetHardlinksByInodeDevice(file.Inode, file.DeviceID)
		if err == nil && len(group) > 1 {
			hardlinks = make([]string, 0, len(group))
			for _, f := range group {
				hardlinks = append(hardlinks, f.Path)
			}
		}
	}

	// Get disk locations if available
	var diskLocations []*database.FileDiskLocation
	if len(s.config.Disks) > 0 {
		diskLocations, _ = s.db.GetDiskLocationsForFile(fileID)
	}

	response := FileDetailsResponse{
		ID:            file.ID,
		Path:          file.Path,
		Size:          file.Size,
		Inode:         file.Inode,
		DeviceID:      file.DeviceID,
		ModifiedTime:  file.ModifiedTime.Unix(),
		LastVerified:  file.LastVerified.Unix(),
		IsOrphaned:    file.IsOrphaned,
		CreatedAt:     file.CreatedAt.Unix(),
		Usage:         usage,
		Hardlinks:     hardlinks,
		DiskLocations: diskLocations,
	}

	// Resolve device name and color
	// Prefer disk location info if available (more accurate for mergerfs setups)
	if len(diskLocations) > 0 {
		// Use the first disk location (files typically exist on one disk)
		loc := diskLocations[0]
		if s.diskResolver != nil {
			response.DeviceName = s.diskResolver.ResolveDisplayName(loc.DiskDeviceID)
			response.DeviceColor = s.diskResolver.ResolveColor(loc.DiskDeviceID)
		} else {
			response.DeviceName = fmt.Sprintf("%s (%d)", loc.DiskName, loc.DiskDeviceID)
			response.DeviceColor = "blue"
		}
	} else if s.diskResolver != nil {
		// Fallback to file's device_id (for files not yet scanned with disk locations)
		response.DeviceName = s.diskResolver.ResolveDisplayName(file.DeviceID)
		response.DeviceColor = s.diskResolver.ResolveColor(file.DeviceID)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleMarkRescan marks files for rescan
func (s *Server) HandleMarkRescan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	fileIDStr := r.URL.Query().Get("id")
	orphaned := r.URL.Query().Get("orphaned") == "true"

	// Single file by ID (safe from SQL injection)
	if fileIDStr != "" {
		fileID, err := strconv.ParseInt(fileIDStr, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, "Invalid file ID", "invalid_file_id")
			return
		}

		if err := s.db.MarkFileForRescan(fileID); err != nil {
			respondError(w, http.StatusInternalServerError, "Failed to mark file for rescan", "rescan_failed")
			return
		}

		w.Header().Set("X-Toast-Message", "Marked for rescan")
		w.Header().Set("X-Toast-Type", "success")

		response := BulkRescanResponse{
			Status:  "success",
			Message: "File marked for rescan",
			Count:   1,
		}
		respondJSON(w, http.StatusOK, response)
		return
	}

	// Bulk orphaned files
	if orphaned {
		count, err := s.db.MarkFilesForRescan("orphaned")
		if err != nil {
			respondError(w, http.StatusInternalServerError, "Failed to mark files for rescan", "rescan_failed")
			return
		}

		w.Header().Set("X-Toast-Message", fmt.Sprintf("Marked %d files for rescan", count))
		w.Header().Set("X-Toast-Type", "success")

		response := BulkRescanResponse{
			Status:  "success",
			Message: "Files marked for rescan",
			Count:   count,
		}
		respondJSON(w, http.StatusOK, response)
		return
	}

	respondError(w, http.StatusBadRequest, "Must specify file ID or orphaned flag", "missing_parameter")
}

// Admin/Advanced page handlers

// HandleAdvanced renders the advanced admin page
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
func (s *Server) HandleAdminClearFiles(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	orphanedOnly := r.URL.Query().Get("orphaned") == "true"

	var count int64
	var err error
	var message string

	if orphanedOnly {
		count, err = s.db.ClearOrphanedFiles()
		message = fmt.Sprintf("Cleared %d orphaned files", count)
	} else {
		count, err = s.db.ClearAllFiles()
		message = fmt.Sprintf("Cleared %d files", count)
	}

	if err != nil {
		log.Printf("Failed to clear files: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to clear files", "clear_failed")
		return
	}

	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
	})
}

// HandleAdminClearScans handles clearing scan history
func (s *Server) HandleAdminClearScans(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	count, err := s.db.ClearScans()
	if err != nil {
		log.Printf("Failed to clear scans: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to clear scan history", "clear_failed")
		return
	}

	message := fmt.Sprintf("Cleared %d scan records", count)
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
	})
}

// HandleAdminClearUsage handles clearing service usage records
func (s *Server) HandleAdminClearUsage(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	service := r.URL.Query().Get("service")

	var count int64
	var err error
	var message string

	if service != "" {
		err = s.db.DeleteUsageByService(r.Context(), service)
		if err != nil {
			log.Printf("Failed to clear usage for service %s: %v", service, err)
			respondError(w, http.StatusInternalServerError, "Failed to clear usage", "clear_failed")
			return
		}
		// We don't get a count from DeleteUsageByService, so we'll just report success
		message = fmt.Sprintf("Cleared usage records for %s", service)
		count = 0 // Unknown count
	} else {
		count, err = s.db.ClearAllUsage()
		if err != nil {
			log.Printf("Failed to clear all usage: %v", err)
			respondError(w, http.StatusInternalServerError, "Failed to clear usage", "clear_failed")
			return
		}
		message = fmt.Sprintf("Cleared %d usage records", count)
	}

	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
	})
}

// HandleAdminVacuum handles database vacuum and analyze
func (s *Server) HandleAdminVacuum(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if err := s.db.VacuumDatabase(); err != nil {
		log.Printf("Failed to vacuum database: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to optimize database", "vacuum_failed")
		return
	}

	message := "Database optimized successfully"
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
	})
}

// HandleAdminRebuildFTS handles rebuilding the full-text search index
func (s *Server) HandleAdminRebuildFTS(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if err := s.db.RebuildFTSIndex(); err != nil {
		log.Printf("Failed to rebuild FTS index: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to rebuild search index", "rebuild_failed")
		return
	}

	message := "Search index rebuilt successfully"
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
	})
}

// HandleAdminCleanStaleScans handles cleaning up stale running scans
func (s *Server) HandleAdminCleanStaleScans(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	count, err := s.db.CleanStaleScans()
	if err != nil {
		log.Printf("Failed to clean stale scans: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to clean stale scans", "clean_failed")
		return
	}

	message := fmt.Sprintf("Cleaned %d stale scans", count)
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
	})
}

// HandleAdminRecalculateOrphaned handles recalculating orphaned file status
func (s *Server) HandleAdminRecalculateOrphaned(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if err := s.db.UpdateOrphanedStatus(r.Context()); err != nil {
		log.Printf("Failed to recalculate orphaned status: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to recalculate orphaned status", "recalc_failed")
		return
	}

	message := "Orphaned status recalculated successfully"
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
	})
}

// HandleAdminDatabaseStats returns database statistics as JSON
func (s *Server) HandleAdminDatabaseStats(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	stats, err := s.db.GetDatabaseStats()
	if err != nil {
		log.Printf("Failed to get database stats: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to get database statistics", "stats_failed")
		return
	}

	respondJSON(w, http.StatusOK, stats)
}

// HandleAdminAuditLog returns paginated audit log entries
func (s *Server) HandleAdminAuditLog(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	limit := 50
	offset := 0

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if parsed, err := strconv.Atoi(offsetStr); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	entries, total, err := s.db.GetAuditLog(limit, offset)
	if err != nil {
		log.Printf("Failed to get audit log: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to get audit log", "audit_failed")
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"entries": entries,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

// HandleAdminClearConfig handles clearing all configuration
func (s *Server) HandleAdminClearConfig(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	count, err := s.db.ClearConfig()
	if err != nil {
		log.Printf("Failed to clear config: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to clear configuration", "clear_failed")
		return
	}

	message := fmt.Sprintf("Cleared %d configuration values", count)
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
	})
}

// HandleAdminClearAuditLog handles clearing old audit log entries
func (s *Server) HandleAdminClearAuditLog(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	days := 90 // Default to 90 days
	if daysStr := r.URL.Query().Get("days"); daysStr != "" {
		if parsed, err := strconv.Atoi(daysStr); err == nil && parsed > 0 {
			days = parsed
		}
	}

	count, err := s.db.ClearAuditLog(days)
	if err != nil {
		log.Printf("Failed to clear audit log: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to clear audit log", "clear_failed")
		return
	}

	message := fmt.Sprintf("Cleared %d audit log entries older than %d days", count, days)
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
		"days":    days,
	})
}

// renderTemplate renders an HTML template
func (s *Server) renderTemplate(w http.ResponseWriter, name string, data interface{}) {
	if s.templates == nil {
		http.Error(w, "Templates not loaded", http.StatusInternalServerError)
		return
	}

	tmpl, ok := s.templates[name]
	if !ok {
		http.Error(w, fmt.Sprintf("Template %s not found", name), http.StatusInternalServerError)
		return
	}

	// Inject version into template data
	if dataMap, ok := data.(map[string]interface{}); ok {
		dataMap["Version"] = s.version
	}
	// Version is already set in DuplicatesData and other typed structs

	// Execute layout.html which will call the "content" block from the specific page template
	if err := tmpl.ExecuteTemplate(w, "layout.html", data); err != nil {
		log.Printf("ERROR: Failed to execute template %s: %v", name, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ValidationErrorsData holds data for rendering validation errors
type ValidationErrorsData struct {
	Title  string
	Errors []string
}

// renderValidationErrors renders the validation errors partial template as HTML
func (s *Server) renderValidationErrors(w http.ResponseWriter, title string, errors []string) {
	if s.templates == nil {
		http.Error(w, "Templates not loaded", http.StatusInternalServerError)
		return
	}

	tmpl, ok := s.templates["partials/validation-errors.html"]
	if !ok {
		http.Error(w, "Validation errors template not found", http.StatusInternalServerError)
		return
	}

	data := ValidationErrorsData{
		Title:  title,
		Errors: errors,
	}

	w.Header().Set("Content-Type", "text/html")
	// Execute the template using the filename (not the full path)
	if err := tmpl.ExecuteTemplate(w, "validation-errors.html", data); err != nil {
		log.Printf("ERROR: Failed to render validation errors: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Helper functions for templates
func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	default:
		return 0
	}
}

// CalculateTotalPages calculates total pages for pagination
func CalculateTotalPages(total, limit int) int {
	if limit <= 0 {
		return 0
	}
	return (total + limit - 1) / limit
}

// createTemplateFuncs creates the template function map (called once at initialization)
func (s *Server) createTemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"formatSize":     stats.FormatSize,
		"formatBytes":    disk.FormatBytes, // For disk space formatting
		"formatDuration": stats.FormatDuration,
		"formatTimestamp": func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05")
		},
		"base64Encode": func(data string) string {
			// Encode data to base64 for safe embedding in HTML attributes
			return base64.StdEncoding.EncodeToString([]byte(data))
		},
		"formatServiceName": func(service string) string {
			// Map internal service names to proper display names
			switch service {
			case "qbittorrent":
				return "qBittorrent"
			case "plex":
				return "Plex"
			case "sonarr":
				return "Sonarr"
			case "radarr":
				return "Radarr"
			case "stash":
				return "Stash"
			default:
				return service
			}
		},
		"serviceClass": func(service string, variant string) string {
			// Returns CSS class for service-specific styling
			// Variants: "bg", "bg-faded", "bg-gradient", "border", "text", "text-on-bg", "hover"
			validServices := map[string]bool{
				"plex": true, "sonarr": true, "radarr": true,
				"qbittorrent": true, "stash": true,
			}
			if !validServices[service] {
				return "" // Invalid service, return empty string
			}

			// Handle compound variants (bg-faded, bg-gradient, text-on-bg)
			// Need to rearrange from "bg-gradient" to "bg-service-{service}-gradient"
			// or from "text-on-bg" to "text-on-service-{service}"
			if strings.Contains(variant, "-") {
				parts := strings.Split(variant, "-")
				if len(parts) == 2 {
					// bg-gradient -> bg-service-plex-gradient
					// bg-faded -> bg-service-plex-faded
					return fmt.Sprintf("%s-service-%s-%s", parts[0], service, parts[1])
				} else if len(parts) == 3 && variant == "text-on-bg" {
					// text-on-bg -> text-on-service-plex
					return fmt.Sprintf("text-on-service-%s", service)
				}
			}

			// Simple variants: border, text, bg, hover
			return fmt.Sprintf("%s-service-%s", variant, service)
		},
		"add": func(a, b interface{}) int64 {
			return int64(toFloat64(a)) + int64(toFloat64(b))
		},
		"sub": func(a, b interface{}) int64 {
			return int64(toFloat64(a)) - int64(toFloat64(b))
		},
		"mul": func(a, b interface{}) float64 {
			return toFloat64(a) * toFloat64(b)
		},
		"mulInt": func(a, b interface{}) int64 {
			return int64(toFloat64(a) * toFloat64(b))
		},
		"div": func(a, b interface{}) float64 {
			fb := toFloat64(b)
			if fb == 0 {
				return 0
			}
			return toFloat64(a) / fb
		},
		"join": strings.Join,
		// Note: Using Go's built-in len function instead of custom override
		"toInt64": func(v interface{}) int64 {
			switch val := v.(type) {
			case float64:
				return int64(val)
			case float32:
				return int64(val)
			case int64:
				return val
			case int:
				return int64(val)
			default:
				return 0
			}
		},
		"formatNumber": func(n int64) string {
			// Format integer with thousand separators
			str := fmt.Sprintf("%d", n)
			if len(str) <= 3 {
				return str
			}

			// Add commas from right to left
			var result string
			for i, digit := range str {
				if i > 0 && (len(str)-i)%3 == 0 {
					result += ","
				}
				result += string(digit)
			}
			return result
		},
		"sequence": func(start, end int) []int {
			// Generate a sequence of integers from start to end (inclusive)
			if start > end {
				return []int{}
			}
			result := make([]int, end-start+1)
			for i := range result {
				result[i] = start + i
			}
			return result
		},
		"hashLevelName": func(level int) string {
			// Convert hash level to display name
			return scanner.GetLevelName(level)
		},
		"maxProgressiveLevel": func(fileSize int64) int {
			// Calculate maximum progressive level for file size
			// Files can only be hashed up to their size:
			// - Level 1 (1MB): files > 1MB
			// - Level 2 (10MB): files > 10MB
			// - Level 3 (100MB): files > 100MB
			// - Level 4 (1GB): files > 1GB
			// - Level 5 (10GB): files > 10GB
			// - Level 6 (full): any size
			const (
				MB = 1024 * 1024
				GB = MB * 1024
			)

			if fileSize > 10*GB {
				return 5 // Can reach level 5 (10GB)
			} else if fileSize > 1*GB {
				return 4 // Can reach level 4 (1GB)
			} else if fileSize > 100*MB {
				return 3 // Can reach level 3 (100MB)
			} else if fileSize > 10*MB {
				return 2 // Can reach level 2 (10MB)
			} else if fileSize > 1*MB {
				return 1 // Can reach level 1 (1MB)
			}
			return 1 // Minimum level for very small files
		},
	}
}

// Hash Scanning Handlers

// HandleStartHashScan starts a hash scanning operation
func (s *Server) HandleStartHashScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if s.hashScanner == nil {
		respondError(w, http.StatusBadRequest, "Hash scanning is disabled in configuration", "hash_disabled")
		return
	}

	// Check if a hash scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil && currentScan.ScanType == "hash_scan" {
		respondError(w, http.StatusConflict, "A hash scan is already running", "hash_scan_already_running")
		return
	}

	// Start hash scan in background
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 48*time.Hour) // Hash scans can take a while
		defer cancel()

		minSize := s.config.DuplicateDetection.MinFileSize
		maxSize := s.config.DuplicateDetection.MaxFileSize
		if err := s.hashScanner.Start(ctx, minSize, maxSize); err != nil {
			log.Printf("ERROR: Hash scan failed: %v", err)
		} else {
			log.Printf("INFO: Hash scan completed successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Hash scan started successfully")
	w.Header().Set("X-Toast-Type", "info")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": "Hash scan started",
	})
}

// HandleCancelHashScan cancels the current hash scan
func (s *Server) HandleCancelHashScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if s.hashScanner == nil {
		respondError(w, http.StatusBadRequest, "Hash scanning is disabled", "hash_disabled")
		return
	}

	if s.hashScanner.Cancel() {
		w.Header().Set("X-Toast-Message", "Hash scan cancelled")
		w.Header().Set("X-Toast-Type", "info")
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"status":  "success",
			"message": "Hash scan cancelled",
		})
	} else {
		respondError(w, http.StatusBadRequest, "No hash scan is running", "no_hash_scan")
	}
}

// HandleHashProgress streams hash scan progress via Server-Sent Events
func (s *Server) HandleHashProgress(w http.ResponseWriter, r *http.Request) {
	if s.hashScanner == nil {
		respondError(w, http.StatusBadRequest, "Hash scanning is disabled", "hash_disabled")
		return
	}

	progress := s.hashScanner.GetProgress()
	if progress == nil {
		respondError(w, http.StatusNotFound, "No hash scan in progress", "no_hash_scan")
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		respondError(w, http.StatusInternalServerError, "Streaming not supported", "streaming_error")
		return
	}

	// Subscribe to log messages
	logChan := progress.Subscribe()
	defer progress.Unsubscribe(logChan)

	// Create a ticker for periodic progress updates
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-logChan:
			if !ok {
				return // Channel closed
			}
			// Send log message
			fmt.Fprintf(w, "event: log\ndata: %s\n\n", msg)
			flusher.Flush()
		case <-ticker.C:
			snapshot := progress.GetSnapshot()

			// If scan is no longer running, send final update and exit
			if !snapshot.IsRunning {
				data, _ := json.Marshal(snapshot)
				fmt.Fprintf(w, "event: progress\ndata: %s\n\n", data)
				fmt.Fprintf(w, "event: complete\ndata: {\"status\":\"completed\"}\n\n")
				flusher.Flush()
				return
			}

			// Send progress update
			data, _ := json.Marshal(snapshot)
			fmt.Fprintf(w, "event: progress\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// HandleHashProgressHTML returns HTML fragment for hash scan progress
func (s *Server) HandleHashProgressHTML(w http.ResponseWriter, r *http.Request) {
	if s.hashScanner == nil {
		// Hash scanning disabled - show nothing
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(""))
		return
	}

	progress := s.hashScanner.GetProgress()
	if progress == nil || !progress.IsRunning {
		// No hash scan running
		hashedCount, _ := s.db.GetHashedFileCount()
		totalCount, _ := s.db.GetTotalHashableFileCount(s.config.DuplicateDetection.MinFileSize, s.config.DuplicateDetection.MaxFileSize)
		quickDupCount, _ := s.db.GetQuickHashDuplicateCount()
		quickHashCount, _ := s.db.GetQuickHashCount()

		tmpl := `
		<div class="text-gray-400 space-y-2">
			<p>No hash scan running</p>
			<div class="text-sm space-y-1">
				<p>Files hashed: {{.HashedCount}} / {{.TotalCount}}</p>
				<p>Hash mode: <span class="text-purple-400 font-medium">{{.HashMode}}</span></p>
				{{if gt .QuickHashCount 0}}
				<p class="text-blue-400">
					📝 {{.QuickHashCount}} files with quick hashes
				</p>
				{{end}}
				{{if gt .QuickDupCount 0}}
				<p class="text-yellow-400">
					⚠️ {{.QuickDupCount}} files with quick-hash duplicates need verification
				</p>
				{{end}}
			</div>
		</div>
		`

		hashModeText := "Unknown"
		switch s.config.DuplicateDetection.HashMode {
		case "full":
			hashModeText = "Full File Hashing"
		case "quick_manual":
			hashModeText = "Quick Hash (Manual Verify)"
		case "quick_auto":
			hashModeText = "Quick Hash (Auto Verify)"
		}

		t := template.Must(template.New("hash-idle").Parse(tmpl))
		t.Execute(w, map[string]interface{}{
			"HashedCount":    hashedCount,
			"TotalCount":     totalCount,
			"QuickDupCount":  quickDupCount,
			"QuickHashCount": quickHashCount,
			"HashMode":       hashModeText,
		})
		return
	}

	snapshot := progress.GetSnapshot()

	tmpl := `
	<div class="space-y-3">
		<div class="flex justify-between items-center">
			<div>
				<div class="text-lg font-semibold">Hash Scan: {{.CurrentPhase}}</div>
				<div class="text-sm text-gray-400">{{.ProcessedFiles}} / {{.TotalFiles}} files ({{printf "%.1f" .PercentComplete}}%)</div>
			</div>
			<button
				hx-post="/api/hash/cancel"
				hx-swap="none"
				class="px-3 py-1 bg-red-600 hover:bg-red-700 rounded text-sm">
				Cancel
			</button>
		</div>

		<!-- Progress Bar -->
		<div class="w-full bg-gray-700 rounded-full h-2.5">
			<div class="bg-purple-600 h-2.5 rounded-full transition-all duration-300"
				 style="width: {{printf "%.1f" .PercentComplete}}%"></div>
		</div>

		<!-- Stats -->
		<div class="grid grid-cols-3 gap-4 text-sm">
			<div>
				<div class="text-gray-400">Elapsed</div>
				<div class="font-semibold">{{formatDuration .Elapsed}}</div>
			</div>
			<div>
				<div class="text-gray-400">ETA</div>
				<div class="font-semibold">{{if gt .ETA 0}}{{formatDuration .ETA}}{{else}}-{{end}}</div>
			</div>
			<div>
				<div class="text-gray-400">Errors</div>
				<div class="font-semibold {{if gt .ErrorCount 0}}text-red-400{{end}}">{{.ErrorCount}}</div>
			</div>
		</div>
	</div>
	`

	funcMap := template.FuncMap{
		"formatDuration": stats.FormatDuration,
	}

	t := template.Must(template.New("hash-progress").Funcs(funcMap).Parse(tmpl))
	t.Execute(w, map[string]interface{}{
		"CurrentPhase":    snapshot.CurrentPhase,
		"ProcessedFiles":  snapshot.ProcessedFiles,
		"TotalFiles":      snapshot.TotalFiles,
		"PercentComplete": snapshot.PercentComplete,
		"Elapsed":         snapshot.Elapsed,
		"ETA":             snapshot.ETA,
		"ErrorCount":      snapshot.ErrorCount,
	})
}

// HandleClearHashes clears all hash data from the database
func (s *Server) HandleClearHashes(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodDelete) {
		return
	}

	if s.hashScanner == nil {
		respondError(w, http.StatusBadRequest, "Hash scanning is disabled", "hash_disabled")
		return
	}

	// Check if hash scan is running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil && currentScan.ScanType == "hash_scan" {
		respondError(w, http.StatusConflict, "Cannot clear hashes while scan is running", "hash_scan_running")
		return
	}

	if err := s.db.ClearAllHashes(); err != nil {
		log.Printf("Failed to clear hashes: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to clear hashes", "clear_failed")
		return
	}

	w.Header().Set("X-Toast-Message", "All hashes cleared successfully")
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": "All hashes cleared",
	})
}

// HandleVerifyDuplicates starts verification of quick-hash duplicates (full hash them)
func (s *Server) HandleVerifyDuplicates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.hashScanner == nil {
		http.Error(w, "Hash scanner not initialized", http.StatusInternalServerError)
		return
	}

	// Start verification in background
	go func() {
		ctx := context.Background()
		minSize := s.config.DuplicateDetection.MinFileSize
		maxSize := s.config.DuplicateDetection.MaxFileSize
		if err := s.hashScanner.VerifyDuplicates(ctx, minSize, maxSize); err != nil {
			log.Printf("Verification error: %v", err)
		}
	}()

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": "Duplicate verification started",
	})
}

// HandleVerifyDuplicatesProgressive progressively verifies duplicates by upgrading hash levels
func (s *Server) HandleVerifyDuplicatesProgressive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.hashScanner == nil {
		http.Error(w, "Hash scanner not initialized", http.StatusInternalServerError)
		return
	}

	// Start progressive verification in background
	go func() {
		ctx := context.Background()
		minSize := s.config.DuplicateDetection.MinFileSize
		maxSize := s.config.DuplicateDetection.MaxFileSize
		if err := s.hashScanner.VerifyDuplicatesProgressive(ctx, minSize, maxSize); err != nil {
			log.Printf("Progressive verification error: %v", err)
		}
	}()

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": "Progressive verification started",
	})
}

// HandleGetHashLevelStats returns statistics about duplicates at each hash level
func (s *Server) HandleGetHashLevelStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats, err := s.db.GetHashLevelStats()
	if err != nil {
		log.Printf("Failed to get hash level stats: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to get hash level stats", "stats_error")
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status": "success",
		"stats":  stats,
	})
}

// HandleUpgradeAllHashes upgrades all quick hashes to full hashes
func (s *Server) HandleUpgradeAllHashes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.hashScanner == nil {
		http.Error(w, "Hash scanner not initialized", http.StatusInternalServerError)
		return
	}

	// Start upgrade in background
	go func() {
		ctx := context.Background()
		minSize := s.config.DuplicateDetection.MinFileSize
		maxSize := s.config.DuplicateDetection.MaxFileSize
		if err := s.hashScanner.UpgradeAllQuickHashes(ctx, minSize, maxSize); err != nil {
			log.Printf("Hash upgrade error: %v", err)
		}
	}()

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": "Hash upgrade started",
	})
}

// HandleUpgradeGroupToFullHash upgrades all files in a duplicate group to full hash (level 6)
func (s *Server) HandleUpgradeGroupToFullHash(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.hashScanner == nil {
		respondError(w, http.StatusBadRequest, "Hash scanning is disabled in configuration", "hash_disabled")
		return
	}

	// Get group hash from query parameter
	groupHash := r.URL.Query().Get("group_hash")
	if groupHash == "" {
		respondError(w, http.StatusBadRequest, "Missing group_hash parameter", "missing_parameter")
		return
	}

	// Query files with hash information
	query := `
		SELECT id, path, size, hash_level
		FROM files
		WHERE file_hash = ? AND hash_level < 6
	`

	rows, err := s.db.Conn().Query(query, groupHash)
	if err != nil {
		log.Printf("Error querying files for group %s: %v", groupHash, err)
		respondError(w, http.StatusInternalServerError, "Failed to get files for group", "database_error")
		return
	}
	defer rows.Close()

	type fileInfo struct {
		ID        int64
		Path      string
		Size      int64
		HashLevel int
	}

	var filesToUpgrade []fileInfo
	for rows.Next() {
		var f fileInfo
		if err := rows.Scan(&f.ID, &f.Path, &f.Size, &f.HashLevel); err != nil {
			log.Printf("Error scanning file row: %v", err)
			continue
		}
		filesToUpgrade = append(filesToUpgrade, f)
	}

	if len(filesToUpgrade) == 0 {
		w.Header().Set("X-Toast-Message", "All files already have full hash")
		w.Header().Set("X-Toast-Type", "info")
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"status":   "success",
			"message":  "All files already have full hash",
			"upgraded": 0,
		})
		return
	}

	// Upgrade files in background
	go func() {
		// Create file hasher
		bufferSize := 4 * 1024 * 1024 // 4MB default
		if s.config.DuplicateDetection.HashBufferSize != "" {
			if size, err := disk.ParseSize(s.config.DuplicateDetection.HashBufferSize); err == nil {
				bufferSize = int(size)
			}
		}
		hasher := scanner.NewFileHasher(s.config.DuplicateDetection.HashAlgorithm, bufferSize)

		upgraded := 0
		for _, file := range filesToUpgrade {
			// Calculate full hash
			hash, err := hasher.FullHash(file.Path)
			if err != nil {
				log.Printf("Error calculating full hash for %s: %v", file.Path, err)
				continue
			}

			// Update database with level 6 (full hash)
			if err := s.db.UpdateFileHashWithLevel(file.ID, hash, s.config.DuplicateDetection.HashAlgorithm, 6); err != nil {
				log.Printf("Error updating hash for %s: %v", file.Path, err)
				continue
			}

			upgraded++
		}

		log.Printf("Upgraded %d/%d files in group %s to full hash", upgraded, len(filesToUpgrade), groupHash)
	}()

	w.Header().Set("X-Toast-Message", fmt.Sprintf("Upgrading %d file(s) to full hash", len(filesToUpgrade)))
	w.Header().Set("X-Toast-Type", "info")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": fmt.Sprintf("Upgrading %d files to full hash", len(filesToUpgrade)),
		"count":   len(filesToUpgrade),
	})
}

// HandleUpgradeGroupProgressive upgrades all files in a duplicate group progressively through hash levels
func (s *Server) HandleUpgradeGroupProgressive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.hashScanner == nil {
		respondError(w, http.StatusBadRequest, "Hash scanning is disabled in configuration", "hash_disabled")
		return
	}

	// Get group hash from query parameter
	groupHash := r.URL.Query().Get("group_hash")
	if groupHash == "" {
		respondError(w, http.StatusBadRequest, "Missing group_hash parameter", "missing_parameter")
		return
	}

	// Query files with hash information
	query := `
		SELECT id, path, size, hash_level
		FROM files
		WHERE file_hash = ? AND hash_level < 6
	`

	rows, err := s.db.Conn().Query(query, groupHash)
	if err != nil {
		log.Printf("Error querying files for group %s: %v", groupHash, err)
		respondError(w, http.StatusInternalServerError, "Failed to get files for group", "database_error")
		return
	}
	defer rows.Close()

	type fileInfo struct {
		ID        int64
		Path      string
		Size      int64
		HashLevel int
	}

	var filesToUpgrade []fileInfo
	for rows.Next() {
		var f fileInfo
		if err := rows.Scan(&f.ID, &f.Path, &f.Size, &f.HashLevel); err != nil {
			log.Printf("Error scanning file row: %v", err)
			continue
		}
		filesToUpgrade = append(filesToUpgrade, f)
	}

	if len(filesToUpgrade) == 0 {
		w.Header().Set("X-Toast-Message", "All files already have full hash")
		w.Header().Set("X-Toast-Type", "info")
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"status":   "success",
			"message":  "All files already have full hash",
			"upgraded": 0,
		})
		return
	}

	// Upgrade files in background
	go func() {
		// Create file hasher
		bufferSize := 4 * 1024 * 1024 // 4MB default
		if s.config.DuplicateDetection.HashBufferSize != "" {
			if size, err := disk.ParseSize(s.config.DuplicateDetection.HashBufferSize); err == nil {
				bufferSize = int(size)
			}
		}
		hasher := scanner.NewFileHasher(s.config.DuplicateDetection.HashAlgorithm, bufferSize)

		upgraded := 0
		for _, file := range filesToUpgrade {
			// Determine next level for this file
			currentLevel := file.HashLevel
			if currentLevel >= 6 {
				continue // Already at full hash
			}

			// Calculate next appropriate level based on file size
			nextLevel := currentLevel + 1
			effectiveLevel := scanner.GetEffectiveLevel(file.Size, nextLevel)

			// If effective level is same as current, skip (file too small for next level)
			if effectiveLevel <= currentLevel {
				// Try jumping to full hash instead
				effectiveLevel = 6
			}

			var hash string
			var err error

			if effectiveLevel == 6 {
				// Full hash
				hash, err = hasher.FullHash(file.Path)
			} else {
				// Progressive hash
				chunkSize := scanner.GetChunkSizeForLevel(effectiveLevel)
				hash, err = hasher.PartialHash(file.Path, file.Size, chunkSize)
			}

			if err != nil {
				log.Printf("Error calculating progressive hash for %s: %v", file.Path, err)
				continue
			}

			// Update database
			if err := s.db.UpdateFileHashWithLevel(file.ID, hash, s.config.DuplicateDetection.HashAlgorithm, effectiveLevel); err != nil {
				log.Printf("Error updating hash for %s: %v", file.Path, err)
				continue
			}

			upgraded++
		}

		log.Printf("Upgraded %d/%d files in group %s progressively", upgraded, len(filesToUpgrade), groupHash)
	}()

	w.Header().Set("X-Toast-Message", "Upgrading group hashes progressively")
	w.Header().Set("X-Toast-Type", "info")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": "Progressive hash upgrade started",
		"count":   len(filesToUpgrade),
	})
}

// HandleDuplicates renders the duplicates page showing cross-disk and same-disk duplicates
func (s *Server) HandleDuplicates(w http.ResponseWriter, r *http.Request) {
	// Get active tab from query parameter (default to same-disk)
	activeTab := r.URL.Query().Get("tab")
	if activeTab == "" {
		activeTab = "same-disk"
	}

	// Check if hash scanning is enabled
	if !s.config.DuplicateDetection.Enabled {
		// Show page with message that hash scanning is disabled
		data := &DuplicatesData{
			Title:               "Duplicate Files",
			Version:             s.version,
			ActiveTab:           activeTab,
			CrossDiskGroups:     []*duplicates.ConsolidationPlan{},
			SameDiskGroups:      []*duplicates.ConsolidationPlan{},
			CrossDiskCount:      0,
			SameDiskCount:       0,
			TotalSavings:        0,
			CrossDiskSavings:    0,
			SameDiskSavings:     0,
			HashScanningEnabled: false,
		}

		s.renderTemplate(w, "duplicates.html", data)
		return
	}

	// Parse filters from URL parameters
	filters := database.DuplicateFilters{
		SearchText: r.URL.Query().Get("search"),
		HashType:   r.URL.Query().Get("hash_type"),
		SortBy:     r.URL.Query().Get("sort"),
		Limit:      25, // Default limit
	}

	// Map "progressive" to "partial" for database query compatibility
	// Progressive hashes at levels 2-5 are stored as hash_type='partial' in the database
	if filters.HashType == "progressive" {
		filters.HashType = "partial"
	}

	// Parse minimum size filter
	if minSizeStr := r.URL.Query().Get("min_size"); minSizeStr != "" {
		switch minSizeStr {
		case "1gb":
			filters.MinSize = 1024 * 1024 * 1024
		case "10gb":
			filters.MinSize = 10 * 1024 * 1024 * 1024
		case "100gb":
			filters.MinSize = 100 * 1024 * 1024 * 1024
		case "1tb":
			filters.MinSize = 1024 * 1024 * 1024 * 1024
		}
	}

	// Parse hash level filter (specific progression level)
	if hashLevelStr := r.URL.Query().Get("hash_level"); hashLevelStr != "" {
		if level, err := strconv.Atoi(hashLevelStr); err == nil && level >= 0 && level <= 6 {
			filters.HashLevel = level
		}
	}

	// Parse pagination
	page := 1
	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}

	// Parse limit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			filters.Limit = l
		}
	}

	// Calculate offset for pagination
	filters.Offset = (page - 1) * filters.Limit

	// Get actual totals from database (not limited)
	duplicateStats, err := s.db.GetDuplicateStats()
	if err != nil {
		log.Printf("ERROR: Failed to get duplicate stats: %v", err)
		duplicateStats = &database.DuplicateStats{}
	}

	// Create analyzer
	analyzer := duplicates.NewAnalyzer(s.db, s.diskDetector, &s.config.DuplicateConsolidation)

	// Get cross-disk duplicates with filters and pagination
	crossDiskPlans, err := analyzer.AnalyzeCrossDiskDuplicates(filters)
	if err != nil {
		log.Printf("ERROR: Failed to analyze cross-disk duplicates: %v", err)
		crossDiskPlans = []*duplicates.ConsolidationPlan{}
	}

	// Get same-disk duplicates with filters and pagination
	sameDiskPlans, err := analyzer.AnalyzeSameDiskDuplicates(filters)
	if err != nil {
		log.Printf("ERROR: Failed to analyze same-disk duplicates: %v", err)
		sameDiskPlans = []*duplicates.ConsolidationPlan{}
	}

	// Get total count for pagination (both tabs now support it)
	var total int64
	var totalPages int
	if activeTab == "same-disk" {
		total, err = s.db.GetSameDiskDuplicateCount(filters)
		if err != nil {
			log.Printf("ERROR: Failed to get same-disk duplicate count: %v", err)
			total = int64(len(sameDiskPlans))
		}
		totalPages = int((total + int64(filters.Limit) - 1) / int64(filters.Limit))
	} else {
		total, err = s.db.GetCrossDiskDuplicateCount(filters)
		if err != nil {
			log.Printf("ERROR: Failed to get cross-disk duplicate count: %v", err)
			total = int64(len(crossDiskPlans))
		}
		totalPages = int((total + int64(filters.Limit) - 1) / int64(filters.Limit))
	}

	// Count files in displayed plans
	crossDiskFilesToDelete := 0
	for _, plan := range crossDiskPlans {
		if plan != nil && plan.DeleteFiles != nil {
			crossDiskFilesToDelete += len(plan.DeleteFiles)
		}
	}

	sameDiskFilesToLink := 0
	for _, plan := range sameDiskPlans {
		if plan != nil && plan.DeleteFiles != nil {
			sameDiskFilesToLink += len(plan.DeleteFiles)
		}
	}

	// Prepare template data
	data := &DuplicatesData{
		Title:                  "Duplicate Files",
		Version:                s.version,
		ActiveTab:              activeTab,
		CrossDiskGroups:        crossDiskPlans,
		SameDiskGroups:         sameDiskPlans,
		CrossDiskCount:         duplicateStats.CrossDiskGroups,
		SameDiskCount:          duplicateStats.SameDiskGroups,
		TotalSavings:           duplicateStats.TotalPotentialSavings,
		CrossDiskSavings:       duplicateStats.CrossDiskPotentialSavings,
		SameDiskSavings:        duplicateStats.SameDiskPotentialSavings,
		CrossDiskFilesToDelete: crossDiskFilesToDelete,
		SameDiskFilesToLink:    sameDiskFilesToLink,
		HashScanningEnabled:    true,
		DisplayLimit:           filters.Limit,
		ShowingCrossDisk:       len(crossDiskPlans),
		ShowingSameDisk:        len(sameDiskPlans),
		// Pagination fields
		Page:       page,
		TotalPages: totalPages,
		Total:      total,
		Limit:      filters.Limit,
		Filters:    filters,
	}

	// Check if this is an HTMX request (for pagination/filtering)
	isHTMX := r.Header.Get("HX-Request") == "true"

	if isHTMX {
		// Return just the table fragment for HTMX updates
		tmplSet, ok := s.templates["duplicates_table.html"]
		if !ok {
			log.Printf("ERROR: duplicates_table template not found")
			http.Error(w, "Template not found", http.StatusInternalServerError)
			return
		}

		if err := tmplSet.ExecuteTemplate(w, "duplicates_table.html", data); err != nil {
			log.Printf("ERROR: Failed to render duplicates_table template: %v", err)
			http.Error(w, "Failed to render template", http.StatusInternalServerError)
			return
		}
	} else {
		// Return full page
		s.renderTemplate(w, "duplicates.html", data)
	}
}

// HandleDuplicateGroupCount returns the count of duplicate groups matching the provided filters
func (s *Server) HandleDuplicateGroupCount(w http.ResponseWriter, r *http.Request) {
	// Parse type parameter (same-disk or cross-disk)
	duplicateType := r.URL.Query().Get("type")
	if duplicateType == "" {
		duplicateType = "same-disk" // Default to same-disk
	}

	// Parse filters from URL parameters (same logic as HandleDuplicates)
	filters := database.DuplicateFilters{
		SearchText: r.URL.Query().Get("search"),
		HashType:   r.URL.Query().Get("hash_type"),
		SortBy:     r.URL.Query().Get("sort"),
		Limit:      0, // No limit for count query
	}

	// Map "progressive" to "partial" for database query compatibility
	// Progressive hashes at levels 2-5 are stored as hash_type='partial' in the database
	if filters.HashType == "progressive" {
		filters.HashType = "partial"
	}

	// Parse minimum size filter
	if minSizeStr := r.URL.Query().Get("min_size"); minSizeStr != "" {
		switch minSizeStr {
		case "1gb":
			filters.MinSize = 1024 * 1024 * 1024
		case "10gb":
			filters.MinSize = 10 * 1024 * 1024 * 1024
		case "100gb":
			filters.MinSize = 100 * 1024 * 1024 * 1024
		case "1tb":
			filters.MinSize = 1024 * 1024 * 1024 * 1024
		}
	}

	// Parse hash level filter (specific progression level)
	if hashLevelStr := r.URL.Query().Get("hash_level"); hashLevelStr != "" {
		if level, err := strconv.Atoi(hashLevelStr); err == nil && level >= 0 && level <= 6 {
			filters.HashLevel = level
		}
	}

	// Get count based on type
	var count int64
	var err error

	if duplicateType == "cross-disk" {
		count, err = s.db.GetCrossDiskDuplicateCount(filters)
	} else {
		count, err = s.db.GetSameDiskDuplicateCount(filters)
	}

	if err != nil {
		log.Printf("ERROR: Failed to get duplicate group count: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to count duplicate groups", "count_failed")
		return
	}

	// Return count as JSON
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"type":  duplicateType,
		"count": count,
	})
}

// HandleConsolidateDuplicates executes cross-disk consolidation
func (s *Server) HandleConsolidateDuplicates(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Parse request body
	var req struct {
		DryRun      bool     `json:"dry_run"`
		GroupHashes []string `json:"group_hashes"` // Optional: specific groups only
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body", "invalid_request")
		return
	}

	// Create analyzer and consolidator
	analyzer := duplicates.NewAnalyzer(s.db, s.diskDetector, &s.config.DuplicateConsolidation)

	// Parse buffer size from config
	bufferSize := 4 * 1024 * 1024 // Default 4MB
	if s.config.DuplicateDetection.HashBufferSize != "" {
		if size, err := disk.ParseSize(s.config.DuplicateDetection.HashBufferSize); err == nil {
			bufferSize = int(size)
		}
	}

	// Create hasher for verification
	hasher := scanner.NewFileHasher(s.config.DuplicateDetection.HashAlgorithm, bufferSize)
	consolidator := duplicates.NewConsolidator(s.db, &s.config.DuplicateConsolidation, hasher)

	// Get all cross-disk duplicates (large limit, needed for consolidation)
	filters := database.DuplicateFilters{
		Limit: 10000, // Large limit to get all groups
	}
	plans, err := analyzer.AnalyzeCrossDiskDuplicates(filters)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to analyze duplicates", "analysis_failed")
		return
	}

	// Filter to specific groups if requested
	if len(req.GroupHashes) > 0 {
		hashSet := make(map[string]bool)
		for _, h := range req.GroupHashes {
			hashSet[h] = true
		}

		filteredPlans := make([]*duplicates.ConsolidationPlan, 0)
		for _, plan := range plans {
			if hashSet[plan.Group.FileHash] {
				filteredPlans = append(filteredPlans, plan)
			}
		}
		plans = filteredPlans
	}

	// Execute consolidation
	result, err := consolidator.ConsolidateCrossDisk(plans, req.DryRun)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Consolidation failed", "consolidation_failed")
		return
	}

	// Invalidate stats cache
	s.statsCache.Invalidate()

	// Return result
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":           "success",
		"dry_run":          result.DryRun,
		"groups_processed": result.GroupsProcessed,
		"files_deleted":    result.FilesDeleted,
		"space_freed":      result.SpaceFreed,
		"errors":           result.Errors,
	})
}

// HandleCreateHardlinks creates hardlinks for same-disk duplicates
func (s *Server) HandleCreateHardlinks(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Parse request body
	var req struct {
		DryRun      bool     `json:"dry_run"`
		GroupHashes []string `json:"group_hashes"` // Optional: specific groups only
		// Filter parameters (optional)
		SearchText string `json:"search_text"`
		HashType   string `json:"hash_type"`
		MinSize    int64  `json:"min_size"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body", "invalid_request")
		return
	}

	// Log the request details for debugging
	log.Printf("HandleCreateHardlinks called: dry_run=%v, group_hashes=%v (count: %d), filters=(search:%s, hash_type:%s, min_size:%d)",
		req.DryRun, req.GroupHashes, len(req.GroupHashes), req.SearchText, req.HashType, req.MinSize)

	// Create analyzer and consolidator
	analyzer := duplicates.NewAnalyzer(s.db, s.diskDetector, &s.config.DuplicateConsolidation)

	// Parse buffer size from config
	bufferSize := 4 * 1024 * 1024 // Default 4MB
	if s.config.DuplicateDetection.HashBufferSize != "" {
		if size, err := disk.ParseSize(s.config.DuplicateDetection.HashBufferSize); err == nil {
			bufferSize = int(size)
		}
	}

	// Create hasher for verification
	hasher := scanner.NewFileHasher(s.config.DuplicateDetection.HashAlgorithm, bufferSize)
	consolidator := duplicates.NewConsolidator(s.db, &s.config.DuplicateConsolidation, hasher)

	// Build filters from request
	filters := database.DuplicateFilters{
		SearchText: req.SearchText,
		HashType:   req.HashType,
		MinSize:    req.MinSize,
		Limit:      10000, // Large limit to get all matching groups
	}

	// Map "progressive" to "partial" for database query compatibility
	// Progressive hashes at levels 2-5 are stored as hash_type='partial' in the database
	if filters.HashType == "progressive" {
		filters.HashType = "partial"
	}

	// Get same-disk duplicates with filters applied
	log.Printf("Analyzing same-disk duplicates with filters...")
	plans, err := analyzer.AnalyzeSameDiskDuplicates(filters)
	if err != nil {
		log.Printf("ERROR: Failed to analyze duplicates: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to analyze duplicates", "analysis_failed")
		return
	}
	log.Printf("Found %d plans matching filters", len(plans))

	// Filter to specific groups if requested
	if len(req.GroupHashes) > 0 {
		log.Printf("Filtering to specific groups: %v", req.GroupHashes)
		hashSet := make(map[string]bool)
		for _, h := range req.GroupHashes {
			hashSet[h] = true
		}

		filteredPlans := make([]*duplicates.ConsolidationPlan, 0)
		for _, plan := range plans {
			if hashSet[plan.Group.FileHash] {
				filteredPlans = append(filteredPlans, plan)
			}
		}
		plans = filteredPlans
		log.Printf("Filtered to %d plans", len(plans))
	}

	// Calculate detailed statistics before execution (for dry run summary)
	var totalFilesToLink, totalFilesAlreadyLinked, totalClustersNeedingLink, totalClustersAlreadyLinked int
	hashBreakdown := map[string]int{
		"full_hash":        0,
		"progressive_hash": 0,
		"quick_hash":       0,
	}
	var warnings []string

	// Build top groups list (for preview)
	type TopGroup struct {
		Hash          string `json:"hash"`
		FileSize      int64  `json:"file_size"`
		TotalFiles    int    `json:"total_files"`
		Savings       int64  `json:"savings"`
		HashLevel     int    `json:"hash_level"`
		HashLevelName string `json:"hash_level_name"`
	}
	topGroups := make([]TopGroup, 0)

	// Build list of files to be linked (for preview)
	type FileToLink struct {
		Path      string `json:"path"`
		Size      int64  `json:"size"`
		GroupHash string `json:"group_hash"`
	}
	filesToLink := make([]FileToLink, 0)

	log.Printf("Building statistics for %d plans...", len(plans))
	for _, plan := range plans {
		// Count files needing action vs already linked
		for _, cluster := range plan.LinkClusters {
			isAlreadyLinked := false
			for _, linkedCluster := range plan.AlreadyLinked {
				if cluster.Inode == linkedCluster.Inode {
					isAlreadyLinked = true
					break
				}
			}

			if isAlreadyLinked {
				totalFilesAlreadyLinked += len(cluster.Files)
			} else {
				totalFilesToLink += len(cluster.Files)
				// Add files to the list (excluding the keeper file which is first in cluster)
				for i, file := range cluster.Files {
					if i > 0 { // Skip first file (keeper)
						filesToLink = append(filesToLink, FileToLink{
							Path:      file.Path,
							Size:      file.Size,
							GroupHash: plan.Group.FileHash,
						})
					}
				}
			}
		}

		// Count clusters
		totalClustersAlreadyLinked += len(plan.AlreadyLinked)
		for _, cluster := range plan.LinkClusters {
			needsAction := true
			for _, linkedCluster := range plan.AlreadyLinked {
				if cluster.Inode == linkedCluster.Inode {
					needsAction = false
					break
				}
			}
			if needsAction {
				totalClustersNeedingLink++
			}
		}

		// Hash breakdown
		hashLevel := plan.Group.HashLevel
		if hashLevel == 6 {
			hashBreakdown["full_hash"]++
		} else if hashLevel == 1 {
			hashBreakdown["quick_hash"]++
		} else if hashLevel > 1 && hashLevel < 6 {
			hashBreakdown["progressive_hash"]++
		}

		// Add to top groups (we'll sort and limit later)
		hashLevelName := "Unknown"
		if hashLevel == 6 {
			hashLevelName = "Full Hash ✓"
		} else if hashLevel == 1 {
			hashLevelName = "Quick Hash ⚠️"
		} else if hashLevel > 1 && hashLevel < 6 {
			hashLevelName = fmt.Sprintf("Progressive %d 🔄", hashLevel)
		}

		topGroups = append(topGroups, TopGroup{
			Hash:          plan.Group.FileHash,
			FileSize:      plan.Group.TotalSize,
			TotalFiles:    plan.TotalFiles,
			Savings:       plan.SpaceSavings,
			HashLevel:     hashLevel,
			HashLevelName: hashLevelName,
		})
	}

	// Sort top groups by savings (descending) and limit to top 10
	for i := 0; i < len(topGroups); i++ {
		for j := i + 1; j < len(topGroups); j++ {
			if topGroups[j].Savings > topGroups[i].Savings {
				topGroups[i], topGroups[j] = topGroups[j], topGroups[i]
			}
		}
	}
	if len(topGroups) > 10 {
		topGroups = topGroups[:10]
	}

	// Generate warnings
	if hashBreakdown["quick_hash"] > 0 {
		warnings = append(warnings, fmt.Sprintf("%d group(s) use quick hash - verification recommended before linking", hashBreakdown["quick_hash"]))
	}

	log.Printf("Statistics complete. About to execute hardlinks for %d plans (dry_run=%v)...", len(plans), req.DryRun)

	// Execute hardlink creation
	result, err := consolidator.CreateHardlinks(plans, req.DryRun)
	if err != nil {
		log.Printf("ERROR: Hardlink creation failed: %v", err)
		respondError(w, http.StatusInternalServerError, "Hardlink creation failed", "hardlink_failed")
		return
	}

	// Log the result
	log.Printf("Hardlink operation completed: dry_run=%v, groups_processed=%d, files_linked=%d, space_saved=%d, errors=%d",
		result.DryRun, result.GroupsProcessed, result.FilesDeleted, result.SpaceFreed, len(result.Errors))

	// Invalidate stats cache
	s.statsCache.Invalidate()

	// Build active filters description for modal display
	activeFilters := make(map[string]interface{})
	if req.SearchText != "" {
		activeFilters["search"] = req.SearchText
	}
	if req.HashType != "" && req.HashType != "all" {
		activeFilters["hash_type"] = req.HashType
	}
	if req.MinSize > 0 {
		activeFilters["min_size"] = req.MinSize
	}

	// Return enhanced result
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":                  "success",
		"dry_run":                 result.DryRun,
		"groups_processed":        result.GroupsProcessed,
		"files_linked":            result.FilesDeleted, // "Deleted" is used for linked in same-disk
		"files_to_link":           totalFilesToLink,
		"files_already_linked":    totalFilesAlreadyLinked,
		"clusters_needing_link":   totalClustersNeedingLink,
		"clusters_already_linked": totalClustersAlreadyLinked,
		"space_saved":             result.SpaceFreed,
		"hash_breakdown":          hashBreakdown,
		"top_groups":              topGroups,
		"warnings":                warnings,
		"errors":                  result.Errors,
		"files_list":              filesToLink,
		"active_filters":          activeFilters,
	})
}

// HandlePreviewConsolidation generates a preview of consolidation impact
func (s *Server) HandlePreviewConsolidation(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	// Get type parameter (cross-disk or same-disk)
	consolidationType := r.URL.Query().Get("type")
	if consolidationType == "" {
		consolidationType = "cross-disk"
	}

	// Parse filter parameters from query string
	filters := database.DuplicateFilters{
		SearchText: r.URL.Query().Get("search"),
		HashType:   r.URL.Query().Get("hash_type"),
		Limit:      10000, // Large limit to get all matching groups for preview
	}

	// Map "progressive" to "partial" for database query compatibility
	// Progressive hashes at levels 2-5 are stored as hash_type='partial' in the database
	if filters.HashType == "progressive" {
		filters.HashType = "partial"
	}

	// Parse minimum size filter
	if minSizeStr := r.URL.Query().Get("min_size"); minSizeStr != "" {
		switch minSizeStr {
		case "1gb":
			filters.MinSize = 1024 * 1024 * 1024
		case "10gb":
			filters.MinSize = 10 * 1024 * 1024 * 1024
		case "100gb":
			filters.MinSize = 100 * 1024 * 1024 * 1024
		case "1tb":
			filters.MinSize = 1024 * 1024 * 1024 * 1024
		}
	}

	// Parse hash level filter (specific progression level)
	if hashLevelStr := r.URL.Query().Get("hash_level"); hashLevelStr != "" {
		if level, err := strconv.Atoi(hashLevelStr); err == nil && level >= 0 && level <= 6 {
			filters.HashLevel = level
		}
	}

	// Create analyzer
	analyzer := duplicates.NewAnalyzer(s.db, s.diskDetector, &s.config.DuplicateConsolidation)

	// Parse buffer size from config
	bufferSize := 4 * 1024 * 1024 // Default 4MB
	if s.config.DuplicateDetection.HashBufferSize != "" {
		if size, err := disk.ParseSize(s.config.DuplicateDetection.HashBufferSize); err == nil {
			bufferSize = int(size)
		}
	}

	// Create hasher and consolidator
	hasher := scanner.NewFileHasher(s.config.DuplicateDetection.HashAlgorithm, bufferSize)
	consolidator := duplicates.NewConsolidator(s.db, &s.config.DuplicateConsolidation, hasher)

	var plans []*duplicates.ConsolidationPlan
	var err error

	// Get duplicates with filters applied
	if consolidationType == "cross-disk" {
		plans, err = analyzer.AnalyzeCrossDiskDuplicates(filters)
	} else {
		plans, err = analyzer.AnalyzeSameDiskDuplicates(filters)
	}

	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to analyze duplicates", "analysis_failed")
		return
	}

	// Generate preview
	preview := consolidator.PreviewConsolidation(plans)

	// Build active filters description for response
	activeFilters := make(map[string]interface{})
	if filters.SearchText != "" {
		activeFilters["search"] = filters.SearchText
	}
	if filters.HashType != "" && filters.HashType != "all" {
		activeFilters["hash_type"] = filters.HashType
	}
	if filters.MinSize > 0 {
		activeFilters["min_size"] = filters.MinSize
	}

	// Return preview data
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"type":                   consolidationType,
		"total_groups":           preview.TotalGroups,
		"total_files_to_process": preview.TotalFilesToDelete,
		"total_space_saved":      preview.TotalSpaceSaved,
		"disk_impacts":           preview.DiskImpacts,
		"active_filters":         activeFilters,
	})
}

// HandleRefreshGroupInodes refreshes inodes from the filesystem for all files in a duplicate group
func (s *Server) HandleRefreshGroupInodes(w http.ResponseWriter, r *http.Request) {
	// Support both GET and POST
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "Method not allowed", "method_not_allowed")
		return
	}

	// Get group hash parameter
	groupHash := r.URL.Query().Get("group_hash")
	if groupHash == "" {
		respondError(w, http.StatusBadRequest, "Missing group_hash parameter", "missing_parameter")
		return
	}

	// Get all files in the duplicate group
	files, err := s.db.GetDuplicateFilesByHash(groupHash)
	if err != nil {
		log.Printf("ERROR: Failed to get files for group %s: %v", groupHash, err)
		respondError(w, http.StatusInternalServerError, "Failed to get duplicate group", "query_failed")
		return
	}

	if len(files) == 0 {
		respondError(w, http.StatusNotFound, "Duplicate group not found", "not_found")
		return
	}

	// Refresh inode for each file
	updated := 0
	errors := []string{}

	for _, file := range files {
		// Stat the file to get current inode/device
		var stat syscall.Stat_t
		if err := syscall.Stat(file.Path, &stat); err != nil {
			errMsg := fmt.Sprintf("Failed to stat %s: %v", file.Path, err)
			log.Printf("WARNING: %s", errMsg)
			errors = append(errors, errMsg)
			continue
		}

		// Update database with current inode
		if err := s.db.UpdateFileInode(file.Path, uint64(stat.Dev), uint64(stat.Ino)); err != nil {
			errMsg := fmt.Sprintf("Failed to update database for %s: %v", file.Path, err)
			log.Printf("WARNING: %s", errMsg)
			errors = append(errors, errMsg)
			continue
		}

		updated++
	}

	log.Printf("Refreshed inodes for group %s: %d updated, %d errors", groupHash, updated, len(errors))

	// Return result
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"updated": updated,
		"total":   len(files),
		"errors":  errors,
	})
}

// HandleGetMissingFiles returns missing files from the latest scan as JSON
func (s *Server) HandleGetMissingFiles(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	ctx := r.Context()
	missingFiles, err := s.db.GetLatestMissingFiles(ctx)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to retrieve missing files", "query_failed")
		return
	}

	// Group by service for easier rendering
	type ServiceMissingFiles struct {
		Service string                    `json:"service"`
		Count   int                       `json:"count"`
		Files   []*database.MissingFile   `json:"files"`
	}

	groupedByService := make(map[string]*ServiceMissingFiles)
	for _, mf := range missingFiles {
		if _, exists := groupedByService[mf.Service]; !exists {
			groupedByService[mf.Service] = &ServiceMissingFiles{
				Service: mf.Service,
				Files:   []*database.MissingFile{},
			}
		}
		groupedByService[mf.Service].Files = append(groupedByService[mf.Service].Files, mf)
		groupedByService[mf.Service].Count++
	}

	// Convert map to slice for consistent ordering
	result := make([]*ServiceMissingFiles, 0, len(groupedByService))
	for _, group := range groupedByService {
		result = append(result, group)
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"total":    len(missingFiles),
		"services": result,
	})
}

// HandleExportMissingFiles exports missing files as CSV
func (s *Server) HandleExportMissingFiles(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	ctx := r.Context()
	missingFiles, err := s.db.GetLatestMissingFiles(ctx)
	if err != nil {
		http.Error(w, "Failed to retrieve missing files", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=missing_files.csv")

	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	// Write CSV header
	header := []string{
		"Service",
		"Service Path",
		"Translated Path",
		"Size (Bytes)",
		"Size (Human)",
		"Grouping Name",
		"Grouping ID",
	}
	if err := csvWriter.Write(header); err != nil {
		http.Error(w, "Failed to write CSV header", http.StatusInternalServerError)
		return
	}

	// Write data rows
	for _, mf := range missingFiles {
		// Format size in human-readable format
		sizeHuman := disk.FormatBytes(mf.Size)

		record := []string{
			mf.Service,
			mf.ServicePath,
			mf.TranslatedPath,
			fmt.Sprintf("%d", mf.Size),
			sizeHuman,
			mf.ServiceGroup,
			mf.ServiceGroupID,
		}
		if err := csvWriter.Write(record); err != nil {
			log.Printf("Failed to write CSV record: %v", err)
			continue
		}
	}

	csvWriter.Flush()
}
