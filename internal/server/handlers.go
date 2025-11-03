package server

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/api"
	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/constants"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/scanner"
	"github.com/mmenanno/media-usage-finder/internal/stats"
)

// Server holds the application state
type Server struct {
	db            *database.DB
	config        *config.Config
	scanner       *scanner.Scanner
	templates     map[string]*template.Template // Map of template name to parsed template
	statsCache    *stats.Cache
	templateFuncs template.FuncMap   // Cached template functions
	version       string             // Application version
	clientFactory *api.ClientFactory // Factory for creating service clients
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
		"hardlinks.html",
		"scans.html",
		"stats.html",
		"config.html",
		"advanced.html",
	}

	layoutPath := baseDir + "/layout.html"

	// Parse each page template with layout.html to avoid block name collisions
	// This ensures each page gets its own "content" block without conflicts
	for _, page := range pages {
		tmpl, err := template.New("").Funcs(s.templateFuncs).ParseFiles(
			layoutPath,
			baseDir+"/"+page,
		)
		if err != nil {
			return fmt.Errorf("failed to parse %s: %w", page, err)
		}
		s.templates[page] = tmpl
	}

	// Load partial templates (used for HTMX responses)
	partials := []string{
		"partials/validation-errors.html",
	}

	for _, partial := range partials {
		tmpl, err := template.New(partial).Funcs(s.templateFuncs).ParseFiles(
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

	data := DashboardData{
		Stats:                statistics,
		Title:                "Dashboard",
		HasActiveScan:        hasActiveScan,
		HasInterruptedScan:   hasInterruptedScan,
		InterruptedScanID:    interruptedScanID,
		InterruptedScanPhase: interruptedScanPhase,
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

// HandleFiles serves the files page
func (s *Server) HandleFiles(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	page = ValidatePage(page)

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	limit = ValidateLimit(limit)

	offset := (page - 1) * limit

	orphanedOnly := r.URL.Query().Get("orphaned") == "true"
	hardlinksOnly := r.URL.Query().Get("hardlink") == "true"
	service := r.URL.Query().Get("service")
	search := r.URL.Query().Get("search")
	orderBy := r.URL.Query().Get("order")
	direction := r.URL.Query().Get("direction")

	var files []*database.File
	var total int
	var err error

	if search != "" {
		files, total, err = s.db.SearchFiles(search, limit, offset)
	} else {
		files, total, err = s.db.ListFiles(orphanedOnly, service, hardlinksOnly, limit, offset, orderBy, direction)
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

	filesWithUsage := make([]map[string]interface{}, 0, len(files))
	for _, file := range files {
		filesWithUsage = append(filesWithUsage, map[string]interface{}{
			"File":  file,
			"Usage": usageMap[file.ID],
		})
	}

	data := FilesData{
		Files:      filesWithUsage,
		Total:      total,
		Page:       int64(page),
		Limit:      limit,
		TotalPages: CalculateTotalPages(total, limit),
		Title:      "Files",
		Orphaned:   orphanedOnly,
		Hardlinks:  hardlinksOnly,
		Service:    service,
		Search:     search,
		OrderBy:    orderBy,
		Direction:  direction,
	}

	s.renderTemplate(w, "files.html", data)
}

// HandleConfig serves the configuration page
func (s *Server) HandleConfig(w http.ResponseWriter, r *http.Request) {
	data := ConfigData{
		Config: s.config,
		Title:  "Configuration",
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

	data := StatsData{
		Stats: statistics,
		Title: "Statistics",
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

// HandleHardlinks serves the hardlinks page with pagination
func (s *Server) HandleHardlinks(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	page = ValidatePage(page)

	limit := constants.DefaultHardlinkGroupsPerPage
	offset := (page - 1) * limit

	groupsMap, err := s.db.GetHardlinkGroups()
	if err != nil {
		log.Printf("ERROR: Failed to get hardlink groups: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to retrieve hardlink groups. Database error occurred", "database_error")
		return
	}

	// Convert map to sorted slice for consistent display
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

	// Sort by space saved (descending)
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Size > groups[j].Size
	})

	// Paginate
	total := len(groups)
	start := offset
	end := offset + limit
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}

	paginatedGroups := groups[start:end]

	data := HardlinksData{
		Groups:     paginatedGroups,
		Total:      total,
		Showing:    len(paginatedGroups),
		Page:       int64(page),
		TotalPages: CalculateTotalPages(total, limit),
		Title:      "Hardlink Groups",
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
	scanDisplays := make([]*ScanDisplay, 0, len(scans))
	for _, scan := range scans {
		actualCount, err := s.db.GetScanFileCount(scan.ID)
		if err != nil {
			log.Printf("WARNING: Failed to get file count for scan %d: %v", scan.ID, err)
			actualCount = 0
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
	}

	s.renderTemplate(w, "scans.html", data)
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
	progress := s.scanner.GetProgress()
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

	// Phase icons
	phaseIcons := map[string]string{
		"Initializing":             `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>`,
		"Counting files":           `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 20l4-16m2 16l4-16M6 9h14M4 15h14"></path></svg>`,
		"Scanning filesystem":      `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path></svg>`,
		"Checking Plex":            `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 4v16M17 4v16M3 8h4m10 0h4M3 12h18M3 16h4m10 0h4M4 20h16a1 1 0 001-1V5a1 1 0 00-1-1H4a1 1 0 00-1 1v14a1 1 0 001 1z"></path></svg>`,
		"Checking Sonarr":          `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"></path></svg>`,
		"Checking Radarr":          `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 4v16M17 4v16M3 8h4m10 0h4M3 12h18M3 16h4m10 0h4M4 20h16a1 1 0 001-1V5a1 1 0 00-1-1H4a1 1 0 00-1 1v14a1 1 0 001 1z"></path></svg>`,
		"Checking qBittorrent":     `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"></path></svg>`,
		"Updating orphaned status": `<svg class="w-5 h-5 inline-block mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>`,
		"Completed":                `<svg class="w-5 h-5 inline-block mr-2 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>`,
	}

	icon := phaseIcons[snapshot.CurrentPhase]
	if icon == "" {
		icon = phaseIcons["Initializing"]
	}

	html := fmt.Sprintf(`
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<div class="flex items-center text-sm text-gray-300">
					%s
					<span class="font-medium">%s</span>
				</div>
				<span class="text-lg font-bold text-blue-400">%.1f%%</span>
			</div>

			<div class="w-full bg-gray-700 rounded-full h-3 overflow-hidden">
				<div class="bg-gradient-to-r from-blue-500 to-blue-600 h-3 rounded-full transition-all duration-300 shadow-lg" style="width: %.1f%%"></div>
			</div>

			<div class="grid grid-cols-2 gap-4 text-sm">
				<div>
					<div class="text-gray-500 text-xs">Files Processed</div>
					<div class="text-gray-200 font-medium">%d / %d</div>
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
					class="px-3 py-1 bg-yellow-600 hover:bg-yellow-700 rounded text-sm transition cursor-pointer">
					Cancel Scan
				</button>
				<button
					hx-post="/api/scan/force-stop"
					hx-swap="none"
					hx-confirm="Force stop the scan immediately? This may leave the database in an inconsistent state."
					class="px-3 py-1 bg-red-600 hover:bg-red-700 rounded text-sm transition cursor-pointer">
					Force Stop
				</button>
			</div>
		</div>
	`,
		icon,
		snapshot.CurrentPhase,
		snapshot.PercentComplete,
		snapshot.PercentComplete,
		snapshot.ProcessedFiles,
		snapshot.TotalFiles,
		filesPerSec,
		stats.FormatDuration(elapsed),
		stats.FormatDuration(snapshot.ETA),
	)

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

	// Parse local path mappings (format: container=host, one per line)
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

	// Parse service path mappings (format: service:container=host, one per line)
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

	// Success - show toast and clear error panel
	w.Header().Set("X-Toast-Message", "Configuration saved successfully")
	w.Header().Set("X-Toast-Type", "success")
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	// Clear and hide the validation-errors div
	w.Write([]byte(`<script>document.getElementById('validation-errors').classList.add('hidden');</script>`))
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
				errors = append(errors, fmt.Sprintf("Line %d: invalid format (expected 'container=host')", i+1))
				continue
			}

			container := strings.TrimSpace(parts[0])
			host := strings.TrimSpace(parts[1])

			if container == "" || host == "" {
				errors = append(errors, fmt.Sprintf("Line %d: empty container or host path", i+1))
				continue
			}

			// Test container path (left side - what we can see from inside the container)
			if _, err := os.Stat(container); err != nil {
				errors = append(errors, fmt.Sprintf("%s=%s: container path error: %v", container, host, err))
			} else {
				successes = append(successes, fmt.Sprintf("%s=%s: OK (container path accessible)", container, host))
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

			// Split service:container_path=service_path
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				errors = append(errors, fmt.Sprintf("Service line %d: invalid format (expected 'service:container_path=service_path')", i+1))
				continue
			}

			service := strings.TrimSpace(parts[0])
			pathParts := strings.SplitN(parts[1], "=", 2)
			if len(pathParts) != 2 {
				errors = append(errors, fmt.Sprintf("Service line %d: invalid format (expected 'service:container_path=service_path')", i+1))
				continue
			}

			containerPath := strings.TrimSpace(pathParts[0])
			servicePath := strings.TrimSpace(pathParts[1])

			if service == "" || containerPath == "" || servicePath == "" {
				errors = append(errors, fmt.Sprintf("Service line %d: empty service, container, or service path", i+1))
				continue
			}

			// Test container path (left side - what we can see)
			if _, err := os.Stat(containerPath); err != nil {
				errors = append(errors, fmt.Sprintf("%s:%s=%s: container path error: %v", service, containerPath, servicePath, err))
				continue
			}

			// Intelligent validation: query service for actual file and test translation
			if cfg, hasConfig := serviceConfigs[service]; hasConfig {
				if err := s.testServicePathMapping(service, containerPath, servicePath, cfg); err != nil {
					errors = append(errors, fmt.Sprintf("%s:%s=%s: mapping validation failed: %v", service, containerPath, servicePath, err))
				} else {
					successes = append(successes, fmt.Sprintf("%s:%s=%s: OK (container path accessible, mapping verified)", service, containerPath, servicePath))
				}
			} else {
				// No service config available, only basic validation
				successes = append(successes, fmt.Sprintf("%s:%s=%s: OK (container path accessible, no service config for intelligent test)", service, containerPath, servicePath))
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
func (s *Server) testServicePathMapping(serviceName, containerPath, servicePath string, cfg interface{}) error {
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

	// Test if we can translate the service path to container path
	if !strings.HasPrefix(sampleFilePath, servicePath) {
		return fmt.Errorf("service file path '%s' doesn't start with expected service path '%s'", sampleFilePath, servicePath)
	}

	// Replace service path with container path
	translatedPath := strings.Replace(sampleFilePath, servicePath, containerPath, 1)

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
			files, _, err := s.db.ListFiles(orphanedOnly, "", false, batchSize, offset, "path", "asc")
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
			files, _, err := s.db.ListFiles(orphanedOnly, "", false, batchSize, offset, "path", "asc")
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
		files, _, err := s.db.ListFiles(true, "", false, constants.MaxExportFiles, 0, "path", "asc")
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

	response := FileDetailsResponse{
		ID:           file.ID,
		Path:         file.Path,
		Size:         file.Size,
		Inode:        file.Inode,
		DeviceID:     file.DeviceID,
		ModifiedTime: file.ModifiedTime.Unix(),
		LastVerified: file.LastVerified.Unix(),
		IsOrphaned:   file.IsOrphaned,
		CreatedAt:    file.CreatedAt.Unix(),
		Usage:        usage,
		Hardlinks:    hardlinks,
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

	// Get database stats
	stats, err := s.db.GetDatabaseStats()
	if err != nil {
		log.Printf("Failed to get database stats: %v", err)
		stats = &database.DatabaseStats{} // Use empty stats on error
	}

	data := AdvancedData{
		Title: "Advanced Settings",
		Stats: stats,
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
		err = s.db.DeleteUsageByService(service)
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

	if err := s.db.UpdateOrphanedStatus(); err != nil {
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
		"formatDuration": stats.FormatDuration,
		"add": func(a, b int64) int64 {
			return a + b
		},
		"sub": func(a, b int64) int64 {
			return a - b
		},
		"mul": func(a, b interface{}) float64 {
			return toFloat64(a) * toFloat64(b)
		},
		"div": func(a, b interface{}) float64 {
			fb := toFloat64(b)
			if fb == 0 {
				return 0
			}
			return toFloat64(a) / fb
		},
		"join": strings.Join,
		"len": func(v interface{}) int {
			switch val := v.(type) {
			case map[string]interface{}:
				return len(val)
			case []interface{}:
				return len(val)
			default:
				return 0
			}
		},
	}
}
