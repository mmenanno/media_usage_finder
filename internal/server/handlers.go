package server

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/api"
	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/scanner"
	"github.com/mmenanno/media-usage-finder/internal/stats"
)

// Server holds the application state
type Server struct {
	db            *database.DB
	config        *config.Config
	scanner       *scanner.Scanner
	templates     *template.Template
	statsCache    *stats.Cache
	templateFuncs template.FuncMap // Cached template functions
}

// NewServer creates a new server instance
func NewServer(db *database.DB, cfg *config.Config) *Server {
	cacheTTL := cfg.StatsCacheTTL
	if cacheTTL == 0 {
		cacheTTL = 30 * time.Second // Default fallback
	}

	srv := &Server{
		db:         db,
		config:     cfg,
		statsCache: stats.NewCache(cacheTTL),
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
func (s *Server) LoadTemplates(pattern string) error {
	tmpl, err := template.New("").Funcs(s.templateFuncs).ParseGlob(pattern)
	if err != nil {
		return err
	}
	s.templates = tmpl
	return nil
}

// HandleIndex serves the dashboard page
func (s *Server) HandleIndex(w http.ResponseWriter, r *http.Request) {
	statistics := s.getStats()
	if statistics == nil {
		http.Error(w, "Failed to calculate stats", http.StatusInternalServerError)
		return
	}

	data := map[string]interface{}{
		"Stats": statistics,
		"Title": "Dashboard",
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

	var files []*database.File
	var total int
	var err error

	if search != "" {
		files, total, err = s.db.SearchFiles(search, limit, offset)
	} else {
		files, total, err = s.db.ListFiles(orphanedOnly, service, hardlinksOnly, limit, offset, orderBy)
	}

	if err != nil {
		http.Error(w, "Failed to list files", http.StatusInternalServerError)
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

	data := map[string]interface{}{
		"Files":      filesWithUsage,
		"Total":      total,
		"Page":       page,
		"Limit":      limit,
		"TotalPages": CalculateTotalPages(total, limit),
		"Title":      "Files",
		"Orphaned":   orphanedOnly,
		"Hardlinks":  hardlinksOnly,
		"Service":    service,
		"Search":     search,
	}

	s.renderTemplate(w, "files.html", data)
}

// HandleConfig serves the configuration page
func (s *Server) HandleConfig(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"Config": s.config,
		"Title":  "Configuration",
	}

	s.renderTemplate(w, "config.html", data)
}

// HandleStats serves the statistics page
func (s *Server) HandleStats(w http.ResponseWriter, r *http.Request) {
	statistics := s.getStats()
	if statistics == nil {
		http.Error(w, "Failed to calculate stats", http.StatusInternalServerError)
		return
	}

	data := map[string]interface{}{
		"Stats": statistics,
		"Title": "Statistics",
	}

	s.renderTemplate(w, "stats.html", data)
}

// HardlinkGroup represents a group of hardlinked files
type HardlinkGroup struct {
	Key   string
	Files []*database.File
	Size  int64
}

// HandleHardlinks serves the hardlinks page with pagination
func (s *Server) HandleHardlinks(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	page = ValidatePage(page)

	limit := 50 // Groups per page
	offset := (page - 1) * limit

	groupsMap, err := s.db.GetHardlinkGroups()
	if err != nil {
		http.Error(w, "Failed to get hardlink groups", http.StatusInternalServerError)
		return
	}

	// Convert map to sorted slice for consistent display
	groups := make([]HardlinkGroup, 0, len(groupsMap))
	for key, files := range groupsMap {
		if len(files) > 0 {
			// Use minimum size to handle potential corruption edge cases
			minSize := files[0].Size
			for _, f := range files {
				if f.Size < minSize {
					minSize = f.Size
				}
			}
			groups = append(groups, HardlinkGroup{
				Key:   key,
				Files: files,
				Size:  minSize * int64(len(files)-1), // Space saved
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

	data := map[string]interface{}{
		"Groups":     paginatedGroups,
		"Total":      total,
		"Page":       page,
		"TotalPages": CalculateTotalPages(total, limit),
		"Title":      "Hardlink Groups",
	}

	s.renderTemplate(w, "hardlinks.html", data)
}

// HandleScans serves the scan history page
func (s *Server) HandleScans(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	page = ValidatePage(page)

	limit := 20
	offset := (page - 1) * limit

	scans, total, err := s.db.ListScans(limit, offset)
	if err != nil {
		http.Error(w, "Failed to list scans", http.StatusInternalServerError)
		return
	}

	data := map[string]interface{}{
		"Scans":      scans,
		"Total":      total,
		"Page":       page,
		"TotalPages": CalculateTotalPages(total, limit),
		"Title":      "Scan History",
	}

	s.renderTemplate(w, "scans.html", data)
}

// HandleHealth serves the health check endpoint with detailed status
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":  "healthy",
		"version": "1.0.0", // Could be from build flag
		"checks":  make(map[string]interface{}),
	}

	// Check database
	dbHealth := map[string]string{"status": "ok"}
	if err := s.db.Ping(); err != nil {
		dbHealth["status"] = "error"
		dbHealth["error"] = err.Error()
		health["status"] = "degraded"
	}
	health["checks"].(map[string]interface{})["database"] = dbHealth

	// Check if scan is running
	scanHealth := map[string]interface{}{"status": "ok"}
	if progress := s.scanner.GetProgress(); progress != nil {
		snapshot := progress.GetSnapshot()
		scanHealth["running"] = true
		scanHealth["progress"] = snapshot.PercentComplete
		scanHealth["phase"] = snapshot.CurrentPhase
	} else {
		scanHealth["running"] = false
	}
	health["checks"].(map[string]interface{})["scanner"] = scanHealth

	// Optionally check external services (quick timeout)
	if r.URL.Query().Get("detailed") == "true" {
		health["checks"].(map[string]interface{})["services"] = s.checkExternalServices()
	}

	// Set status code based on health
	statusCode := http.StatusOK
	if health["status"] == "degraded" {
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(health)
}

// checkExternalServices checks connectivity to external services
func (s *Server) checkExternalServices() map[string]interface{} {
	services := make(map[string]interface{})
	timeout := 2 * time.Second

	// Check Plex
	if s.config.Services.Plex.URL != "" {
		plexClient := api.NewPlexClient(s.config.Services.Plex.URL, s.config.Services.Plex.Token, timeout)
		if err := plexClient.Test(); err != nil {
			services["plex"] = map[string]string{"status": "error", "error": err.Error()}
		} else {
			services["plex"] = map[string]string{"status": "ok"}
		}
	}

	// Check Sonarr
	if s.config.Services.Sonarr.URL != "" {
		sonarrClient := api.NewSonarrClient(s.config.Services.Sonarr.URL, s.config.Services.Sonarr.APIKey, timeout)
		if err := sonarrClient.Test(); err != nil {
			services["sonarr"] = map[string]string{"status": "error", "error": err.Error()}
		} else {
			services["sonarr"] = map[string]string{"status": "ok"}
		}
	}

	// Check Radarr
	if s.config.Services.Radarr.URL != "" {
		radarrClient := api.NewRadarrClient(s.config.Services.Radarr.URL, s.config.Services.Radarr.APIKey, timeout)
		if err := radarrClient.Test(); err != nil {
			services["radarr"] = map[string]string{"status": "error", "error": err.Error()}
		} else {
			services["radarr"] = map[string]string{"status": "ok"}
		}
	}

	// Check qBittorrent
	qbConfig := s.config.Services.QBittorrent
	if qbConfig.URL != "" || qbConfig.QuiProxyURL != "" {
		qbClient := api.NewQBittorrentClient(qbConfig.URL, qbConfig.Username, qbConfig.Password, qbConfig.QuiProxyURL, timeout)
		if err := qbClient.Test(); err != nil {
			services["qbittorrent"] = map[string]string{"status": "error", "error": err.Error()}
		} else {
			services["qbittorrent"] = map[string]string{"status": "ok"}
		}
	}

	return services
}

// HandleStartScan starts a new scan
func (s *Server) HandleStartScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "Method not allowed", "method_not_allowed")
		return
	}

	incremental := r.URL.Query().Get("incremental") == "true"

	// Create context with timeout for scan operation
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
		defer cancel()

		if err := s.scanner.Scan(ctx, incremental); err != nil {
			fmt.Printf("Scan error: %v\n", err)
		}
	}()

	w.Header().Set("X-Toast-Message", "Scan started successfully")
	w.Header().Set("X-Toast-Type", "info")
	respondSuccess(w, "Scan started", map[string]interface{}{
		"incremental": incremental,
	})
}

// HandleScanProgress returns the current scan progress
func (s *Server) HandleScanProgress(w http.ResponseWriter, r *http.Request) {
	progress := s.scanner.GetProgress()
	if progress == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"running": false,
		})
		return
	}

	snapshot := progress.GetSnapshot()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snapshot)
}

// HandleScanProgressHTML returns HTML for scan progress (HTMX endpoint)
func (s *Server) HandleScanProgressHTML(w http.ResponseWriter, r *http.Request) {
	progress := s.scanner.GetProgress()
	if progress == nil {
		w.Write([]byte(`<div class="text-gray-400">No scan running</div>`))
		return
	}

	snapshot := progress.GetSnapshot()

	html := fmt.Sprintf(`
		<div class="space-y-2">
			<div class="flex justify-between text-sm">
				<span class="text-gray-400">%s</span>
				<span class="text-gray-300">%.1f%%</span>
			</div>
			<div class="w-full bg-gray-700 rounded-full h-4">
				<div class="bg-blue-600 h-4 rounded-full transition-all duration-300" style="width: %.1f%%"></div>
			</div>
			<div class="flex justify-between text-sm text-gray-400">
				<span>%d / %d files</span>
				<span>ETA: %s</span>
			</div>
		</div>
	`,
		snapshot.CurrentPhase,
		snapshot.PercentComplete,
		snapshot.PercentComplete,
		snapshot.ProcessedFiles,
		snapshot.TotalFiles,
		stats.FormatDuration(snapshot.ETA),
	)

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
}

// HandleScanLogs streams scan logs via SSE
func (s *Server) HandleScanLogs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	// Send initial connection message
	fmt.Fprintf(w, "data: Connected to log stream\n\n")
	flusher.Flush()

	progress := s.scanner.GetProgress()
	if progress == nil {
		fmt.Fprintf(w, "data: No scan currently running\n\n")
		flusher.Flush()
		return
	}

	// Subscribe to log messages
	logChan := progress.Subscribe()
	defer progress.Unsubscribe(logChan)

	// Stream log messages
	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-logChan:
			if !ok {
				// Channel closed, scan finished
				fmt.Fprintf(w, "data: Scan completed\n\n")
				flusher.Flush()
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

// HandleSaveConfig saves configuration
func (s *Server) HandleSaveConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "Method not allowed", "method_not_allowed")
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

	if port := r.FormValue("server_port"); port != "" {
		if p, err := strconv.Atoi(port); err == nil && p > 0 && p <= 65535 {
			s.config.ServerPort = p
		}
	}

	// Update Plex config
	s.config.Services.Plex.URL = r.FormValue("plex_url")
	s.config.Services.Plex.Token = r.FormValue("plex_token")

	// Update Sonarr config
	s.config.Services.Sonarr.URL = r.FormValue("sonarr_url")
	s.config.Services.Sonarr.APIKey = r.FormValue("sonarr_api_key")

	// Update Radarr config
	s.config.Services.Radarr.URL = r.FormValue("radarr_url")
	s.config.Services.Radarr.APIKey = r.FormValue("radarr_api_key")

	// Update qBittorrent config
	s.config.Services.QBittorrent.URL = r.FormValue("qbittorrent_url")
	s.config.Services.QBittorrent.Username = r.FormValue("qbittorrent_username")
	s.config.Services.QBittorrent.Password = r.FormValue("qbittorrent_password")
	s.config.Services.QBittorrent.QuiProxyURL = r.FormValue("qbittorrent_qui_proxy_url")

	// Validate config before saving
	if err := s.config.Validate(); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid configuration: %v", err), "validation_failed")
		return
	}

	// Save config to file
	if err := s.config.Save("/config/config.yaml"); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to save configuration", "save_failed")
		return
	}

	w.Header().Set("X-Toast-Message", "Configuration saved successfully")
	w.Header().Set("X-Toast-Type", "success")
	respondSuccess(w, "Configuration saved", nil)
}

// HandleTestService tests connection to a service
func (s *Server) HandleTestService(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")

	var err error
	switch service {
	case "plex":
		client := api.NewPlexClient(
			s.config.Services.Plex.URL,
			s.config.Services.Plex.Token,
			s.config.APITimeout,
		)
		err = client.Test()
	case "sonarr":
		client := api.NewSonarrClient(
			s.config.Services.Sonarr.URL,
			s.config.Services.Sonarr.APIKey,
			s.config.APITimeout,
		)
		err = client.Test()
	case "radarr":
		client := api.NewRadarrClient(
			s.config.Services.Radarr.URL,
			s.config.Services.Radarr.APIKey,
			s.config.APITimeout,
		)
		err = client.Test()
	case "qbittorrent":
		qbConfig := s.config.Services.QBittorrent
		client := api.NewQBittorrentClient(
			qbConfig.URL,
			qbConfig.Username,
			qbConfig.Password,
			qbConfig.QuiProxyURL,
			s.config.APITimeout,
		)
		err = client.Test()
	default:
		respondError(w, http.StatusBadRequest, "Unknown service", "unknown_service")
		return
	}

	if err != nil {
		w.Header().Set("X-Toast-Message", fmt.Sprintf("%s connection failed: %v", service, err))
		w.Header().Set("X-Toast-Type", "error")
		respondError(w, http.StatusBadRequest, err.Error(), "connection_failed")
		return
	}

	w.Header().Set("X-Toast-Message", fmt.Sprintf("%s connection successful", service))
	w.Header().Set("X-Toast-Type", "success")
	respondSuccess(w, "Connection successful", nil)
}

// HandleExport exports files list
func (s *Server) HandleExport(w http.ResponseWriter, r *http.Request) {
	format := r.URL.Query().Get("format")
	orphanedOnly := r.URL.Query().Get("orphaned") == "true"

	files, _, err := s.db.ListFiles(orphanedOnly, "", false, 100000, 0, "path")
	if err != nil {
		http.Error(w, "Failed to list files", http.StatusInternalServerError)
		return
	}

	switch format {
	case "json":
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=files.json")
		json.NewEncoder(w).Encode(files)
	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=files.csv")
		w.Write([]byte("path,size,is_orphaned\n"))
		for _, file := range files {
			fmt.Fprintf(w, "%s,%d,%v\n", file.Path, file.Size, file.IsOrphaned)
		}
	default:
		http.Error(w, "Invalid format", http.StatusBadRequest)
	}
}

// HandleDeleteFile deletes a file or files
func (s *Server) HandleDeleteFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		respondError(w, http.StatusMethodNotAllowed, "Method not allowed", "method_not_allowed")
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
		files, _, err := s.db.ListFiles(true, "", false, 100000, 0, "path")
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
		respondSuccess(w, "Bulk deletion completed", map[string]interface{}{
			"deleted": deleted,
			"errors":  errors,
		})
		return
	}

	respondError(w, http.StatusBadRequest, "Must specify file ID or orphaned flag", "missing_parameter")
}

// HandleMarkRescan marks files for rescan
func (s *Server) HandleMarkRescan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "Method not allowed", "method_not_allowed")
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
		respondSuccess(w, "File marked for rescan", map[string]interface{}{
			"count": 1,
		})
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
		respondSuccess(w, "Files marked for rescan", map[string]interface{}{
			"count": count,
		})
		return
	}

	respondError(w, http.StatusBadRequest, "Must specify file ID or orphaned flag", "missing_parameter")
}

// renderTemplate renders an HTML template
func (s *Server) renderTemplate(w http.ResponseWriter, name string, data map[string]interface{}) {
	if s.templates == nil {
		http.Error(w, "Templates not loaded", http.StatusInternalServerError)
		return
	}

	if err := s.templates.ExecuteTemplate(w, name, data); err != nil {
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
		"add": func(a, b int) int {
			return a + b
		},
		"sub": func(a, b int) int {
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
