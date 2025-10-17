package server

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
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
	db        *database.DB
	config    *config.Config
	scanner   *scanner.Scanner
	templates *template.Template
}

// NewServer creates a new server instance
func NewServer(db *database.DB, cfg *config.Config) *Server {
	return &Server{
		db:      db,
		config:  cfg,
		scanner: scanner.NewScanner(db, cfg),
	}
}

// LoadTemplates loads HTML templates
func (s *Server) LoadTemplates(pattern string) error {
	tmpl, err := template.New("").Funcs(s.templateFuncs()).ParseGlob(pattern)
	if err != nil {
		return err
	}
	s.templates = tmpl
	return nil
}

// HandleIndex serves the dashboard page
func (s *Server) HandleIndex(w http.ResponseWriter, r *http.Request) {
	calculator := stats.NewCalculator(s.db)
	statistics, err := calculator.Calculate()
	if err != nil {
		http.Error(w, "Failed to calculate stats", http.StatusInternalServerError)
		return
	}

	data := map[string]interface{}{
		"Stats": statistics,
		"Title": "Dashboard",
	}

	s.renderTemplate(w, "dashboard.html", data)
}

// HandleFiles serves the files page
func (s *Server) HandleFiles(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	page = ValidatePage(page)

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	limit = ValidateLimit(limit)

	offset := (page - 1) * limit

	orphanedOnly := r.URL.Query().Get("orphaned") == "true"
	service := r.URL.Query().Get("service")
	search := r.URL.Query().Get("search")
	orderBy := r.URL.Query().Get("order")

	var files []*database.File
	var total int
	var err error

	if search != "" {
		files, total, err = s.db.SearchFiles(search, limit, offset)
	} else {
		files, total, err = s.db.ListFiles(orphanedOnly, service, limit, offset, orderBy)
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
		"TotalPages": (total + limit - 1) / limit,
		"Title":      "Files",
		"Orphaned":   orphanedOnly,
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
	calculator := stats.NewCalculator(s.db)
	statistics, err := calculator.Calculate()
	if err != nil {
		http.Error(w, "Failed to calculate stats", http.StatusInternalServerError)
		return
	}

	data := map[string]interface{}{
		"Stats": statistics,
		"Title": "Statistics",
	}

	s.renderTemplate(w, "stats.html", data)
}

// HandleStartScan starts a new scan
func (s *Server) HandleStartScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "Method not allowed", "method_not_allowed")
		return
	}

	incremental := r.URL.Query().Get("incremental") == "true"

	go func() {
		ctx := context.Background()
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
		formatDuration(snapshot.ETA),
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

	// TODO: Implement actual log streaming from scanner
	// For now, send periodic updates
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			progress := s.scanner.GetProgress()
			if progress != nil {
				snapshot := progress.GetSnapshot()
				msg := fmt.Sprintf("Processed %d/%d files (%.1f%%) - %s",
					snapshot.ProcessedFiles,
					snapshot.TotalFiles,
					snapshot.PercentComplete,
					snapshot.CurrentPhase,
				)
				fmt.Fprintf(w, "data: %s\n\n", msg)
				flusher.Flush()
			}
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

	// Update config from form
	s.config.DatabasePath = r.FormValue("database_path")

	if workers := r.FormValue("scan_workers"); workers != "" {
		if w, err := strconv.Atoi(workers); err == nil && w > 0 && w <= 100 {
			s.config.ScanWorkers = w
		}
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

	files, _, err := s.db.ListFiles(orphanedOnly, "", 100000, 0, "path")
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
		files, _, err := s.db.ListFiles(true, "", 100000, 0, "path")
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
		count, err := s.db.MarkFilesForRescan("is_orphaned = 1")
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

// formatDuration formats a duration to a human-readable string
func formatDuration(d time.Duration) string {
	if d == 0 {
		return "calculating..."
	}

	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm", h, m)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// Helper function to format file size
func formatSize(bytes int64) string {
	return stats.FormatSize(bytes)
}

// Add custom template functions
func (s *Server) templateFuncs() template.FuncMap {
	return template.FuncMap{
		"formatSize": formatSize,
		"add": func(a, b int) int {
			return a + b
		},
		"sub": func(a, b int) int {
			return a - b
		},
		"mul": func(a, b interface{}) float64 {
			var fa, fb float64
			switch v := a.(type) {
			case int:
				fa = float64(v)
			case int64:
				fa = float64(v)
			case float64:
				fa = v
			}
			switch v := b.(type) {
			case int:
				fb = float64(v)
			case int64:
				fb = float64(v)
			case float64:
				fb = v
			}
			return fa * fb
		},
		"div": func(a, b interface{}) float64 {
			var fa, fb float64
			switch v := a.(type) {
			case int:
				fa = float64(v)
			case int64:
				fa = float64(v)
			case float64:
				fa = v
			}
			switch v := b.(type) {
			case int:
				fb = float64(v)
			case int64:
				fb = float64(v)
			case float64:
				fb = v
			}
			if fb == 0 {
				return 0
			}
			return fa / fb
		},
		"join": strings.Join,
	}
}
