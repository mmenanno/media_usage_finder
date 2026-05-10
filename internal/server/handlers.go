package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/api"
	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
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

	// On scan completion: invalidate stats cache and truncate the WAL so the
	// write-ahead log doesn't grow unbounded across consecutive scans.
	srv.scanner.SetOnScanComplete(func() {
		srv.statsCache.Invalidate()
		if err := srv.db.CheckpointWAL(); err != nil {
			log.Printf("Warning: WAL checkpoint after scan failed: %v", err)
		}
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

	// Page-specific partials to parse alongside the page template so the page
	// can invoke them via {{template "name.html" .}}. Keep names unique across
	// pages — Go templates share a namespace within each parsed set.
	pagePartials := map[string][]string{
		"dashboard.html": {
			"partials/dashboard_progress.html",
			"partials/dashboard_disks.html",
			"partials/dashboard_services.html",
			"partials/dashboard_manual_updates.html",
		},
	}

	layoutPath := baseDir + "/layout.html"

	// Parse each page template with layout.html to avoid block name collisions
	// This ensures each page gets its own "content" block without conflicts
	for _, page := range pages {
		fullPath := baseDir + "/" + page

		parseFiles := []string{layoutPath, fullPath}
		for _, p := range pagePartials[page] {
			parseFiles = append(parseFiles, baseDir+"/"+p)
		}

		tmpl, err := template.New("").Funcs(s.templateFuncs).ParseFiles(parseFiles...)
		if err != nil {
			return fmt.Errorf("failed to parse %s: %w", page, err)
		}

		s.templates[page] = tmpl
	}

	// Load partial templates (used for HTMX responses)
	partials := []string{
		"partials/validation-errors.html",
		"logs_table.html",
		"audit_logs_table.html",
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

	serviceNames := []string{"plex", "sonarr", "radarr", "qbittorrent", "stash", "calibre"}
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
		// Don't try to send error response if template execution failed
		// Headers may have already been sent, causing "superfluous WriteHeader" error
		// This is especially common with broken pipe errors (client disconnect)
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
		"base64Encode": func(data interface{}) string {
			// Encode data to base64 for safe embedding in HTML attributes
			// Handle both string and *string types
			var str string
			switch v := data.(type) {
			case string:
				str = v
			case *string:
				if v != nil {
					str = *v
				}
			default:
				str = fmt.Sprintf("%v", v)
			}
			return base64.StdEncoding.EncodeToString([]byte(str))
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
			case "calibre":
				return "Calibre"
			default:
				return service
			}
		},
		"serviceClass": func(service string, variant string) string {
			// Returns CSS class for service-specific styling
			// Variants: "bg", "bg-faded", "bg-gradient", "border", "text", "text-on-bg", "hover"
			validServices := map[string]bool{
				"plex": true, "sonarr": true, "radarr": true,
				"qbittorrent": true, "stash": true, "calibre": true,
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
		"pluralize": func(count int, singular, plural string) string {
			// Returns the correct singular or plural form based on count
			if count == 1 {
				return singular
			}
			return plural
		},
		"isNotEmptyString": func(s *string) bool {
			// Check if a string pointer is not nil and not empty
			return s != nil && *s != ""
		},
	}
}

// Hash Scanning Handlers

// HandleStartHashScan starts a hash scanning operation
