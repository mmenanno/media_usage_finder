package server

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/api"
	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/disk"
	"github.com/mmenanno/media-usage-finder/internal/scanner"
)

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
	s.config.DeleteFilesFromFilesystem = r.FormValue("delete_files_from_filesystem") != ""

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

	// Validate Calibre config (paths don't need URL validation)
	calibreLibraryPath := r.FormValue("calibre_library_path")
	calibreDBPath := r.FormValue("calibre_db_path")
	// Calibre paths are optional, but if one is set, both should be set
	if (calibreLibraryPath != "" && calibreDBPath == "") || (calibreLibraryPath == "" && calibreDBPath != "") {
		validationErrors = append(validationErrors, "Calibre: Both library path and database path must be set together")
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

	s.config.Services.Calibre.LibraryPath = calibreLibraryPath
	s.config.Services.Calibre.DBPath = calibreDBPath

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

	// Parse progressive level settings
	// Note: HTML checkboxes send "on" when checked, or nothing when unchecked
	s.config.DuplicateDetection.EnableLevel1 = r.FormValue("enable_level_1") != ""
	s.config.DuplicateDetection.EnableLevel2 = r.FormValue("enable_level_2") != ""
	s.config.DuplicateDetection.EnableLevel3 = r.FormValue("enable_level_3") != ""
	s.config.DuplicateDetection.EnableLevel4 = r.FormValue("enable_level_4") != ""
	s.config.DuplicateDetection.EnableLevel5 = r.FormValue("enable_level_5") != ""
	s.config.DuplicateDetection.EnableLevel6 = r.FormValue("enable_level_6") != ""

	// Validate: At least one level must be enabled (used by Progressive Verify)
	enabledLevels := s.config.DuplicateDetection.GetEnabledProgressiveLevels()
	if len(enabledLevels) == 0 {
		validationErrors = append(validationErrors, "At least one progressive hash level must be enabled")
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
	case "calibre":
		return "Calibre"
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
	case "calibre":
		libraryPath := strings.TrimSpace(r.FormValue("calibre_library_path"))
		dbPath := strings.TrimSpace(r.FormValue("calibre_db_path"))
		if libraryPath == "" {
			missingField = "Calibre Library Path"
		} else if dbPath == "" {
			missingField = "Calibre Database Path"
		} else {
			testConfig.Services.Calibre.LibraryPath = libraryPath
			testConfig.Services.Calibre.DBPath = dbPath
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
