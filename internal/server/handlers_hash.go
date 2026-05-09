package server

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/disk"
	"github.com/mmenanno/media-usage-finder/internal/scanner"
	"github.com/mmenanno/media-usage-finder/internal/stats"
)

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
