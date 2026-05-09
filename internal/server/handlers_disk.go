package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/disk"
	"github.com/mmenanno/media-usage-finder/internal/stats"
)

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
