package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/stats"
)

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
