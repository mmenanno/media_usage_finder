package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Run starts the HTTP server with graceful shutdown on port 8787
func (s *Server) Run() error {
	// Setup routes
	mux := http.NewServeMux()

	// Static files (serve from filesystem for now)
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("web/static"))))

	// Health check
	mux.HandleFunc("/health", s.HandleHealth)

	// Page routes
	mux.HandleFunc("/", s.HandleIndex)
	mux.HandleFunc("/files", s.HandleFiles)
	mux.HandleFunc("/hardlinks", s.HandleHardlinks)
	mux.HandleFunc("/scans", s.HandleScans)
	mux.HandleFunc("/stats", s.HandleStats)
	mux.HandleFunc("/advanced", s.HandleAdvanced)
	mux.HandleFunc("/config", s.HandleConfig)

	// API routes
	mux.HandleFunc("/api/scan/start", s.HandleStartScan)
	mux.HandleFunc("/api/scan/resume", s.HandleResumeScan)
	mux.HandleFunc("/api/scan/cancel", s.HandleCancelScan)
	mux.HandleFunc("/api/scan/force-stop", s.HandleForceStopScan)
	mux.HandleFunc("/api/scan/progress", s.HandleScanProgress)
	mux.HandleFunc("/api/scan/progress-html", s.HandleScanProgressHTML)
	mux.HandleFunc("/api/scan/logs", s.HandleScanLogs)
	mux.HandleFunc("/api/scan/update-services", s.HandleUpdateAllServices)
	mux.HandleFunc("/api/scan/update-service", s.HandleUpdateSingleService)
	mux.HandleFunc("/api/scan/recalculate-orphaned", s.HandleRecalculateOrphaned)
	mux.HandleFunc("/api/scan/disk-locations", s.HandleScanDiskLocations)
	mux.HandleFunc("/api/scan/disk-progress", s.HandleDiskScanProgress)
	mux.HandleFunc("/api/scan/disk-progress-html", s.HandleDiskScanProgressHTML)
	mux.HandleFunc("/api/hash/start", s.HandleStartHashScan)
	mux.HandleFunc("/api/hash/cancel", s.HandleCancelHashScan)
	mux.HandleFunc("/api/hash/progress", s.HandleHashProgress)
	mux.HandleFunc("/api/hash/progress-html", s.HandleHashProgressHTML)
	mux.HandleFunc("/api/hash/clear", s.HandleClearHashes)
	mux.HandleFunc("/api/hash/verify", s.HandleVerifyDuplicates)
	mux.HandleFunc("/api/hash/upgrade-all", s.HandleUpgradeAllHashes)
	mux.HandleFunc("/api/config/save", s.HandleSaveConfig)
	mux.HandleFunc("/api/config/test", s.HandleTestService)
	mux.HandleFunc("/api/config/test-scan-paths", s.HandleTestScanPaths)
	mux.HandleFunc("/api/plex/libraries", s.HandleGetPlexLibraries)
	mux.HandleFunc("/api/config/test-path-mappings", s.HandleTestPathMappings)
	mux.HandleFunc("/api/disks/detect", s.HandleDetectDisks)
	mux.HandleFunc("/api/export", s.HandleExport)
	mux.HandleFunc("/api/files/extensions", s.HandleGetFileExtensions)
	mux.HandleFunc("/api/files/delete", s.HandleDeleteFile)
	mux.HandleFunc("/api/files/mark-rescan", s.HandleMarkRescan)

	// Admin API routes
	mux.HandleFunc("/api/admin/clear-files", s.HandleAdminClearFiles)
	mux.HandleFunc("/api/admin/clear-scans", s.HandleAdminClearScans)
	mux.HandleFunc("/api/admin/clear-usage", s.HandleAdminClearUsage)
	mux.HandleFunc("/api/admin/vacuum", s.HandleAdminVacuum)
	mux.HandleFunc("/api/admin/rebuild-fts", s.HandleAdminRebuildFTS)
	mux.HandleFunc("/api/admin/clean-stale-scans", s.HandleAdminCleanStaleScans)
	mux.HandleFunc("/api/admin/recalculate-orphaned", s.HandleAdminRecalculateOrphaned)
	mux.HandleFunc("/api/admin/database-stats", s.HandleAdminDatabaseStats)
	mux.HandleFunc("/api/admin/audit-log", s.HandleAdminAuditLog)
	mux.HandleFunc("/api/admin/clear-config", s.HandleAdminClearConfig)
	mux.HandleFunc("/api/admin/clear-audit-log", s.HandleAdminClearAuditLog)

	// File details endpoint (with dynamic ID in query param)
	mux.HandleFunc("/api/files/", func(w http.ResponseWriter, r *http.Request) {
		// Extract ID from path like /api/files/123/details or /api/files/123/disk-locations
		path := r.URL.Path
		if len(path) > len("/api/files/") && path[len(path)-8:] == "/details" {
			// Extract ID from path
			idPart := path[len("/api/files/") : len(path)-8]
			r.URL.RawQuery = "id=" + idPart
			s.HandleFileDetails(w, r)
		} else if len(path) > len("/api/files/") && path[len(path)-15:] == "/disk-locations" {
			// Extract ID from path like /api/files/123/disk-locations
			idPart := path[len("/api/files/") : len(path)-15]
			r.URL.RawQuery = "id=" + idPart
			s.HandleGetFileDiskLocations(w, r)
		} else {
			http.NotFound(w, r)
		}
	})

	// Create rate limiter (10 requests per second with burst of 20)
	rateLimiter := NewRateLimiter(10.0, 20)
	rateLimiter.StartPeriodicCleanup(1 * time.Hour)

	// Apply middleware chain (order matters: Recovery -> RateLimit -> RequestID -> Logger -> RequestSizeLimit -> CORS -> handlers)
	handler := Recovery(rateLimiter.Middleware(RequestID(Logger(RequestSizeLimit(CORS(s.config.CORSAllowedOrigin)(mux))))))

	// Start server on hardcoded port 8787
	addr := ":8787"
	log.Printf("Starting server on %s", addr)

	server := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 5 * time.Minute, // Increased for SSE endpoints (logs streaming)
		IdleTimeout:  60 * time.Second,
	}

	// Run server in a goroutine
	serverErrors := make(chan error, 1)
	go func() {
		log.Println("Server is ready to handle requests")
		serverErrors <- server.ListenAndServe()
	}()

	// Setup graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// Block until we receive a signal or server error
	select {
	case err := <-serverErrors:
		return fmt.Errorf("server error: %w", err)
	case sig := <-shutdown:
		log.Printf("Received signal %v, starting graceful shutdown", sig)

		// Give outstanding requests 30 seconds to complete
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Could not gracefully shutdown server: %v", err)
			return fmt.Errorf("server shutdown error: %w", err)
		}

		log.Println("Server stopped gracefully")
		return nil
	}
}
