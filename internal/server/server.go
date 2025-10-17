package server

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

// Run starts the HTTP server
func (s *Server) Run(port int) error {
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
	mux.HandleFunc("/config", s.HandleConfig)
	mux.HandleFunc("/stats", s.HandleStats)

	// API routes
	mux.HandleFunc("/api/scan/start", s.HandleStartScan)
	mux.HandleFunc("/api/scan/progress", s.HandleScanProgress)
	mux.HandleFunc("/api/scan/progress-html", s.HandleScanProgressHTML)
	mux.HandleFunc("/api/scan/logs", s.HandleScanLogs)
	mux.HandleFunc("/api/config/save", s.HandleSaveConfig)
	mux.HandleFunc("/api/config/test", s.HandleTestService)
	mux.HandleFunc("/api/export", s.HandleExport)
	mux.HandleFunc("/api/files/delete", s.HandleDeleteFile)
	mux.HandleFunc("/api/files/mark-rescan", s.HandleMarkRescan)

	// Apply middleware
	handler := Recovery(Logger(CORS(mux)))

	// Start server
	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting server on %s", addr)

	server := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return server.ListenAndServe()
}
