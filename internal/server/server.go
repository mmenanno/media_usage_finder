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

// Run starts the HTTP server with graceful shutdown
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

	// Apply middleware chain (order matters: Recovery -> RequestID -> Logger -> RequestSizeLimit -> CORS -> handlers)
	handler := Recovery(RequestID(Logger(RequestSizeLimit(CORS(s.config.CORSAllowedOrigin)(mux)))))

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
