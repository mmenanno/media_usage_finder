package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"
)

func (s *Server) HandleStartScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "A scan is already running", "scan_already_running")
		return
	}

	incremental := r.URL.Query().Get("incremental") == "true"

	// Create context with timeout for scan operation
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
		defer cancel()

		if err := s.scanner.Scan(ctx, incremental); err != nil {
			log.Printf("ERROR: Scan failed: %v", err)
			// Scan error will be recorded in database by scanner itself
		} else {
			log.Printf("INFO: Scan completed successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Scan started successfully")
	w.Header().Set("X-Toast-Type", "info")

	response := ScanStartResponse{
		Status:      "success",
		Message:     "Scan started",
		Incremental: incremental,
	}
	respondJSON(w, http.StatusOK, response)
}

// HandleCancelScan gracefully cancels the current scan
func (s *Server) HandleCancelScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if s.scanner.Cancel() {
		w.Header().Set("X-Toast-Message", "Scan cancelled successfully")
		w.Header().Set("X-Toast-Type", "info")
		respondSuccess(w, "Scan cancelled", nil)
	} else {
		respondError(w, http.StatusConflict, "No scan is currently running", "no_scan_running")
	}
}

// HandleForceStopScan immediately terminates the current scan
func (s *Server) HandleForceStopScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if s.scanner.ForceStop() {
		w.Header().Set("X-Toast-Message", "Scan force stopped")
		w.Header().Set("X-Toast-Type", "warning")
		respondSuccess(w, "Scan force stopped", nil)
	} else {
		respondError(w, http.StatusConflict, "No scan is currently running", "no_scan_running")
	}
}

// HandleResumeScan resumes an interrupted scan from where it left off
func (s *Server) HandleResumeScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "A scan is already running", "scan_already_running")
		return
	}

	// Check if there's an interrupted scan to resume
	interruptedScan, err := s.db.GetLastInterruptedScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check for interrupted scans", "check_failed")
		return
	}

	if interruptedScan == nil {
		respondError(w, http.StatusNotFound, "No interrupted scan found to resume", "no_interrupted_scan")
		return
	}

	// Run resume scan in background
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
		defer cancel()

		if err := s.scanner.ResumeScan(ctx); err != nil {
			log.Printf("ERROR: Resume scan failed: %v", err)
		} else {
			log.Printf("INFO: Resume scan completed successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Resuming scan from checkpoint...")
	w.Header().Set("X-Toast-Type", "info")

	response := ScanStartResponse{
		Status:  "success",
		Message: fmt.Sprintf("Resuming scan #%d", interruptedScan.ID),
	}
	respondJSON(w, http.StatusOK, response)
}

// HandleUpdateAllServices manually updates all service usage information
func (s *Server) HandleUpdateAllServices(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "Cannot update services while a scan is running", "scan_running")
		return
	}

	// Run service updates in background
	go func() {
		if err := s.scanner.UpdateAllServices(); err != nil {
			log.Printf("ERROR: Failed to update all services: %v", err)
		} else {
			log.Printf("INFO: All services updated successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Updating all services...")
	w.Header().Set("X-Toast-Type", "info")

	respondSuccess(w, "Service update started", nil)
}

// HandleUpdateSingleService manually updates a specific service's usage information
func (s *Server) HandleUpdateSingleService(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Get service name from query parameter
	serviceName := r.URL.Query().Get("service")
	if serviceName == "" {
		respondError(w, http.StatusBadRequest, "Service name is required", "missing_service")
		return
	}

	// Validate service name
	validServices := map[string]bool{
		"plex":        true,
		"sonarr":      true,
		"radarr":      true,
		"qbittorrent": true,
		"stash":       true,
		"calibre":     true,
	}
	if !validServices[serviceName] {
		respondError(w, http.StatusBadRequest, "Invalid service name", "invalid_service")
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "Cannot update service while a scan is running", "scan_running")
		return
	}

	// Run service update in background
	go func() {
		if err := s.scanner.UpdateSingleService(serviceName); err != nil {
			log.Printf("ERROR: Failed to update %s: %v", serviceName, err)
		} else {
			log.Printf("INFO: %s updated successfully", serviceName)
		}
	}()

	w.Header().Set("X-Toast-Message", fmt.Sprintf("Updating %s...", serviceName))
	w.Header().Set("X-Toast-Type", "info")

	respondSuccess(w, fmt.Sprintf("%s update started", serviceName), nil)
}

// HandleRecalculateOrphaned manually recalculates which files are orphaned
func (s *Server) HandleRecalculateOrphaned(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "Cannot recalculate while a scan is running", "scan_running")
		return
	}

	// Run recalculation in background
	go func() {
		if err := s.scanner.RecalculateOrphanedStatus(); err != nil {
			log.Printf("ERROR: Failed to recalculate orphaned status: %v", err)
		} else {
			log.Printf("INFO: Orphaned status recalculated successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Recalculating orphaned status...")
	w.Header().Set("X-Toast-Type", "info")

	respondSuccess(w, "Recalculation started", nil)
}

// HandleCleanupScan runs a manual cleanup scan to remove database entries for missing files
func (s *Server) HandleCleanupScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Check if a scan is already running
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to check scan status", "scan_check_failed")
		return
	}

	if currentScan != nil {
		respondError(w, http.StatusConflict, "Cannot start cleanup while a scan is running", "scan_running")
		return
	}

	// Run cleanup scan in background
	go func() {
		if err := s.scanner.RunCleanupScan(); err != nil {
			log.Printf("ERROR: Failed to run cleanup scan: %v", err)
		} else {
			log.Printf("INFO: Cleanup scan completed successfully")
		}
	}()

	w.Header().Set("X-Toast-Message", "Starting cleanup scan...")
	w.Header().Set("X-Toast-Type", "info")

	respondSuccess(w, "Cleanup scan started", nil)
}

// HandleScanProgress returns the current scan progress
