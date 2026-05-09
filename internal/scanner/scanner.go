package scanner

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/constants"
	"github.com/mmenanno/media-usage-finder/internal/database"
)

// Scanner coordinates the entire scanning process
type Scanner struct {
	db               *database.DB
	config           *config.Config
	progress         *Progress
	diskScanProgress *Progress // Progress tracker for disk location scanning
	cancel           context.CancelFunc
	scanCtx          context.Context // Current scan context for cancellation
	onScanComplete   func()          // Callback when scan completes
}

// NewScanner creates a new scanner
func NewScanner(db *database.DB, cfg *config.Config) *Scanner {
	return &Scanner{
		db:     db,
		config: cfg,
	}
}

// serializeErrors converts a slice of error strings to a JSON string for database storage
// Returns empty string if there are no errors
func serializeErrors(errors []string) string {
	if len(errors) == 0 {
		return ""
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(errors)
	if err != nil {
		// Fallback to simple concatenation if JSON marshaling fails
		log.Printf("Warning: Failed to marshal errors to JSON: %v", err)
		result := ""
		for i, e := range errors {
			if i > 0 {
				result += "\n"
			}
			result += e
		}
		return result
	}

	return string(jsonData)
}

// SetOnScanComplete sets the callback to be called when scan completes
func (s *Scanner) SetOnScanComplete(callback func()) {
	s.onScanComplete = callback
}

// Cancel gracefully stops the current scan
func (s *Scanner) Cancel() bool {
	if s.cancel != nil {
		log.Println("Gracefully cancelling scan...")
		if s.progress != nil {
			s.progress.SetPhase("Cancelling")
			s.progress.Log("Scan cancelled by user")
		}
		s.cancel()
		return true
	}
	return false
}

// ForceStop immediately terminates the current scan
func (s *Scanner) ForceStop() bool {
	if s.cancel != nil {
		log.Println("Force stopping scan...")
		if s.progress != nil {
			s.progress.SetPhase("Force Stopped")
			s.progress.Log("Scan force stopped by user")
		}
		s.cancel()
		// For force stop, we call cancel immediately without grace period
		// The context cancellation will propagate and stop all operations
		return true
	}
	return false
}

// RescanFiles rescans specific files immediately
// This checks if files still exist, updates their metadata, queries all services, and recalculates orphaned status
func (s *Scanner) Scan(ctx context.Context, incremental bool) error {
	// Check if there's already a running scan
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		return fmt.Errorf("failed to check for running scan: %w", err)
	}

	if currentScan != nil {
		return fmt.Errorf("scan already running (ID: %d)", currentScan.ID)
	}

	// Create scan record
	scanType := "full"
	if incremental {
		scanType = "incremental"
	}

	scan, err := s.db.CreateScan(scanType)
	if err != nil {
		return fmt.Errorf("failed to create scan record: %w", err)
	}

	// Initialize progress tracker
	s.progress = NewProgress(scan.ID, s.db)
	s.progress.SetPhase("Initializing")

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received interrupt signal, stopping scan gracefully...")
		s.progress.SetPhase("Stopping")
		cancel()
	}()

	// Run the scan
	scanErr := s.runScan(ctx, scan.ID, incremental)

	// Update scan status
	status := "completed"
	var errorMsg *string
	if scanErr != nil {
		if ctx.Err() != nil {
			status = "interrupted"
		} else {
			status = "failed"
		}
		msg := scanErr.Error()
		errorMsg = &msg
	} else if s.progress != nil && len(s.progress.Errors) > 0 {
		// Scan completed but had errors during processing
		status = "completed_with_errors"
		// Serialize all accumulated errors to JSON
		serialized := serializeErrors(s.progress.Errors)
		errorMsg = &serialized
	}

	s.progress.Stop()

	if err := s.db.UpdateScan(scan.ID, status, s.progress.ProcessedFiles, errorMsg); err != nil {
		log.Printf("Failed to update scan status: %v", err)
	}

	// Call completion callback if set
	if s.onScanComplete != nil && status == "completed" {
		s.onScanComplete()
	}

	// Clear progress object so GetProgress() returns nil
	// This prevents the UI from showing stale progress after scan completes
	s.progress = nil

	return scanErr
}

// ResumeScan resumes an interrupted scan from where it left off
func (s *Scanner) ResumeScan(ctx context.Context) error {
	// Check if there's already a running scan
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		return fmt.Errorf("failed to check for running scan: %w", err)
	}

	if currentScan != nil {
		return fmt.Errorf("scan already running (ID: %d)", currentScan.ID)
	}

	// Get the last interrupted scan
	interruptedScan, err := s.db.GetLastInterruptedScan()
	if err != nil {
		return fmt.Errorf("failed to get interrupted scan: %w", err)
	}

	if interruptedScan == nil {
		return fmt.Errorf("no interrupted scan found to resume")
	}

	// Create a new scan that resumes from the interrupted one
	scan, err := s.db.CreateResumeScan(interruptedScan.ScanType, interruptedScan.ID)
	if err != nil {
		return fmt.Errorf("failed to create resume scan record: %w", err)
	}

	log.Printf("Resuming scan #%d from where it left off (originally scan #%d)", scan.ID, interruptedScan.ID)
	if interruptedScan.LastProcessedPath != nil {
		log.Printf("Last processed path: %s", *interruptedScan.LastProcessedPath)
	}

	// Initialize progress tracker
	s.progress = NewProgress(scan.ID, s.db)
	s.progress.SetPhase("Initializing")
	s.progress.Log(fmt.Sprintf("Resuming scan #%d from where it left off", interruptedScan.ID))

	// Set processed files from interrupted scan
	s.progress.ProcessedFiles = interruptedScan.FilesScanned

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received interrupt signal, stopping scan gracefully...")
		s.progress.SetPhase("Stopping")
		cancel()
	}()

	// Run the scan with resume path
	scanErr := s.runScanWithResume(ctx, scan.ID, interruptedScan.ScanType == "incremental", interruptedScan.LastProcessedPath)

	// Update scan status
	status := "completed"
	var errorMsg *string
	if scanErr != nil {
		if ctx.Err() != nil {
			status = "interrupted"
		} else {
			status = "failed"
		}
		msg := scanErr.Error()
		errorMsg = &msg
	} else if s.progress != nil && len(s.progress.Errors) > 0 {
		// Scan completed but had errors during processing
		status = "completed_with_errors"
		// Serialize all accumulated errors to JSON
		serialized := serializeErrors(s.progress.Errors)
		errorMsg = &serialized
	}

	s.progress.Stop()

	if err := s.db.UpdateScan(scan.ID, status, s.progress.ProcessedFiles, errorMsg); err != nil {
		log.Printf("Failed to update scan status: %v", err)
	}

	// Call completion callback if set
	if s.onScanComplete != nil && status == "completed" {
		s.onScanComplete()
	}

	// Clear progress object so GetProgress() returns nil
	// This prevents the UI from showing stale progress after scan completes
	s.progress = nil

	return scanErr
}

// runScan performs the actual scanning work
func (s *Scanner) runScan(ctx context.Context, scanID int64, incremental bool) error {
	// Store scan context for service updates to respect cancellation
	s.scanCtx = ctx

	// Ensure files_scanned is persisted even if scan is interrupted/cancelled/panics
	// This is critical for scan history to show accurate counts
	defer func() {
		if s.progress != nil {
			processedFiles := s.progress.ProcessedFiles
			if processedFiles > 0 {
				// Update the scan record with the current file count
				// This persists the count for interrupted, cancelled, or crashed scans
				if err := s.db.UpdateScanFilesProcessed(scanID, processedFiles); err != nil {
					// Log error but don't fail - this is cleanup code
					s.progress.Log(fmt.Sprintf("Warning: Failed to persist file count on cleanup: %v", err))
				}
			}
		}
	}()

	// Initialize progress totals using the current database contents (with fallback)
	s.initializeProgressTotal()

	// Scan filesystem immediately (no file counting phase)
	// Files are counted dynamically as they're processed
	s.updatePhase(scanID, "Scanning filesystem")
	if incremental {
		s.progress.Log("Starting incremental filesystem scan (only changed files)...")
	} else {
		s.progress.Log("Starting full filesystem scan...")
	}

	if err := s.scanFilesystem(ctx, scanID, incremental); err != nil {
		return fmt.Errorf("filesystem scan failed: %w", err)
	}

	// Phase 2.5: Clean up deleted files (only during full scans if auto-cleanup is enabled)
	if !incremental && s.config.AutoCleanupDeletedFiles {
		s.updatePhase(scanID, "Cleaning Up Deleted Files")
		s.progress.Log("Removing files from database that no longer exist on disk...")

		deletedCount, err := s.db.DeleteUnverifiedFiles(ctx, scanID)
		if err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to cleanup deleted files: %v", err))
		} else if deletedCount > 0 {
			s.progress.Log(fmt.Sprintf("Removed %d files that no longer exist on disk", deletedCount))
			// Update scan record with deleted count
			if err := s.db.UpdateScanDeletedCount(scanID, deletedCount); err != nil {
				s.progress.Log(fmt.Sprintf("Warning: Failed to update deleted files count: %v", err))
			}
		} else {
			s.progress.Log("No deleted files found to cleanup")
		}
	}

	// Phase 3: Update service usage
	// Count configured services for progress tracking
	totalServices := 0
	if s.config.Services.Plex.URL != "" {
		totalServices++
	}
	if s.config.Services.Sonarr.URL != "" {
		totalServices++
	}
	if s.config.Services.Radarr.URL != "" {
		totalServices++
	}
	if s.config.Services.QBittorrent.URL != "" || s.config.Services.QBittorrent.QuiProxyURL != "" {
		totalServices++
	}
	if s.config.Services.Stash.URL != "" {
		totalServices++
	}
	if s.config.Services.Calibre.LibraryPath != "" && s.config.Services.Calibre.DBPath != "" {
		totalServices++
	}
	currentService := 0

	// Update Plex if configured
	if s.config.Services.Plex.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking Plex")
		s.progress.Log("Querying Plex for tracked files...")
		if err := s.updatePlexUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update Plex usage: %v", err))
		}
	}

	// Update Sonarr if configured
	if s.config.Services.Sonarr.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking Sonarr")
		s.progress.Log("Querying Sonarr for tracked files...")
		if err := s.updateSonarrUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update Sonarr usage: %v", err))
		}
	}

	// Update Radarr if configured
	if s.config.Services.Radarr.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking Radarr")
		s.progress.Log("Querying Radarr for tracked files...")
		if err := s.updateRadarrUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update Radarr usage: %v", err))
		}
	}

	// Update qBittorrent if configured
	if s.config.Services.QBittorrent.URL != "" || s.config.Services.QBittorrent.QuiProxyURL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking qBittorrent")
		s.progress.Log("Querying qBittorrent for tracked files...")
		if err := s.updateQBittorrentUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update qBittorrent usage: %v", err))
		}
	}

	// Update Stash if configured
	if s.config.Services.Stash.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking Stash")
		s.progress.Log("Querying Stash for tracked files...")
		if err := s.updateStashUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update Stash usage: %v", err))
		}
	}

	// Update Calibre if configured
	if s.config.Services.Calibre.LibraryPath != "" && s.config.Services.Calibre.DBPath != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking Calibre")
		s.progress.Log("Querying Calibre for tracked files...")
		if err := s.updateCalibreUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update Calibre usage: %v", err))
		}
	}

	// Phase 4: Update orphaned status
	s.updatePhase(scanID, "Updating orphaned status")
	s.progress.Log("Calculating orphaned file status...")

	if err := s.db.UpdateOrphanedStatus(ctx); err != nil {
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

	// Log path cache performance statistics
	s.logCacheStats()

	s.updatePhase(scanID, "Completed")
	s.progress.Log("Scan completed successfully!")

	return nil
}

// runScanWithResume performs scanning work, optionally resuming from a checkpoint
func (s *Scanner) runScanWithResume(ctx context.Context, scanID int64, incremental bool, resumeFromPath *string) error {
	// Store scan context for service updates to respect cancellation
	s.scanCtx = ctx

	// Ensure files_scanned is persisted even if scan is interrupted/cancelled/panics
	// This is critical for scan history to show accurate counts
	defer func() {
		if s.progress != nil {
			processedFiles := s.progress.ProcessedFiles
			if processedFiles > 0 {
				// Update the scan record with the current file count
				// This persists the count for interrupted, cancelled, or crashed scans
				if err := s.db.UpdateScanFilesProcessed(scanID, processedFiles); err != nil {
					// Log error but don't fail - this is cleanup code
					s.progress.Log(fmt.Sprintf("Warning: Failed to persist file count on cleanup: %v", err))
				}
			}
		}
	}()

	// Initialize progress totals using the current database contents (with fallback)
	s.initializeProgressTotal()

	// Scan filesystem immediately (no file counting phase)
	// Files are counted dynamically as they're processed
	s.updatePhase(scanID, "Scanning filesystem")
	if resumeFromPath != nil {
		s.progress.Log(fmt.Sprintf("Resuming from checkpoint: %s", *resumeFromPath))
	} else if incremental {
		s.progress.Log("Starting incremental filesystem scan (only changed files)...")
	} else {
		s.progress.Log("Starting full filesystem scan...")
	}

	// Save checkpoints every 1000 files
	checkpointInterval := int64(1000)
	lastCheckpoint := int64(0)

	// Set up checkpoint saving
	checkpointTicker := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-checkpointTicker:
				if s.progress.ProcessedFiles-lastCheckpoint >= checkpointInterval {
					// Get the last processed file path from progress
					// For now, we'll just save the processed count
					// In a more complete implementation, we'd track the actual last file path
					if err := s.db.UpdateScanCheckpoint(scanID, "checkpoint"); err != nil {
						log.Printf("WARNING: Failed to save checkpoint: %v", err)
					}
					lastCheckpoint = s.progress.ProcessedFiles
				}
			}
		}
	}()

	if err := s.scanFilesystem(ctx, scanID, incremental); err != nil {
		close(checkpointTicker)
		return fmt.Errorf("filesystem scan failed: %w", err)
	}
	close(checkpointTicker)

	// Phase 3: Update service usage
	// Count configured services for progress tracking
	totalServices := 0
	if s.config.Services.Plex.URL != "" {
		totalServices++
	}
	if s.config.Services.Sonarr.URL != "" {
		totalServices++
	}
	if s.config.Services.Radarr.URL != "" {
		totalServices++
	}
	if s.config.Services.QBittorrent.URL != "" || s.config.Services.QBittorrent.QuiProxyURL != "" {
		totalServices++
	}
	if s.config.Services.Stash.URL != "" {
		totalServices++
	}
	if s.config.Services.Calibre.LibraryPath != "" && s.config.Services.Calibre.DBPath != "" {
		totalServices++
	}
	currentService := 0

	// Update Plex if configured
	if s.config.Services.Plex.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking Plex")
		s.progress.Log("Querying Plex for tracked files...")
		if err := s.updatePlexUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update Plex usage: %v", err))
		}
	}

	// Update Sonarr if configured
	if s.config.Services.Sonarr.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking Sonarr")
		s.progress.Log("Querying Sonarr for tracked files...")
		if err := s.updateSonarrUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update Sonarr usage: %v", err))
		}
	}

	// Update Radarr if configured
	if s.config.Services.Radarr.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking Radarr")
		s.progress.Log("Querying Radarr for tracked files...")
		if err := s.updateRadarrUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update Radarr usage: %v", err))
		}
	}

	// Update qBittorrent if configured
	if s.config.Services.QBittorrent.URL != "" || s.config.Services.QBittorrent.QuiProxyURL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking qBittorrent")
		s.progress.Log("Querying qBittorrent for tracked files...")
		if err := s.updateQBittorrentUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update qBittorrent usage: %v", err))
		}
	}

	// Update Stash if configured
	if s.config.Services.Stash.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking Stash")
		s.progress.Log("Querying Stash for tracked files...")
		if err := s.updateStashUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update Stash usage: %v", err))
		}
	}

	// Update Calibre if configured
	if s.config.Services.Calibre.LibraryPath != "" && s.config.Services.Calibre.DBPath != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.updatePhase(scanID, "Checking Calibre")
		s.progress.Log("Querying Calibre for tracked files...")
		if err := s.updateCalibreUsage(); err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to update Calibre usage: %v", err))
		}
	}

	// Phase 4: Update orphaned status
	s.updatePhase(scanID, "Updating orphaned status")
	s.progress.Log("Calculating orphaned file status...")

	if err := s.db.UpdateOrphanedStatus(ctx); err != nil {
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

	// Log path cache performance statistics
	s.logCacheStats()

	s.updatePhase(scanID, "Completed")
	s.progress.Log("Scan completed successfully!")

	return nil
}

// serviceFile is a generic interface for files from different services
func (s *Scanner) scanFilesystem(ctx context.Context, scanID int64, incremental bool) error {
	// For incremental scans, pre-load all files into memory for fast lookups
	// This eliminates individual database queries for each file during processing
	var fileMap map[string]*database.File
	if incremental {
		s.progress.Log("Pre-loading file index for incremental scan...")
		var err error
		fileMap, err = s.db.GetAllFilesMap(ctx)
		if err != nil {
			return fmt.Errorf("failed to pre-load file index: %w", err)
		}
		s.progress.Log(fmt.Sprintf("Loaded %d files into memory index", len(fileMap)))
	}

	// Create worker pool with configurable buffer size and optional file map
	pool := NewWorkerPool(s.config.ScanWorkers, s.config.ScanBufferSize, s.db, fileMap, scanID, s.progress, incremental)
	pool.Start()

	// Walk filesystem in goroutine
	walkDone := make(chan error, 1)
	go func() {
		walkDone <- WalkFiles(ctx, s.config.ScanPaths, pool.GetInputChannel(), s.progress)
	}()

	// Wait for walk to complete or context cancellation
	select {
	case <-ctx.Done():
		pool.Cancel()
		return ctx.Err()
	case err := <-walkDone:
		pool.Stop() // Graceful shutdown after walk completes
		return err
	}
}

// updatePlexUsage updates usage information from Plex
func (s *Scanner) GetProgress() *Progress {
	return s.progress
}

// GetDiskScanProgress returns the current disk scan progress tracker
func (s *Scanner) GetDiskScanProgress() *Progress {
	return s.diskScanProgress
}

// updatePhase updates both the progress phase and the database
func (s *Scanner) updatePhase(scanID int64, phase string) {
	if s.progress != nil {
		s.progress.SetPhase(phase)
	}
	if err := s.db.UpdateScanPhase(scanID, phase); err != nil {
		log.Printf("WARNING: Failed to update scan phase in database: %v", err)
	}
}

// logCacheStats logs path cache performance statistics
func (s *Scanner) logCacheStats() {
	hits, total, evictions, size, hitRate := s.config.GetPathCacheStats()
	log.Printf("Path cache stats - Size: %d/%d, Hits: %d/%d (%.2f%%), Evictions: %d",
		size, constants.PathCacheSize, hits, total, hitRate*100, evictions)
	if s.progress != nil {
		s.progress.Log(fmt.Sprintf("Path cache: %d entries, %.1f%% hit rate, %d evictions",
			size, hitRate*100, evictions))
	}
}

// initializeProgressTotal sets up the progress tracker with the best-known total file count
func (s *Scanner) initializeProgressTotal() {
	if s.progress == nil {
		return
	}

	if currentCount, err := s.db.GetCurrentFileCount(); err == nil {
		if currentCount > 0 {
			// Use the current DB count but mark it as an estimate since new files may appear mid-scan
			s.progress.SetEstimatedTotal(currentCount)
			s.progress.Log(fmt.Sprintf("Estimating progress using %d files currently stored in the database", currentCount))
			return
		}
	} else {
		s.progress.Log(fmt.Sprintf("Warning: Failed to read current database file count: %v", err))
	}

	// Fallback to last completed scan count if database has no files yet
	if lastCount, err := s.db.GetLastCompletedScanFileCount(); err == nil && lastCount > 0 {
		s.progress.SetEstimatedTotal(lastCount)
		s.progress.Log(fmt.Sprintf("Using previous scan count (%d files) as estimate for progress tracking", lastCount))
	}
}

// UpdateAllServices manually updates all service usage information
// This can be called independently without a full scan
