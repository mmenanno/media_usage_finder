package scanner

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mmenanno/media-usage-finder/internal/api"
	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/constants"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
)

// Scanner coordinates the entire scanning process
type Scanner struct {
	db                *database.DB
	config            *config.Config
	progress          *Progress
	diskScanProgress  *Progress           // Progress tracker for disk location scanning
	cancel            context.CancelFunc
	scanCtx           context.Context     // Current scan context for cancellation
	onScanComplete    func()              // Callback when scan completes
}

// NewScanner creates a new scanner
func NewScanner(db *database.DB, cfg *config.Config) *Scanner {
	return &Scanner{
		db:     db,
		config: cfg,
	}
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

// Scan performs a full or incremental scan
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
	s.progress = NewProgress()
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
	s.progress = NewProgress()
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

	// Try to get previous scan's file count as estimate for progress tracking
	// This allows showing percentage and ETA even without pre-counting files
	if lastCount, err := s.db.GetLastCompletedScanFileCount(); err == nil && lastCount > 0 {
		s.progress.SetEstimatedTotal(lastCount)
		s.progress.Log(fmt.Sprintf("Using previous scan count (%d files) as estimate for progress tracking", lastCount))
	}

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

	// Try to get previous scan's file count as estimate for progress tracking
	// This allows showing percentage and ETA even without pre-counting files
	if lastCount, err := s.db.GetLastCompletedScanFileCount(); err == nil && lastCount > 0 {
		s.progress.SetEstimatedTotal(lastCount)
		s.progress.Log(fmt.Sprintf("Using previous scan count (%d files) as estimate for progress tracking", lastCount))
	}

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
type serviceFile interface {
	GetPath() string
	GetMetadata() map[string]interface{}
}

// Implement serviceFile for each service type
type plexServiceFile struct{ api.PlexFile }

func (f plexServiceFile) GetPath() string { return f.Path }
func (f plexServiceFile) GetMetadata() map[string]interface{} {
	return map[string]interface{}{"size": f.Size}
}

type sonarrServiceFile struct{ api.SonarrFile }

func (f sonarrServiceFile) GetPath() string { return f.Path }
func (f sonarrServiceFile) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"series_title":  f.SeriesTitle,
		"season_number": f.SeasonNumber,
		"episode_id":    f.EpisodeID,
	}
}

type radarrServiceFile struct{ api.RadarrFile }

func (f radarrServiceFile) GetPath() string { return f.Path }
func (f radarrServiceFile) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"movie_title": f.MovieTitle,
		"movie_year":  f.MovieYear,
		"movie_id":    f.MovieID,
	}
}

type qbittorrentServiceFile struct{ api.QBittorrentFile }

func (f qbittorrentServiceFile) GetPath() string { return f.Path }
func (f qbittorrentServiceFile) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"torrent_hash": f.TorrentHash,
		"torrent_name": f.TorrentName,
	}
}

type stashServiceFile struct{ api.StashFile }

func (f stashServiceFile) GetPath() string { return f.Path }
func (f stashServiceFile) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"scene_id":   f.SceneID,
		"title":      f.Title,
		"studio":     f.Studio,
		"tags":       f.Tags,
		"play_count": f.PlayCount,
	}
}

// updateServiceUsage is a generic method to update usage information for any service
func (s *Scanner) updateServiceUsage(ctx context.Context, serviceName string, files []serviceFile) error {
	if len(files) == 0 {
		log.Printf("%s: No files returned from service", serviceName)
		return nil
	}

	log.Printf("%s: Starting update with %d files from service", serviceName, len(files))

	// Clear old usage records
	if err := s.db.DeleteUsageByService(ctx, serviceName); err != nil {
		return err
	}
	log.Printf("%s: Cleared old usage records", serviceName)

	// Translate all paths and collect for batch lookup
	hostPaths := make([]string, 0, len(files))
	pathToFile := make(map[string]serviceFile)

	for i, file := range files {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		originalPath := file.GetPath()
		hostPath := s.config.TranslatePathToHost(originalPath, serviceName)

		// Log first few path translations for debugging
		if i < 3 {
			log.Printf("%s: Path translation example %d: %s -> %s", serviceName, i+1, originalPath, hostPath)
		}

		hostPaths = append(hostPaths, hostPath)
		pathToFile[hostPath] = file
	}

	log.Printf("%s: Translated %d paths, querying database...", serviceName, len(hostPaths))

	// Batch load all files from database
	dbFiles, err := s.db.GetFilesByPaths(ctx, hostPaths)
	if err != nil {
		return fmt.Errorf("failed to batch load files: %w", err)
	}

	log.Printf("%s: Found %d files in database out of %d queried", serviceName, len(dbFiles), len(hostPaths))

	// Collect usage records
	var usages []*database.Usage
	notFoundCount := 0
	for hostPath, file := range pathToFile {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		dbFile, ok := dbFiles[hostPath]
		if !ok {
			// Log first few missing files for debugging
			if notFoundCount < 3 {
				log.Printf("%s: File not in database: %s", serviceName, hostPath)
			}
			notFoundCount++
			continue
		}

		usages = append(usages, &database.Usage{
			FileID:        dbFile.ID,
			Service:       serviceName,
			ReferencePath: file.GetPath(),
			Metadata:      file.GetMetadata(),
		})
	}

	log.Printf("%s: Created %d usage records (%d files not found in database)", serviceName, len(usages), notFoundCount)

	// Batch insert all usage records
	if len(usages) > 0 {
		if err := s.db.BatchUpsertUsage(ctx, usages); err != nil {
			return fmt.Errorf("failed to batch insert %s usage: %w", serviceName, err)
		}
		log.Printf("%s: Successfully inserted %d usage records", serviceName, len(usages))
	}

	matched := len(usages)
	total := len(files)
	s.progress.Log(fmt.Sprintf("%s: matched %d of %d files (%d not found in filesystem)",
		serviceName, matched, total, total-matched))
	return nil
}

// scanFilesystem scans the filesystem and processes files
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
func (s *Scanner) updatePlexUsage() error {
	if s.config.Services.Plex.URL == "" {
		return nil
	}

	return s.updateServiceUsageWithTimeout(
		"plex",
		func(ctx context.Context) ([]serviceFile, error) {
			client := api.NewPlexClient(s.config.Services.Plex.URL, s.config.Services.Plex.Token, s.config.APITimeout)
			// Pass library filter from config (empty = scan all libraries)
			files, err := client.GetAllFiles(ctx, s.config.Services.Plex.Libraries)
			if err != nil {
				return nil, err
			}
			serviceFiles := make([]serviceFile, len(files))
			for i, f := range files {
				serviceFiles[i] = plexServiceFile{f}
			}
			return serviceFiles, nil
		},
	)
}

// updateSonarrUsage updates usage information from Sonarr
func (s *Scanner) updateSonarrUsage() error {
	if s.config.Services.Sonarr.URL == "" {
		return nil
	}

	return s.updateServiceUsageWithTimeout(
		"sonarr",
		func(ctx context.Context) ([]serviceFile, error) {
			client := api.NewSonarrClient(s.config.Services.Sonarr.URL, s.config.Services.Sonarr.APIKey, s.config.APITimeout)
			files, err := client.GetAllFiles(ctx)
			if err != nil {
				return nil, err
			}
			serviceFiles := make([]serviceFile, len(files))
			for i, f := range files {
				serviceFiles[i] = sonarrServiceFile{f}
			}
			return serviceFiles, nil
		},
	)
}

// updateRadarrUsage updates usage information from Radarr
func (s *Scanner) updateRadarrUsage() error {
	if s.config.Services.Radarr.URL == "" {
		return nil
	}

	return s.updateServiceUsageWithTimeout(
		"radarr",
		func(ctx context.Context) ([]serviceFile, error) {
			client := api.NewRadarrClient(s.config.Services.Radarr.URL, s.config.Services.Radarr.APIKey, s.config.APITimeout)
			files, err := client.GetAllFiles(ctx)
			if err != nil {
				return nil, err
			}
			serviceFiles := make([]serviceFile, len(files))
			for i, f := range files {
				serviceFiles[i] = radarrServiceFile{f}
			}
			return serviceFiles, nil
		},
	)
}

// updateQBittorrentUsage updates usage information from qBittorrent
func (s *Scanner) updateQBittorrentUsage() error {
	qbConfig := s.config.Services.QBittorrent
	if qbConfig.URL == "" && qbConfig.QuiProxyURL == "" {
		return nil
	}

	return s.updateServiceUsageWithTimeout(
		"qbittorrent",
		func(ctx context.Context) ([]serviceFile, error) {
			client := api.NewQBittorrentClient(qbConfig.URL, qbConfig.Username, qbConfig.Password, qbConfig.QuiProxyURL, s.config.APITimeout)
			files, err := client.GetAllFiles(ctx)
			if err != nil {
				return nil, err
			}
			serviceFiles := make([]serviceFile, len(files))
			for i, f := range files {
				serviceFiles[i] = qbittorrentServiceFile{f}
			}
			return serviceFiles, nil
		},
	)
}

// updateStashUsage updates usage information from Stash
func (s *Scanner) updateStashUsage() error {
	if s.config.Services.Stash.URL == "" {
		return nil
	}

	return s.updateServiceUsageWithTimeout(
		"stash",
		func(ctx context.Context) ([]serviceFile, error) {
			client := api.NewStashClient(s.config.Services.Stash.URL, s.config.Services.Stash.APIKey, s.config.APITimeout)
			files, err := client.GetAllFiles(ctx)
			if err != nil {
				return nil, err
			}
			serviceFiles := make([]serviceFile, len(files))
			for i, f := range files {
				serviceFiles[i] = stashServiceFile{f}
			}
			return serviceFiles, nil
		},
	)
}

// updateServiceUsageWithTimeout is a generic helper to update service usage with timeout handling
// This eliminates duplication across all service update methods
func (s *Scanner) updateServiceUsageWithTimeout(serviceName string, getFiles func(context.Context) ([]serviceFile, error)) error {
	// Use scan context if available (for cancellation during full scans), otherwise use Background (for manual updates)
	baseCtx := s.scanCtx
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	ctx, cancel := context.WithTimeout(baseCtx, s.config.APITimeout*constants.MaxAPITimeoutMultiplier)
	defer cancel()

	resultChan := make(chan error, 1)
	go func() {
		files, err := getFiles(ctx)
		if err != nil {
			resultChan <- err
			return
		}
		resultChan <- s.updateServiceUsage(ctx, serviceName, files)
	}()

	select {
	case <-ctx.Done():
		// Check if cancellation was due to scan cancellation or timeout
		if baseCtx.Err() == context.Canceled {
			return fmt.Errorf("%s update cancelled", serviceName)
		}
		return fmt.Errorf("%s request timed out after %v", serviceName, s.config.APITimeout*constants.MaxAPITimeoutMultiplier)
	case err := <-resultChan:
		return err
	}
}

// GetProgress returns the current scan progress
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

// UpdateAllServices manually updates all service usage information
// This can be called independently without a full scan
func (s *Scanner) UpdateAllServices() error {
	// Create temporary progress for logging
	tempProgress := NewProgress()
	tempProgress.SetPhase("Updating services")
	originalProgress := s.progress
	s.progress = tempProgress
	defer func() {
		s.progress = originalProgress
		tempProgress.Stop()
	}()

	s.progress.Log("Manually updating all services...")

	// Update each service
	s.progress.Log("Checking Plex...")
	if err := s.updatePlexUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Plex usage: %v", err))
	}

	s.progress.Log("Checking Sonarr...")
	if err := s.updateSonarrUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Sonarr usage: %v", err))
	}

	s.progress.Log("Checking Radarr...")
	if err := s.updateRadarrUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Radarr usage: %v", err))
	}

	s.progress.Log("Checking qBittorrent...")
	if err := s.updateQBittorrentUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update qBittorrent usage: %v", err))
	}

	// Update orphaned status after service checks
	s.progress.Log("Recalculating orphaned status...")
	if err := s.db.UpdateOrphanedStatus(context.Background()); err != nil {
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

	s.progress.Log("All services updated successfully!")
	return nil
}

// UpdateSingleService manually updates a specific service's usage information
// serviceName should be one of: plex, sonarr, radarr, qbittorrent
func (s *Scanner) UpdateSingleService(serviceName string) error {
	// Create temporary progress for logging
	tempProgress := NewProgress()
	tempProgress.SetPhase(fmt.Sprintf("Updating %s", serviceName))
	originalProgress := s.progress
	s.progress = tempProgress
	defer func() {
		s.progress = originalProgress
		tempProgress.Stop()
	}()

	s.progress.Log(fmt.Sprintf("Manually updating %s...", serviceName))

	var err error
	switch serviceName {
	case "plex":
		err = s.updatePlexUsage()
	case "sonarr":
		err = s.updateSonarrUsage()
	case "radarr":
		err = s.updateRadarrUsage()
	case "qbittorrent":
		err = s.updateQBittorrentUsage()
	case "stash":
		err = s.updateStashUsage()
	default:
		return fmt.Errorf("unknown service: %s", serviceName)
	}

	if err != nil {
		return fmt.Errorf("failed to update %s usage: %w", serviceName, err)
	}

	// Update orphaned status after service check
	s.progress.Log("Recalculating orphaned status...")
	if err := s.db.UpdateOrphanedStatus(context.Background()); err != nil {
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

	s.progress.Log(fmt.Sprintf("%s updated successfully!", serviceName))
	return nil
}

// RecalculateOrphanedStatus manually recalculates which files are orphaned
// This can be called independently without updating services
func (s *Scanner) RecalculateOrphanedStatus() error {
	// Create temporary progress for logging
	tempProgress := NewProgress()
	tempProgress.SetPhase("Recalculating orphaned status")
	originalProgress := s.progress
	s.progress = tempProgress
	defer func() {
		s.progress = originalProgress
		tempProgress.Stop()
	}()

	s.progress.Log("Manually recalculating orphaned status...")

	if err := s.db.UpdateOrphanedStatus(context.Background()); err != nil {
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

	s.progress.Log("Orphaned status recalculated successfully!")
	return nil
}

// ScanDiskLocations scans configured disks and populates disk-specific file locations
// This enables cross-disk duplicate detection while maintaining FUSE paths as canonical
func (s *Scanner) ScanDiskLocations(detector *disk.Detector) error {
	// Check if disks are configured
	if len(s.config.Disks) == 0 {
		return fmt.Errorf("no disks configured - disk scanning not available")
	}

	// Check if there's already a running scan
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		return fmt.Errorf("failed to check for running scan: %w", err)
	}

	if currentScan != nil {
		return fmt.Errorf("cannot start disk scan while another scan is running (ID: %d)", currentScan.ID)
	}

	// Create scan record
	scan, err := s.db.CreateScan("disk_location")
	if err != nil {
		return fmt.Errorf("failed to create disk scan record: %w", err)
	}

	// Initialize progress tracker
	s.diskScanProgress = NewProgress()
	s.diskScanProgress.SetPhase("Initializing")
	s.diskScanProgress.Log("Starting disk location scan...")

	// Create disk scanner
	diskScanner := NewDiskScanner(context.Background(), s.config, s.db, detector, s.diskScanProgress)

	// Update phase to loading cache
	s.diskScanProgress.SetPhase("Loading File Cache")
	if err := s.db.UpdateScanPhase(scan.ID, "Loading File Cache"); err != nil {
		log.Printf("Warning: failed to update scan phase: %v", err)
	}

	// Run disk scan
	err = diskScanner.ScanDiskLocations()

	// Determine final status
	status := "completed"
	if err != nil {
		status = "failed"
		s.diskScanProgress.SetPhase("Failed")
		s.diskScanProgress.Log(fmt.Sprintf("Disk scan failed: %v", err))
	} else {
		s.diskScanProgress.SetPhase("Completed")
		s.diskScanProgress.Log("Disk location scanning completed successfully")
	}

	// Update scan record with final status
	var errorMsg *string
	if err != nil {
		msg := err.Error()
		errorMsg = &msg
	}
	if updateErr := s.db.UpdateScan(scan.ID, status, diskScanner.filesScanned, errorMsg); updateErr != nil {
		log.Printf("Warning: failed to update scan record: %v", updateErr)
	}

	// Stop progress
	s.diskScanProgress.Stop()

	// Call onScanComplete callback to invalidate stats cache
	if s.onScanComplete != nil {
		s.onScanComplete()
	}

	// Clear progress reference
	s.diskScanProgress = nil

	if err != nil {
		return fmt.Errorf("disk scanning failed: %w", err)
	}

	return nil
}
