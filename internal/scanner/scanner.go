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
)

// Scanner coordinates the entire scanning process
type Scanner struct {
	db             *database.DB
	config         *config.Config
	progress       *Progress
	cancel         context.CancelFunc
	onScanComplete func() // Callback when scan completes
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

	return scanErr
}

// runScan performs the actual scanning work
func (s *Scanner) runScan(ctx context.Context, scanID int64, incremental bool) error {
	// Phase 1: Count files
	s.updatePhase(scanID, "Counting files")
	s.progress.Log("Counting files...")

	totalFiles, err := CountFiles(s.config.ScanPaths)
	if err != nil {
		return fmt.Errorf("failed to count files: %w", err)
	}

	s.progress.SetTotalFiles(totalFiles)
	s.progress.Log(fmt.Sprintf("Found %d files to scan", totalFiles))

	// Phase 2: Scan filesystem
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
	s.updatePhase(scanID, "Checking Plex")
	s.progress.Log("Querying Plex for tracked files...")
	if err := s.updatePlexUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Plex usage: %v", err))
	}

	s.updatePhase(scanID, "Checking Sonarr")
	s.progress.Log("Querying Sonarr for tracked files...")
	if err := s.updateSonarrUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Sonarr usage: %v", err))
	}

	s.updatePhase(scanID, "Checking Radarr")
	s.progress.Log("Querying Radarr for tracked files...")
	if err := s.updateRadarrUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Radarr usage: %v", err))
	}

	s.updatePhase(scanID, "Checking qBittorrent")
	s.progress.Log("Querying qBittorrent for tracked files...")
	if err := s.updateQBittorrentUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update qBittorrent usage: %v", err))
	}

	// Phase 4: Update orphaned status
	s.updatePhase(scanID, "Updating orphaned status")
	s.progress.Log("Calculating orphaned file status...")

	if err := s.db.UpdateOrphanedStatus(); err != nil {
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

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

// updateServiceUsage is a generic method to update usage information for any service
func (s *Scanner) updateServiceUsage(serviceName string, files []serviceFile) error {
	if len(files) == 0 {
		return nil
	}

	// Clear old usage records
	if err := s.db.DeleteUsageByService(serviceName); err != nil {
		return err
	}

	// Translate all paths and collect for batch lookup
	hostPaths := make([]string, 0, len(files))
	pathToFile := make(map[string]serviceFile)

	for _, file := range files {
		hostPath := s.config.TranslatePathToHost(file.GetPath(), serviceName)
		hostPaths = append(hostPaths, hostPath)
		pathToFile[hostPath] = file
	}

	// Batch load all files from database
	dbFiles, err := s.db.GetFilesByPaths(hostPaths)
	if err != nil {
		return fmt.Errorf("failed to batch load files: %w", err)
	}

	// Collect usage records
	var usages []*database.Usage
	for hostPath, file := range pathToFile {
		dbFile, ok := dbFiles[hostPath]
		if !ok {
			continue
		}

		usages = append(usages, &database.Usage{
			FileID:        dbFile.ID,
			Service:       serviceName,
			ReferencePath: file.GetPath(),
			Metadata:      file.GetMetadata(),
		})
	}

	// Batch insert all usage records
	if len(usages) > 0 {
		if err := s.db.BatchUpsertUsage(usages); err != nil {
			return fmt.Errorf("failed to batch insert %s usage: %w", serviceName, err)
		}
	}

	matched := len(usages)
	total := len(files)
	s.progress.Log(fmt.Sprintf("%s: matched %d of %d files (%d not found in filesystem)",
		serviceName, matched, total, total-matched))
	return nil
}

// scanFilesystem scans the filesystem and processes files
func (s *Scanner) scanFilesystem(ctx context.Context, scanID int64, incremental bool) error {
	// Create worker pool with configurable buffer size
	pool := NewWorkerPool(s.config.ScanWorkers, s.config.ScanBufferSize, s.db, scanID, s.progress, incremental)
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
		func() ([]serviceFile, error) {
			client := api.NewPlexClient(s.config.Services.Plex.URL, s.config.Services.Plex.Token, s.config.APITimeout)
			files, err := client.GetAllFiles()
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
		func() ([]serviceFile, error) {
			client := api.NewSonarrClient(s.config.Services.Sonarr.URL, s.config.Services.Sonarr.APIKey, s.config.APITimeout)
			files, err := client.GetAllFiles()
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
		func() ([]serviceFile, error) {
			client := api.NewRadarrClient(s.config.Services.Radarr.URL, s.config.Services.Radarr.APIKey, s.config.APITimeout)
			files, err := client.GetAllFiles()
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
		func() ([]serviceFile, error) {
			client := api.NewQBittorrentClient(qbConfig.URL, qbConfig.Username, qbConfig.Password, qbConfig.QuiProxyURL, s.config.APITimeout)
			files, err := client.GetAllFiles()
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

// updateServiceUsageWithTimeout is a generic helper to update service usage with timeout handling
// This eliminates duplication across all service update methods
func (s *Scanner) updateServiceUsageWithTimeout(serviceName string, getFiles func() ([]serviceFile, error)) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.APITimeout*constants.MaxAPITimeoutMultiplier)
	defer cancel()

	resultChan := make(chan error, 1)
	go func() {
		files, err := getFiles()
		if err != nil {
			resultChan <- err
			return
		}
		resultChan <- s.updateServiceUsage(serviceName, files)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("%s request timed out after %v", serviceName, s.config.APITimeout*constants.MaxAPITimeoutMultiplier)
	case err := <-resultChan:
		return err
	}
}

// GetProgress returns the current scan progress
func (s *Scanner) GetProgress() *Progress {
	return s.progress
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
	if err := s.db.UpdateOrphanedStatus(); err != nil {
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
	default:
		return fmt.Errorf("unknown service: %s", serviceName)
	}

	if err != nil {
		return fmt.Errorf("failed to update %s usage: %w", serviceName, err)
	}

	// Update orphaned status after service check
	s.progress.Log("Recalculating orphaned status...")
	if err := s.db.UpdateOrphanedStatus(); err != nil {
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

	if err := s.db.UpdateOrphanedStatus(); err != nil {
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

	s.progress.Log("Orphaned status recalculated successfully!")
	return nil
}
