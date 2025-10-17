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
	s.progress.SetPhase("Counting files")
	s.progress.Log("Counting files...")

	totalFiles, err := CountFiles(s.config.ScanPaths)
	if err != nil {
		return fmt.Errorf("failed to count files: %w", err)
	}

	s.progress.SetTotalFiles(totalFiles)
	s.progress.Log(fmt.Sprintf("Found %d files to scan", totalFiles))

	// Phase 2: Scan filesystem
	s.progress.SetPhase("Scanning filesystem")
	if incremental {
		s.progress.Log("Starting incremental filesystem scan (only changed files)...")
	} else {
		s.progress.Log("Starting full filesystem scan...")
	}

	if err := s.scanFilesystem(ctx, scanID, incremental); err != nil {
		return fmt.Errorf("filesystem scan failed: %w", err)
	}

	// Phase 3: Update service usage
	s.progress.SetPhase("Checking Plex")
	s.progress.Log("Querying Plex for tracked files...")
	if err := s.updatePlexUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Plex usage: %v", err))
	}

	s.progress.SetPhase("Checking Sonarr")
	s.progress.Log("Querying Sonarr for tracked files...")
	if err := s.updateSonarrUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Sonarr usage: %v", err))
	}

	s.progress.SetPhase("Checking Radarr")
	s.progress.Log("Querying Radarr for tracked files...")
	if err := s.updateRadarrUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Radarr usage: %v", err))
	}

	s.progress.SetPhase("Checking qBittorrent")
	s.progress.Log("Querying qBittorrent for tracked files...")
	if err := s.updateQBittorrentUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update qBittorrent usage: %v", err))
	}

	// Phase 4: Update orphaned status
	s.progress.SetPhase("Updating orphaned status")
	s.progress.Log("Calculating orphaned file status...")

	if err := s.db.UpdateOrphanedStatus(); err != nil {
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

	s.progress.SetPhase("Completed")
	s.progress.Log("Scan completed successfully!")

	return nil
}

// scanFilesystem scans the filesystem and processes files
func (s *Scanner) scanFilesystem(ctx context.Context, scanID int64, incremental bool) error {
	// Create worker pool
	pool := NewWorkerPool(s.config.ScanWorkers, s.db, scanID, s.progress, incremental)
	pool.Start()

	// Walk filesystem in goroutine
	walkDone := make(chan error, 1)
	go func() {
		walkDone <- WalkFiles(s.config.ScanPaths, pool.GetInputChannel(), s.progress)
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

	client := api.NewPlexClient(
		s.config.Services.Plex.URL,
		s.config.Services.Plex.Token,
		s.config.APITimeout,
	)

	files, err := client.GetAllFiles()
	if err != nil {
		return err
	}

	// Clear old usage records
	if err := s.db.DeleteUsageByService("plex"); err != nil {
		return err
	}

	// Translate all paths and collect for batch lookup
	hostPaths := make([]string, 0, len(files))
	pathToFile := make(map[string]api.PlexFile)

	for _, file := range files {
		hostPath := s.config.TranslatePathToHost(file.Path, "plex")
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
			Service:       "plex",
			ReferencePath: file.Path,
			Metadata: map[string]interface{}{
				"size": file.Size,
			},
		})
	}

	// Batch insert all usage records
	if len(usages) > 0 {
		if err := s.db.BatchUpsertUsage(usages); err != nil {
			return fmt.Errorf("failed to batch insert Plex usage: %w", err)
		}
	}

	matched := len(usages)
	total := len(files)
	s.progress.Log(fmt.Sprintf("Plex: matched %d of %d files (%d not found in filesystem)", matched, total, total-matched))
	return nil
}

// updateSonarrUsage updates usage information from Sonarr
func (s *Scanner) updateSonarrUsage() error {
	if s.config.Services.Sonarr.URL == "" {
		return nil
	}

	client := api.NewSonarrClient(
		s.config.Services.Sonarr.URL,
		s.config.Services.Sonarr.APIKey,
		s.config.APITimeout,
	)

	files, err := client.GetAllFiles()
	if err != nil {
		return err
	}

	// Clear old usage records
	if err := s.db.DeleteUsageByService("sonarr"); err != nil {
		return err
	}

	// Translate all paths and collect for batch lookup
	hostPaths := make([]string, 0, len(files))
	pathToFile := make(map[string]api.SonarrFile)

	for _, file := range files {
		hostPath := s.config.TranslatePathToHost(file.Path, "sonarr")
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
			Service:       "sonarr",
			ReferencePath: file.Path,
			Metadata: map[string]interface{}{
				"series_title":  file.SeriesTitle,
				"season_number": file.SeasonNumber,
				"episode_id":    file.EpisodeID,
			},
		})
	}

	// Batch insert all usage records
	if len(usages) > 0 {
		if err := s.db.BatchUpsertUsage(usages); err != nil {
			return fmt.Errorf("failed to batch insert Sonarr usage: %w", err)
		}
	}

	matched := len(usages)
	total := len(files)
	s.progress.Log(fmt.Sprintf("Sonarr: matched %d of %d files (%d not found in filesystem)", matched, total, total-matched))
	return nil
}

// updateRadarrUsage updates usage information from Radarr
func (s *Scanner) updateRadarrUsage() error {
	if s.config.Services.Radarr.URL == "" {
		return nil
	}

	client := api.NewRadarrClient(
		s.config.Services.Radarr.URL,
		s.config.Services.Radarr.APIKey,
		s.config.APITimeout,
	)

	files, err := client.GetAllFiles()
	if err != nil {
		return err
	}

	// Clear old usage records
	if err := s.db.DeleteUsageByService("radarr"); err != nil {
		return err
	}

	// Translate all paths and collect for batch lookup
	hostPaths := make([]string, 0, len(files))
	pathToFile := make(map[string]api.RadarrFile)

	for _, file := range files {
		hostPath := s.config.TranslatePathToHost(file.Path, "radarr")
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
			Service:       "radarr",
			ReferencePath: file.Path,
			Metadata: map[string]interface{}{
				"movie_title": file.MovieTitle,
				"movie_year":  file.MovieYear,
				"movie_id":    file.MovieID,
			},
		})
	}

	// Batch insert all usage records
	if len(usages) > 0 {
		if err := s.db.BatchUpsertUsage(usages); err != nil {
			return fmt.Errorf("failed to batch insert Radarr usage: %w", err)
		}
	}

	matched := len(usages)
	total := len(files)
	s.progress.Log(fmt.Sprintf("Radarr: matched %d of %d files (%d not found in filesystem)", matched, total, total-matched))
	return nil
}

// updateQBittorrentUsage updates usage information from qBittorrent
func (s *Scanner) updateQBittorrentUsage() error {
	qbConfig := s.config.Services.QBittorrent
	if qbConfig.URL == "" && qbConfig.QuiProxyURL == "" {
		return nil
	}

	client := api.NewQBittorrentClient(
		qbConfig.URL,
		qbConfig.Username,
		qbConfig.Password,
		qbConfig.QuiProxyURL,
		s.config.APITimeout,
	)

	files, err := client.GetAllFiles()
	if err != nil {
		return err
	}

	// Clear old usage records
	if err := s.db.DeleteUsageByService("qbittorrent"); err != nil {
		return err
	}

	// Translate all paths and collect for batch lookup
	hostPaths := make([]string, 0, len(files))
	pathToFile := make(map[string]api.QBittorrentFile)

	for _, file := range files {
		hostPath := s.config.TranslatePathToHost(file.Path, "qbittorrent")
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
			Service:       "qbittorrent",
			ReferencePath: file.Path,
			Metadata: map[string]interface{}{
				"torrent_hash": file.TorrentHash,
				"torrent_name": file.TorrentName,
			},
		})
	}

	// Batch insert all usage records
	if len(usages) > 0 {
		if err := s.db.BatchUpsertUsage(usages); err != nil {
			return fmt.Errorf("failed to batch insert qBittorrent usage: %w", err)
		}
	}

	matched := len(usages)
	total := len(files)
	s.progress.Log(fmt.Sprintf("qBittorrent: matched %d of %d files (%d not found in filesystem)", matched, total, total-matched))
	return nil
}

// GetProgress returns the current scan progress
func (s *Scanner) GetProgress() *Progress {
	return s.progress
}

// Cancel cancels the current scan
func (s *Scanner) Cancel() {
	if s.cancel != nil {
		s.cancel()
	}
}
