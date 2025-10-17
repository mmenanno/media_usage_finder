package scanner

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/api"
	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/database"
)

// Scanner coordinates the entire scanning process
type Scanner struct {
	db       *database.DB
	config   *config.Config
	progress *Progress
	cancel   context.CancelFunc
}

// NewScanner creates a new scanner
func NewScanner(db *database.DB, cfg *config.Config) *Scanner {
	return &Scanner{
		db:     db,
		config: cfg,
	}
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

	return scanErr
}

// runScan performs the actual scanning work
func (s *Scanner) runScan(ctx context.Context, scanID int64, incremental bool) error {
	// Phase 1: Count files
	s.progress.SetPhase("Counting files")
	log.Println("Counting files...")

	totalFiles, err := CountFiles(s.config.ScanPaths)
	if err != nil {
		return fmt.Errorf("failed to count files: %w", err)
	}

	s.progress.SetTotalFiles(totalFiles)
	log.Printf("Found %d files to scan\n", totalFiles)

	// Phase 2: Scan filesystem
	s.progress.SetPhase("Scanning filesystem")
	log.Println("Starting filesystem scan...")

	if err := s.scanFilesystem(ctx, scanID); err != nil {
		return fmt.Errorf("filesystem scan failed: %w", err)
	}

	// Phase 3: Update service usage
	s.progress.SetPhase("Checking Plex")
	if err := s.updatePlexUsage(); err != nil {
		log.Printf("Warning: Failed to update Plex usage: %v", err)
	}

	s.progress.SetPhase("Checking Sonarr")
	if err := s.updateSonarrUsage(); err != nil {
		log.Printf("Warning: Failed to update Sonarr usage: %v", err)
	}

	s.progress.SetPhase("Checking Radarr")
	if err := s.updateRadarrUsage(); err != nil {
		log.Printf("Warning: Failed to update Radarr usage: %v", err)
	}

	s.progress.SetPhase("Checking qBittorrent")
	if err := s.updateQBittorrentUsage(); err != nil {
		log.Printf("Warning: Failed to update qBittorrent usage: %v", err)
	}

	// Phase 4: Update orphaned status
	s.progress.SetPhase("Updating orphaned status")
	log.Println("Updating orphaned file status...")

	if err := s.db.UpdateOrphanedStatus(); err != nil {
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

	s.progress.SetPhase("Completed")
	log.Println("Scan completed successfully")

	return nil
}

// scanFilesystem scans the filesystem and processes files
func (s *Scanner) scanFilesystem(ctx context.Context, scanID int64) error {
	// Create worker pool
	pool := NewWorkerPool(s.config.ScanWorkers, s.db, scanID, s.progress)
	pool.Start()

	// Walk filesystem and feed workers
	go func() {
		if err := WalkFiles(s.config.ScanPaths, pool.GetInputChannel(), s.progress); err != nil {
			log.Printf("Error walking files: %v", err)
		}
	}()

	// Wait for completion or cancellation
	select {
	case <-ctx.Done():
		pool.Cancel()
		return ctx.Err()
	case <-time.After(time.Until(time.Now().Add(24 * time.Hour))): // Max scan time
		pool.Stop()
	}

	return nil
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

	// Add new usage records
	for _, file := range files {
		// Translate Plex path to host path
		hostPath := s.config.TranslatePathToHost(file.Path, "plex")

		// Find file in database
		dbFile, err := s.db.GetFileByPath(hostPath)
		if err != nil {
			continue
		}

		usage := &database.Usage{
			FileID:        dbFile.ID,
			Service:       "plex",
			ReferencePath: file.Path,
			Metadata: map[string]interface{}{
				"size": file.Size,
			},
		}

		if err := s.db.UpsertUsage(usage); err != nil {
			log.Printf("Failed to upsert Plex usage for %s: %v", file.Path, err)
		}
	}

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

	// Add new usage records
	for _, file := range files {
		hostPath := s.config.TranslatePathToHost(file.Path, "sonarr")

		dbFile, err := s.db.GetFileByPath(hostPath)
		if err != nil {
			continue
		}

		metadata, _ := json.Marshal(map[string]interface{}{
			"series_title":  file.SeriesTitle,
			"season_number": file.SeasonNumber,
			"episode_id":    file.EpisodeID,
		})

		var metadataMap map[string]interface{}
		json.Unmarshal(metadata, &metadataMap)

		usage := &database.Usage{
			FileID:        dbFile.ID,
			Service:       "sonarr",
			ReferencePath: file.Path,
			Metadata:      metadataMap,
		}

		if err := s.db.UpsertUsage(usage); err != nil {
			log.Printf("Failed to upsert Sonarr usage for %s: %v", file.Path, err)
		}
	}

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

	// Add new usage records
	for _, file := range files {
		hostPath := s.config.TranslatePathToHost(file.Path, "radarr")

		dbFile, err := s.db.GetFileByPath(hostPath)
		if err != nil {
			continue
		}

		metadata, _ := json.Marshal(map[string]interface{}{
			"movie_title": file.MovieTitle,
			"movie_year":  file.MovieYear,
			"movie_id":    file.MovieID,
		})

		var metadataMap map[string]interface{}
		json.Unmarshal(metadata, &metadataMap)

		usage := &database.Usage{
			FileID:        dbFile.ID,
			Service:       "radarr",
			ReferencePath: file.Path,
			Metadata:      metadataMap,
		}

		if err := s.db.UpsertUsage(usage); err != nil {
			log.Printf("Failed to upsert Radarr usage for %s: %v", file.Path, err)
		}
	}

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

	// Add new usage records
	for _, file := range files {
		hostPath := s.config.TranslatePathToHost(file.Path, "qbittorrent")

		dbFile, err := s.db.GetFileByPath(hostPath)
		if err != nil {
			continue
		}

		metadata, _ := json.Marshal(map[string]interface{}{
			"torrent_hash": file.TorrentHash,
			"torrent_name": file.TorrentName,
		})

		var metadataMap map[string]interface{}
		json.Unmarshal(metadata, &metadataMap)

		usage := &database.Usage{
			FileID:        dbFile.ID,
			Service:       "qbittorrent",
			ReferencePath: file.Path,
			Metadata:      metadataMap,
		}

		if err := s.db.UpsertUsage(usage); err != nil {
			log.Printf("Failed to upsert qBittorrent usage for %s: %v", file.Path, err)
		}
	}

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
