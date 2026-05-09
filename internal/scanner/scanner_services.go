package scanner

import (
	"context"
	"fmt"
	"log"

	"github.com/mmenanno/media-usage-finder/internal/api"
	"github.com/mmenanno/media-usage-finder/internal/constants"
	"github.com/mmenanno/media-usage-finder/internal/database"
)

type serviceFile interface {
	GetPath() string
	GetMetadata() map[string]interface{}
}

// Implement serviceFile for each service type
type plexServiceFile struct{ api.PlexFile }

func (f plexServiceFile) GetPath() string { return f.Path }
func (f plexServiceFile) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"size":         f.Size,
		"library_name": f.LibraryName,
		"title":        f.Title,
	}
}

type sonarrServiceFile struct{ api.SonarrFile }

func (f sonarrServiceFile) GetPath() string { return f.Path }
func (f sonarrServiceFile) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"series_title":   f.SeriesTitle,
		"season_number":  f.SeasonNumber,
		"episode_number": f.EpisodeNumber,
		"episode_id":     f.EpisodeID,
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
		"category":     f.Category,
		"tags":         f.Tags,
	}
}

type stashServiceFile struct{ api.StashFile }

func (f stashServiceFile) GetPath() string { return f.Path }
func (f stashServiceFile) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"scene_id": f.SceneID,
		"title":    f.Title,
		"studio":   f.Studio,
	}
}

type calibreServiceFile struct{ api.CalibreFile }

func (f calibreServiceFile) GetPath() string { return f.Path }
func (f calibreServiceFile) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"book_id":      f.BookID,
		"title":        f.Title,
		"author":       f.Author,
		"series":       f.Series,
		"series_index": f.SeriesIndex,
		"format":       f.Format,
	}
}

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

	// Get scan ID from progress tracker (if available)
	var scanID int64
	if s.progress != nil {
		scanID = s.progress.GetScanID()
	}

	// Collect usage records and track missing files
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

			// Track missing file in database if we have a scan ID
			if scanID > 0 {
				metadata := file.GetMetadata()

				// Extract service-specific grouping information
				var serviceGroup, serviceGroupID string
				var size int64

				switch serviceName {
				case "qbittorrent":
					if name, ok := metadata["torrent_name"].(string); ok {
						serviceGroup = name
					}
					if hash, ok := metadata["torrent_hash"].(string); ok {
						serviceGroupID = hash
					}
					if s, ok := metadata["size"].(int64); ok {
						size = s
					}
				case "stash":
					if title, ok := metadata["title"].(string); ok {
						serviceGroup = title
					}
					if sceneID, ok := metadata["scene_id"].(string); ok {
						serviceGroupID = sceneID
					}
					if s, ok := metadata["size"].(int64); ok {
						size = s
					}
				case "sonarr":
					if seriesTitle, ok := metadata["series_title"].(string); ok {
						serviceGroup = seriesTitle
					}
					if episodeID, ok := metadata["episode_id"].(float64); ok {
						serviceGroupID = fmt.Sprintf("%d", int64(episodeID))
					}
					if s, ok := metadata["size"].(int64); ok {
						size = s
					}
				case "radarr":
					if movieTitle, ok := metadata["movie_title"].(string); ok {
						serviceGroup = movieTitle
					}
					if movieID, ok := metadata["movie_id"].(float64); ok {
						serviceGroupID = fmt.Sprintf("%d", int64(movieID))
					}
					if s, ok := metadata["size"].(int64); ok {
						size = s
					}
				case "plex":
					// Plex doesn't have grouping info in metadata typically
					if s, ok := metadata["size"].(int64); ok {
						size = s
					}
				}

				missingFile := &database.MissingFile{
					ScanID:         scanID,
					Service:        serviceName,
					ServicePath:    file.GetPath(),
					TranslatedPath: hostPath,
					Size:           size,
					ServiceGroup:   serviceGroup,
					ServiceGroupID: serviceGroupID,
					Metadata:       metadata,
				}

				if err := s.db.InsertMissingFile(ctx, missingFile); err != nil {
					log.Printf("Warning: Failed to insert missing file record: %v", err)
				}
			}

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

// updateServiceUsageForPaths updates usage for a service, filtering to only specified paths
// This is used by RescanFiles to update only the rescanned files
func (s *Scanner) updateServiceUsageForPaths(ctx context.Context, serviceName string, files []serviceFile, pathFilter map[string]bool) error {
	if len(files) == 0 {
		log.Printf("%s: No files returned from service", serviceName)
		return nil
	}

	log.Printf("%s: Starting filtered update with %d files from service", serviceName, len(files))

	// Delete usage records only for the filtered paths
	// We need to get file IDs for the paths first
	paths := make([]string, 0, len(pathFilter))
	for path := range pathFilter {
		paths = append(paths, path)
	}

	dbFilesMap, err := s.db.GetFilesByPaths(ctx, paths)
	if err != nil {
		return fmt.Errorf("failed to get files for deletion: %w", err)
	}

	// Delete existing usage records for these specific files and service
	for _, dbFile := range dbFilesMap {
		// Delete only for this service and file
		if _, err := s.db.Conn().Exec("DELETE FROM usage WHERE file_id = ? AND service = ?", dbFile.ID, serviceName); err != nil {
			log.Printf("Warning: Failed to delete usage for file ID %d: %v", dbFile.ID, err)
		}
	}

	log.Printf("%s: Cleared old usage records for %d files", serviceName, len(dbFilesMap))

	// Translate all paths and collect for batch lookup - filter to only paths we care about
	hostPaths := make([]string, 0)
	pathToFile := make(map[string]serviceFile)

	for _, file := range files {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		originalPath := file.GetPath()
		hostPath := s.config.TranslatePathToHost(originalPath, serviceName)

		// Only include if this path is in our filter
		if pathFilter[hostPath] {
			hostPaths = append(hostPaths, hostPath)
			pathToFile[hostPath] = file
		}
	}

	if len(hostPaths) == 0 {
		log.Printf("%s: No files matched the path filter", serviceName)
		return nil
	}

	log.Printf("%s: Filtered to %d paths, querying database...", serviceName, len(hostPaths))

	// Batch load filtered files from database
	dbFiles, err := s.db.GetFilesByPaths(ctx, hostPaths)
	if err != nil {
		return fmt.Errorf("failed to batch load files: %w", err)
	}

	log.Printf("%s: Found %d files in database out of %d filtered", serviceName, len(dbFiles), len(hostPaths))

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
			log.Printf("%s: File not in database (may have been deleted): %s", serviceName, hostPath)
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
	total := len(pathToFile)
	s.progress.Log(fmt.Sprintf("%s: matched %d of %d filtered files",
		serviceName, matched, total))
	return nil
}

// updateAllServicesForPaths queries all configured services and updates usage for specific paths only
// This is used by RescanFiles to avoid querying all files from services
func (s *Scanner) updateAllServicesForPaths(ctx context.Context, scanID int64, pathFilter map[string]bool) error {
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

	if totalServices == 0 {
		s.progress.Log("No services configured, skipping service updates")
		return nil
	}

	currentService := 0
	s.progress.SetPhase(fmt.Sprintf("Querying %d Services", totalServices))

	// Update Plex if configured
	if s.config.Services.Plex.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.progress.Log("Querying Plex for tracked files...")

		client := api.NewPlexClient(s.config.Services.Plex.URL, s.config.Services.Plex.Token, s.config.APITimeout)
		plexFiles, err := client.GetAllFiles(ctx, s.config.Services.Plex.Libraries)
		if err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to query Plex: %v", err))
		} else {
			wrappedFiles := make([]serviceFile, len(plexFiles))
			for i, f := range plexFiles {
				wrappedFiles[i] = plexServiceFile{f}
			}
			if err := s.updateServiceUsageForPaths(ctx, "plex", wrappedFiles, pathFilter); err != nil {
				s.progress.Log(fmt.Sprintf("Warning: Failed to update Plex usage: %v", err))
			}
		}
	}

	// Update Sonarr if configured
	if s.config.Services.Sonarr.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.progress.Log("Querying Sonarr for tracked files...")

		client := api.NewSonarrClient(s.config.Services.Sonarr.URL, s.config.Services.Sonarr.APIKey, s.config.APITimeout)
		sonarrFiles, err := client.GetAllFiles(ctx)
		if err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to query Sonarr: %v", err))
		} else {
			wrappedFiles := make([]serviceFile, len(sonarrFiles))
			for i, f := range sonarrFiles {
				wrappedFiles[i] = sonarrServiceFile{f}
			}
			if err := s.updateServiceUsageForPaths(ctx, "sonarr", wrappedFiles, pathFilter); err != nil {
				s.progress.Log(fmt.Sprintf("Warning: Failed to update Sonarr usage: %v", err))
			}
		}
	}

	// Update Radarr if configured
	if s.config.Services.Radarr.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.progress.Log("Querying Radarr for tracked files...")

		client := api.NewRadarrClient(s.config.Services.Radarr.URL, s.config.Services.Radarr.APIKey, s.config.APITimeout)
		radarrFiles, err := client.GetAllFiles(ctx)
		if err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to query Radarr: %v", err))
		} else {
			wrappedFiles := make([]serviceFile, len(radarrFiles))
			for i, f := range radarrFiles {
				wrappedFiles[i] = radarrServiceFile{f}
			}
			if err := s.updateServiceUsageForPaths(ctx, "radarr", wrappedFiles, pathFilter); err != nil {
				s.progress.Log(fmt.Sprintf("Warning: Failed to update Radarr usage: %v", err))
			}
		}
	}

	// Update qBittorrent if configured
	if s.config.Services.QBittorrent.URL != "" || s.config.Services.QBittorrent.QuiProxyURL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.progress.Log("Querying qBittorrent for tracked files...")

		client := api.NewQBittorrentClient(s.config.Services.QBittorrent.URL, s.config.Services.QBittorrent.QuiProxyURL, s.config.Services.QBittorrent.Username, s.config.Services.QBittorrent.Password, s.config.APITimeout)
		qbtFiles, err := client.GetAllFiles(ctx)
		if err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to query qBittorrent: %v", err))
		} else {
			wrappedFiles := make([]serviceFile, len(qbtFiles))
			for i, f := range qbtFiles {
				wrappedFiles[i] = qbittorrentServiceFile{f}
			}
			if err := s.updateServiceUsageForPaths(ctx, "qbittorrent", wrappedFiles, pathFilter); err != nil {
				s.progress.Log(fmt.Sprintf("Warning: Failed to update qBittorrent usage: %v", err))
			}
		}
	}

	// Update Stash if configured
	if s.config.Services.Stash.URL != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.progress.Log("Querying Stash for tracked files...")

		client := api.NewStashClient(s.config.Services.Stash.URL, s.config.Services.Stash.APIKey, s.config.APITimeout)
		stashFiles, err := client.GetAllFiles(ctx)
		if err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to query Stash: %v", err))
		} else {
			wrappedFiles := make([]serviceFile, len(stashFiles))
			for i, f := range stashFiles {
				wrappedFiles[i] = stashServiceFile{f}
			}
			if err := s.updateServiceUsageForPaths(ctx, "stash", wrappedFiles, pathFilter); err != nil {
				s.progress.Log(fmt.Sprintf("Warning: Failed to update Stash usage: %v", err))
			}
		}
	}

	// Update Calibre if configured
	if s.config.Services.Calibre.LibraryPath != "" && s.config.Services.Calibre.DBPath != "" {
		currentService++
		s.progress.SetServiceProgress(currentService, totalServices)
		s.progress.Log("Querying Calibre for tracked files...")

		client := api.NewCalibreClient(s.config.Services.Calibre.LibraryPath, s.config.Services.Calibre.DBPath, s.config.APITimeout)
		calibreFiles, err := client.GetAllFiles(ctx)
		if err != nil {
			s.progress.Log(fmt.Sprintf("Warning: Failed to query Calibre: %v", err))
		} else {
			wrappedFiles := make([]serviceFile, len(calibreFiles))
			for i, f := range calibreFiles {
				wrappedFiles[i] = calibreServiceFile{f}
			}
			if err := s.updateServiceUsageForPaths(ctx, "calibre", wrappedFiles, pathFilter); err != nil {
				s.progress.Log(fmt.Sprintf("Warning: Failed to update Calibre usage: %v", err))
			}
		}
	}

	s.progress.Log(fmt.Sprintf("Completed querying %d services", totalServices))
	return nil
}

// scanFilesystem scans the filesystem and processes files
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
func (s *Scanner) UpdateAllServices() error {
	// Create scan record
	scan, err := s.db.CreateScan("service_update_all")
	if err != nil {
		return fmt.Errorf("failed to create scan record: %w", err)
	}

	// Create temporary progress for logging
	tempProgress := NewProgress(scan.ID, s.db)
	tempProgress.SetPhase("Updating all services")
	originalProgress := s.progress
	s.progress = tempProgress
	defer func() {
		s.progress = originalProgress
		tempProgress.Stop()
	}()

	s.progress.Log("Manually updating all services...")

	// Track if we had any errors
	hadErrors := false

	// Update each service
	s.progress.Log("Checking Plex...")
	if err := s.updatePlexUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Plex usage: %v", err))
		hadErrors = true
	}

	s.progress.Log("Checking Sonarr...")
	if err := s.updateSonarrUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Sonarr usage: %v", err))
		hadErrors = true
	}

	s.progress.Log("Checking Radarr...")
	if err := s.updateRadarrUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Radarr usage: %v", err))
		hadErrors = true
	}

	s.progress.Log("Checking qBittorrent...")
	if err := s.updateQBittorrentUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update qBittorrent usage: %v", err))
		hadErrors = true
	}

	s.progress.Log("Checking Stash...")
	if err := s.updateStashUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Stash usage: %v", err))
		hadErrors = true
	}

	s.progress.Log("Checking Calibre...")
	if err := s.updateCalibreUsage(); err != nil {
		s.progress.Log(fmt.Sprintf("Warning: Failed to update Calibre usage: %v", err))
		hadErrors = true
	}

	// Update orphaned status after service checks
	s.progress.Log("Recalculating orphaned status...")
	if err := s.db.UpdateOrphanedStatus(context.Background()); err != nil {
		// Mark scan as failed
		s.db.UpdateScanStatus(scan.ID, "failed", fmt.Sprintf("Failed to update orphaned status: %v", err))
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

	// Complete scan record
	status := "completed"
	if hadErrors {
		status = "completed_with_errors"
	}
	if err := s.db.CompleteScan(scan.ID, status, ""); err != nil {
		log.Printf("Warning: Failed to complete scan record: %v", err)
	}

	s.progress.Log("All services updated successfully!")
	return nil
}

// UpdateSingleService manually updates a specific service's usage information
// serviceName should be one of: plex, sonarr, radarr, qbittorrent
func (s *Scanner) UpdateSingleService(serviceName string) error {
	// Create scan record
	scanType := fmt.Sprintf("service_update_%s", serviceName)
	scan, err := s.db.CreateScan(scanType)
	if err != nil {
		return fmt.Errorf("failed to create scan record: %w", err)
	}

	// Create temporary progress for logging
	tempProgress := NewProgress(scan.ID, s.db)
	tempProgress.SetPhase(fmt.Sprintf("Updating %s", serviceName))
	originalProgress := s.progress
	s.progress = tempProgress
	defer func() {
		s.progress = originalProgress
		tempProgress.Stop()
	}()

	s.progress.Log(fmt.Sprintf("Manually updating %s...", serviceName))

	var updateErr error
	switch serviceName {
	case "plex":
		updateErr = s.updatePlexUsage()
	case "sonarr":
		updateErr = s.updateSonarrUsage()
	case "radarr":
		updateErr = s.updateRadarrUsage()
	case "qbittorrent":
		updateErr = s.updateQBittorrentUsage()
	case "stash":
		updateErr = s.updateStashUsage()
	case "calibre":
		updateErr = s.updateCalibreUsage()
	default:
		errMsg := fmt.Sprintf("unknown service: %s", serviceName)
		s.db.UpdateScanStatus(scan.ID, "failed", errMsg)
		return fmt.Errorf("unknown service: %s", serviceName)
	}

	if updateErr != nil {
		// Mark scan as failed
		s.db.UpdateScanStatus(scan.ID, "failed", fmt.Sprintf("Failed to update %s: %v", serviceName, updateErr))
		return fmt.Errorf("failed to update %s usage: %w", serviceName, updateErr)
	}

	// Update orphaned status after service check
	s.progress.Log("Recalculating orphaned status...")
	if err := s.db.UpdateOrphanedStatus(context.Background()); err != nil {
		// Mark scan as failed
		s.db.UpdateScanStatus(scan.ID, "failed", fmt.Sprintf("Failed to update orphaned status: %v", err))
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

	// Complete scan record
	if err := s.db.CompleteScan(scan.ID, "completed", ""); err != nil {
		log.Printf("Warning: Failed to complete scan record: %v", err)
	}

	s.progress.Log(fmt.Sprintf("%s updated successfully!", serviceName))
	return nil
}

// RecalculateOrphanedStatus manually recalculates which files are orphaned
// This can be called independently without updating services
