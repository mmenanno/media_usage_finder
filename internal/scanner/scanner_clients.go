package scanner

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/mmenanno/media-usage-finder/internal/api"
	"github.com/mmenanno/media-usage-finder/internal/database"
)

func (s *Scanner) updatePlexUsage() error {
	if s.config.Services.Plex.URL == "" {
		return nil
	}

	err := s.updateServiceUsageWithTimeout(
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

	if err != nil {
		return err
	}

	// Associate subtitle files with Plex media
	return s.associatePlexSubtitles()
}

// associatePlexSubtitles finds subtitle files associated with Plex media and marks them as used by Plex
func (s *Scanner) associatePlexSubtitles() error {
	ctx := context.Background()
	if s.scanCtx != nil {
		ctx = s.scanCtx
	}

	log.Printf("plex: Associating subtitle files with media")

	// Get all files currently used by Plex (these are the video files)
	plexVideoFiles, err := s.db.GetFilesByService(ctx, "plex")
	if err != nil {
		return fmt.Errorf("failed to get Plex video files: %w", err)
	}

	if len(plexVideoFiles) == 0 {
		log.Printf("plex: No video files found, skipping subtitle association")
		return nil
	}

	log.Printf("plex: Found %d video files, searching for associated subtitles", len(plexVideoFiles))

	// Build a map of video file basenames (dir + basename without extension) for quick lookup
	// Key: directory/basename (e.g., "/media/movies/Movie Name")
	videoBasenames := make(map[string]bool)
	for _, videoFile := range plexVideoFiles {
		dir := filepath.Dir(videoFile.Path)
		ext := filepath.Ext(videoFile.Path)
		basename := strings.TrimSuffix(filepath.Base(videoFile.Path), ext)
		key := filepath.Join(dir, basename)
		videoBasenames[key] = true
	}

	log.Printf("plex: Built lookup map with %d unique video basenames", len(videoBasenames))

	// Query for all subtitle files in the database (by extension)
	// Note: Extensions are stored WITH leading dot in database
	subtitleExts := []string{".srt", ".sub", ".sbv", ".ssa", ".ass", ".vtt"}
	allSubtitles, err := s.db.GetFilesByExtensions(ctx, subtitleExts)
	if err != nil {
		return fmt.Errorf("failed to query subtitle files: %w", err)
	}

	if len(allSubtitles) == 0 {
		log.Printf("plex: No subtitle files found in database")
		return nil
	}

	log.Printf("plex: Found %d subtitle files, matching to video files...", len(allSubtitles))

	// Match subtitle files to video files
	var matchedSubtitles []*database.File
	for _, subtitleFile := range allSubtitles {
		dir := filepath.Dir(subtitleFile.Path)
		filename := filepath.Base(subtitleFile.Path)
		ext := filepath.Ext(filename)
		nameWithoutExt := strings.TrimSuffix(filename, ext)

		// Try to extract the base video filename from the subtitle filename
		// Handle patterns like:
		// - Movie.srt -> Movie
		// - Movie.en.srt -> Movie
		// - Movie.1.en.srt -> Movie
		// - Movie.en.forced.srt -> Movie

		// Split by dots to find the base name
		parts := strings.Split(nameWithoutExt, ".")

		// Try progressively shorter basenames (from full to first part)
		for i := len(parts); i > 0; i-- {
			candidateBasename := strings.Join(parts[:i], ".")
			videoKey := filepath.Join(dir, candidateBasename)

			if videoBasenames[videoKey] {
				matchedSubtitles = append(matchedSubtitles, subtitleFile)
				break
			}
		}
	}

	if len(matchedSubtitles) == 0 {
		log.Printf("plex: No subtitle files matched to Plex videos")
		return nil
	}

	log.Printf("plex: Matched %d subtitle files to Plex videos, creating usage records", len(matchedSubtitles))

	// Create usage records for matched subtitle files
	var usages []*database.Usage
	for _, subtitleFile := range matchedSubtitles {
		usages = append(usages, &database.Usage{
			FileID:  subtitleFile.ID,
			Service: "plex",
		})
	}

	// Batch insert usage records
	if err := s.db.BatchUpsertUsage(ctx, usages); err != nil {
		return fmt.Errorf("failed to create subtitle usage records: %w", err)
	}

	log.Printf("plex: Successfully associated %d subtitle files with Plex media", len(usages))
	return nil
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

	err := s.updateServiceUsageWithTimeout(
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

	if err != nil {
		return err
	}

	// Associate incomplete download files (.!qB) with qBittorrent
	return s.associateQBittorrentIncompleteFiles()
}

// associateQBittorrentIncompleteFiles finds incomplete download files (.!qB) and marks them as used by qBittorrent
// qBittorrent adds .!qB extension to files during download, then removes it when complete
// The API reports the final filename (without .!qB), so we need to match incomplete files to active torrents
func (s *Scanner) associateQBittorrentIncompleteFiles() error {
	ctx := context.Background()
	if s.scanCtx != nil {
		ctx = s.scanCtx
	}

	log.Printf("qBittorrent: Associating incomplete download files (.!qB)")

	// Get all files currently tracked by qBittorrent (these are the final filenames)
	qbFiles, err := s.db.GetFilesByService(ctx, "qbittorrent")
	if err != nil {
		return fmt.Errorf("failed to get qBittorrent files: %w", err)
	}

	if len(qbFiles) == 0 {
		log.Printf("qBittorrent: No files found in active torrents, skipping .!qB association")
		return nil
	}

	log.Printf("qBittorrent: Found %d files in active torrents, searching for .!qB files", len(qbFiles))

	// Build a map of qBittorrent file paths for quick lookup
	qbFilePaths := make(map[string]bool)
	for _, qbFile := range qbFiles {
		qbFilePaths[qbFile.Path] = true
	}

	// Query for all .!qB files in the database
	// Note: Extensions are stored as compound extensions (.mkv.!qb, .mp4.!qb, etc.)
	// So we need to search for files where extension ends with .!qb
	incompleteFiles, err := s.db.GetFilesByExtensionSuffix(ctx, ".!qb")
	if err != nil {
		return fmt.Errorf("failed to query .!qB files: %w", err)
	}

	if len(incompleteFiles) == 0 {
		log.Printf("qBittorrent: No .!qB files found in database")
		return nil
	}

	log.Printf("qBittorrent: Found %d .!qB files, matching to active torrents...", len(incompleteFiles))

	// Match .!qB files to qBittorrent files by stripping the .!qb extension
	var matchedIncomplete []*database.File
	for _, incompleteFile := range incompleteFiles {
		// Strip .!qb extension to get the expected final filename
		// e.g., /downloads/Movie.mkv.!qb -> /downloads/Movie.mkv
		// Note: Extensions are stored lowercase, so trim lowercase .!qb
		expectedPath := strings.TrimSuffix(incompleteFile.Path, ".!qb")

		// Check if qBittorrent has this file in its active torrents
		if qbFilePaths[expectedPath] {
			matchedIncomplete = append(matchedIncomplete, incompleteFile)
		}
	}

	if len(matchedIncomplete) == 0 {
		log.Printf("qBittorrent: No .!qB files matched to active torrents (all may be orphaned)")
		return nil
	}

	log.Printf("qBittorrent: Matched %d .!qB files to active torrents, creating usage records", len(matchedIncomplete))

	// Create usage records for matched incomplete files
	var usages []*database.Usage
	for _, file := range matchedIncomplete {
		usages = append(usages, &database.Usage{
			FileID:  file.ID,
			Service: "qbittorrent",
			Metadata: map[string]interface{}{
				"status": "downloading",
				"type":   "incomplete",
			},
		})
	}

	// Batch upsert usage records
	if err := s.db.BatchUpsertUsage(ctx, usages); err != nil {
		return fmt.Errorf("failed to create usage records for .!qB files: %w", err)
	}

	log.Printf("qBittorrent: Successfully associated %d incomplete download files", len(usages))
	return nil
}

// associateStashGalleryImages finds image files in Stash gallery folders and marks them as used by Stash
// Stash gallery folders are tracked by folder path, but individual images aren't returned by the API
func (s *Scanner) associateStashGalleryImages() error {
	ctx := context.Background()
	if s.scanCtx != nil {
		ctx = s.scanCtx
	}

	log.Printf("stash: Associating gallery images with folders")

	// Query Stash API directly to get gallery folders
	// Note: Gallery folder paths are NOT in the database (only actual files are scanned)
	client := api.NewStashClient(s.config.Services.Stash.URL, s.config.Services.Stash.APIKey, s.config.APITimeout)
	stashFiles, err := client.GetAllFiles(ctx)
	if err != nil {
		return fmt.Errorf("failed to get Stash files from API: %w", err)
	}

	if len(stashFiles) == 0 {
		log.Printf("stash: No files found in Stash, skipping gallery image association")
		return nil
	}

	// Build a list of gallery folder paths and apply path mappings
	var galleryFolders []string
	for _, stashFile := range stashFiles {
		// Check if this is a folder path (no extension = folder)
		ext := filepath.Ext(stashFile.Path)
		if ext == "" {
			// This is a folder path - apply path mapping
			translatedPath := s.config.TranslatePathToHost(stashFile.Path, "stash")
			galleryFolders = append(galleryFolders, translatedPath)
		}
	}

	if len(galleryFolders) == 0 {
		log.Printf("stash: No gallery folders found in Stash, skipping image association")
		return nil
	}

	log.Printf("stash: Found %d gallery folders from Stash API, searching for images", len(galleryFolders))

	// Debug: Log first few gallery folders
	for i := 0; i < len(galleryFolders) && i < 3; i++ {
		log.Printf("stash: Sample gallery folder: %s", galleryFolders[i])
	}

	// Query for all image files in the database
	imageExts := []string{".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
	allImages, err := s.db.GetFilesByExtensions(ctx, imageExts)
	if err != nil {
		return fmt.Errorf("failed to query image files: %w", err)
	}

	if len(allImages) == 0 {
		log.Printf("stash: No image files found in database")
		return nil
	}

	log.Printf("stash: Found %d image files, matching to gallery folders...", len(allImages))

	// Debug: Log first few image paths
	for i := 0; i < len(allImages) && i < 3; i++ {
		log.Printf("stash: Sample image path: %s", allImages[i].Path)
	}

	// Match image files to gallery folders
	var matchedImages []*database.File
	for _, imageFile := range allImages {
		// Check if this image is inside any gallery folder
		for _, galleryFolder := range galleryFolders {
			if strings.HasPrefix(imageFile.Path, galleryFolder+"/") || strings.HasPrefix(imageFile.Path, galleryFolder+"\\") {
				matchedImages = append(matchedImages, imageFile)
				break
			}
		}
	}

	if len(matchedImages) == 0 {
		log.Printf("stash: No gallery images matched to folders")
		log.Printf("stash: Debug - this likely means gallery folders don't match image file paths")
		return nil
	}

	log.Printf("stash: Matched %d gallery images, creating usage records", len(matchedImages))

	// Create usage records for matched images
	var usages []*database.Usage
	for _, imageFile := range matchedImages {
		usages = append(usages, &database.Usage{
			FileID:  imageFile.ID,
			Service: "stash",
			Metadata: map[string]interface{}{
				"type": "gallery_image",
			},
		})
	}

	// Batch upsert usage records
	if err := s.db.BatchUpsertUsage(ctx, usages); err != nil {
		return fmt.Errorf("failed to create usage records for gallery images: %w", err)
	}

	log.Printf("stash: Successfully associated %d gallery images", len(usages))
	return nil
}

// updateStashUsage updates usage information from Stash
func (s *Scanner) updateStashUsage() error {
	if s.config.Services.Stash.URL == "" {
		return nil
	}

	err := s.updateServiceUsageWithTimeout(
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

	if err != nil {
		return err
	}

	// Associate gallery images with Stash
	return s.associateStashGalleryImages()
}

func (s *Scanner) updateCalibreUsage() error {
	if s.config.Services.Calibre.LibraryPath == "" || s.config.Services.Calibre.DBPath == "" {
		return nil
	}

	return s.updateServiceUsageWithTimeout(
		"calibre",
		func(ctx context.Context) ([]serviceFile, error) {
			client := api.NewCalibreClient(s.config.Services.Calibre.LibraryPath, s.config.Services.Calibre.DBPath, s.config.APITimeout)
			files, err := client.GetAllFiles(ctx)
			if err != nil {
				return nil, err
			}
			serviceFiles := make([]serviceFile, len(files))
			for i, f := range files {
				serviceFiles[i] = calibreServiceFile{f}
			}
			return serviceFiles, nil
		},
	)
}

// updateServiceUsageWithTimeout is a generic helper to update service usage with timeout handling
// This eliminates duplication across all service update methods
