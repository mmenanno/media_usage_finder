package api

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// SonarrClient handles communication with Sonarr
type SonarrClient struct {
	*ArrClient
}

// SonarrFile represents a file tracked by Sonarr
type SonarrFile struct {
	Path          string
	Size          int64
	SeriesTitle   string
	SeasonNumber  int
	EpisodeNumber int
	EpisodeID     int64
}

// NewSonarrClient creates a new Sonarr API client
func NewSonarrClient(baseURL, apiKey string, timeout time.Duration) *SonarrClient {
	return &SonarrClient{
		ArrClient: NewArrClient(baseURL, apiKey, "Sonarr", "episode", timeout),
	}
}

// GetAllFiles retrieves all episode files tracked by Sonarr
func (s *SonarrClient) GetAllFiles(ctx context.Context) ([]SonarrFile, error) {
	// First, get all series
	seriesMap, err := s.getAllSeries(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get series: %w", err)
	}

	// Then, get episode files for each series
	var files []SonarrFile
	for seriesID, seriesTitle := range seriesMap {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Get all episodes for this series to map episode file IDs to episode numbers
		var episodes []struct {
			EpisodeFileID int64 `json:"episodeFileId"`
			EpisodeNumber int   `json:"episodeNumber"`
		}

		episodesEndpoint := fmt.Sprintf("/api/v3/episode?seriesId=%d", seriesID)
		if err := s.doRequest(ctx, episodesEndpoint, &episodes); err != nil {
			return nil, fmt.Errorf("failed to get episodes for series %d: %w", seriesID, err)
		}

		// Create map of episode file ID to episode number
		episodeFileToNumber := make(map[int64]int)
		for _, ep := range episodes {
			if ep.EpisodeFileID > 0 {
				episodeFileToNumber[ep.EpisodeFileID] = ep.EpisodeNumber
			}
		}

		var episodeFiles []struct {
			ID           int64  `json:"id"`
			SeriesID     int64  `json:"seriesId"`
			SeasonNumber int    `json:"seasonNumber"`
			Path         string `json:"path"`
			Size         int64  `json:"size"`
		}

		// Query episode files for this specific series
		endpoint := fmt.Sprintf("/api/v3/episodefile?seriesId=%d", seriesID)
		if err := s.doRequest(ctx, endpoint, &episodeFiles); err != nil {
			return nil, fmt.Errorf("failed to get episode files for series %d: %w", seriesID, err)
		}

		// Add all episode files for this series
		for _, ef := range episodeFiles {
			files = append(files, SonarrFile{
				Path:          ef.Path,
				Size:          ef.Size,
				SeriesTitle:   seriesTitle,
				SeasonNumber:  ef.SeasonNumber,
				EpisodeNumber: episodeFileToNumber[ef.ID], // Will be 0 if not found
				EpisodeID:     ef.ID,
			})
		}
	}

	return files, nil
}

// GetSampleFile retrieves a single sample file from Sonarr that matches the path prefix
// This is optimized for path mapping validation - it stops as soon as it finds one matching file
func (s *SonarrClient) GetSampleFile(pathPrefix string) (string, error) {
	// Use background context (not cancellable)
	ctx := context.Background()

	// Get all series
	seriesMap, err := s.getAllSeries(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get series: %w", err)
	}

	// Try each series until we find a matching file
	for seriesID := range seriesMap {
		var episodeFiles []struct {
			Path string `json:"path"`
		}

		// Query episode files for this specific series
		endpoint := fmt.Sprintf("/api/v3/episodefile?seriesId=%d", seriesID)
		if err := s.doRequest(ctx, endpoint, &episodeFiles); err != nil {
			// Log and continue to next series
			continue
		}

		// Check if any file matches the path prefix
		for _, ef := range episodeFiles {
			if ef.Path != "" && (pathPrefix == "" || strings.HasPrefix(ef.Path, pathPrefix)) {
				return ef.Path, nil
			}
		}
	}

	// No matching file found
	return "", nil
}

func (s *SonarrClient) getAllSeries(ctx context.Context) (map[int64]string, error) {
	var series []struct {
		ID    int64  `json:"id"`
		Title string `json:"title"`
	}

	if err := s.doRequest(ctx, "/api/v3/series", &series); err != nil {
		return nil, err
	}

	seriesMap := make(map[int64]string)
	for _, s := range series {
		seriesMap[s.ID] = s.Title
	}

	return seriesMap, nil
}
