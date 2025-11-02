package api

import (
	"fmt"
	"time"
)

// SonarrClient handles communication with Sonarr
type SonarrClient struct {
	*ArrClient
}

// SonarrFile represents a file tracked by Sonarr
type SonarrFile struct {
	Path         string
	Size         int64
	SeriesTitle  string
	SeasonNumber int
	EpisodeID    int64
}

// NewSonarrClient creates a new Sonarr API client
func NewSonarrClient(baseURL, apiKey string, timeout time.Duration) *SonarrClient {
	return &SonarrClient{
		ArrClient: NewArrClient(baseURL, apiKey, "Sonarr", "episode", timeout),
	}
}

// GetAllFiles retrieves all episode files tracked by Sonarr
func (s *SonarrClient) GetAllFiles() ([]SonarrFile, error) {
	// First, get all series
	seriesMap, err := s.getAllSeries()
	if err != nil {
		return nil, fmt.Errorf("failed to get series: %w", err)
	}

	// Then, get episode files for each series
	var files []SonarrFile
	for seriesID, seriesTitle := range seriesMap {
		var episodeFiles []struct {
			ID           int64  `json:"id"`
			SeriesID     int64  `json:"seriesId"`
			SeasonNumber int    `json:"seasonNumber"`
			Path         string `json:"path"`
			Size         int64  `json:"size"`
		}

		// Query episode files for this specific series
		endpoint := fmt.Sprintf("/api/v3/episodefile?seriesId=%d", seriesID)
		if err := s.doRequest(endpoint, &episodeFiles); err != nil {
			return nil, fmt.Errorf("failed to get episode files for series %d: %w", seriesID, err)
		}

		// Add all episode files for this series
		for _, ef := range episodeFiles {
			files = append(files, SonarrFile{
				Path:         ef.Path,
				Size:         ef.Size,
				SeriesTitle:  seriesTitle,
				SeasonNumber: ef.SeasonNumber,
				EpisodeID:    ef.ID,
			})
		}
	}

	return files, nil
}

func (s *SonarrClient) getAllSeries() (map[int64]string, error) {
	var series []struct {
		ID    int64  `json:"id"`
		Title string `json:"title"`
	}

	if err := s.doRequest("/api/v3/series", &series); err != nil {
		return nil, err
	}

	seriesMap := make(map[int64]string)
	for _, s := range series {
		seriesMap[s.ID] = s.Title
	}

	return seriesMap, nil
}
