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
	var episodeFiles []struct {
		ID           int64  `json:"id"`
		SeriesID     int64  `json:"seriesId"`
		SeasonNumber int    `json:"seasonNumber"`
		Path         string `json:"path"`
		Size         int64  `json:"size"`
	}

	if err := s.doRequest("/api/v3/episodefile", &episodeFiles); err != nil {
		return nil, err
	}

	// Get series information for each file
	seriesMap, err := s.getAllSeries()
	if err != nil {
		return nil, fmt.Errorf("failed to get series: %w", err)
	}

	var files []SonarrFile
	for _, ef := range episodeFiles {
		seriesTitle := ""
		if series, ok := seriesMap[ef.SeriesID]; ok {
			seriesTitle = series
		}

		files = append(files, SonarrFile{
			Path:         ef.Path,
			Size:         ef.Size,
			SeriesTitle:  seriesTitle,
			SeasonNumber: ef.SeasonNumber,
			EpisodeID:    ef.ID,
		})
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
