package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SonarrClient handles communication with Sonarr
type SonarrClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
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
		baseURL: baseURL,
		apiKey:  apiKey,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Test tests the connection to Sonarr
func (s *SonarrClient) Test() error {
	req, err := http.NewRequest("GET", s.baseURL+"/api/v3/system/status", nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-Api-Key", s.apiKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Sonarr: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Sonarr returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetAllFiles retrieves all episode files tracked by Sonarr
func (s *SonarrClient) GetAllFiles() ([]SonarrFile, error) {
	req, err := http.NewRequest("GET", s.baseURL+"/api/v3/episodefile", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Api-Key", s.apiKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get episode files: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Sonarr API returned status %d: %s", resp.StatusCode, string(body))
	}

	var episodeFiles []struct {
		ID           int64  `json:"id"`
		SeriesID     int64  `json:"seriesId"`
		SeasonNumber int    `json:"seasonNumber"`
		Path         string `json:"path"`
		Size         int64  `json:"size"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&episodeFiles); err != nil {
		return nil, fmt.Errorf("failed to decode episode files: %w", err)
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
	req, err := http.NewRequest("GET", s.baseURL+"/api/v3/series", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Api-Key", s.apiKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Sonarr API returned status %d", resp.StatusCode)
	}

	var series []struct {
		ID    int64  `json:"id"`
		Title string `json:"title"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&series); err != nil {
		return nil, err
	}

	seriesMap := make(map[int64]string)
	for _, s := range series {
		seriesMap[s.ID] = s.Title
	}

	return seriesMap, nil
}
