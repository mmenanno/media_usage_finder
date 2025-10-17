package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// RadarrClient handles communication with Radarr
type RadarrClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// RadarrFile represents a file tracked by Radarr
type RadarrFile struct {
	Path       string
	Size       int64
	MovieTitle string
	MovieYear  int
	MovieID    int64
}

// NewRadarrClient creates a new Radarr API client
func NewRadarrClient(baseURL, apiKey string, timeout time.Duration) *RadarrClient {
	return &RadarrClient{
		baseURL: baseURL,
		apiKey:  apiKey,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Test tests the connection to Radarr
func (r *RadarrClient) Test() error {
	req, err := http.NewRequest("GET", r.baseURL+"/api/v3/system/status", nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-Api-Key", r.apiKey)

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Radarr: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Radarr returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetAllFiles retrieves all movie files tracked by Radarr
func (r *RadarrClient) GetAllFiles() ([]RadarrFile, error) {
	req, err := http.NewRequest("GET", r.baseURL+"/api/v3/moviefile", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Api-Key", r.apiKey)

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get movie files: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Radarr API returned status %d: %s", resp.StatusCode, string(body))
	}

	var movieFiles []struct {
		ID      int64  `json:"id"`
		MovieID int64  `json:"movieId"`
		Path    string `json:"path"`
		Size    int64  `json:"size"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&movieFiles); err != nil {
		return nil, fmt.Errorf("failed to decode movie files: %w", err)
	}

	// Get movie information for each file
	movieMap, err := r.getAllMovies()
	if err != nil {
		return nil, fmt.Errorf("failed to get movies: %w", err)
	}

	var files []RadarrFile
	for _, mf := range movieFiles {
		movieTitle := ""
		movieYear := 0
		if movie, ok := movieMap[mf.MovieID]; ok {
			movieTitle = movie.Title
			movieYear = movie.Year
		}

		files = append(files, RadarrFile{
			Path:       mf.Path,
			Size:       mf.Size,
			MovieTitle: movieTitle,
			MovieYear:  movieYear,
			MovieID:    mf.MovieID,
		})
	}

	return files, nil
}

type movieInfo struct {
	Title string
	Year  int
}

func (r *RadarrClient) getAllMovies() (map[int64]movieInfo, error) {
	req, err := http.NewRequest("GET", r.baseURL+"/api/v3/movie", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Api-Key", r.apiKey)

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Radarr API returned status %d", resp.StatusCode)
	}

	var movies []struct {
		ID    int64  `json:"id"`
		Title string `json:"title"`
		Year  int    `json:"year"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&movies); err != nil {
		return nil, err
	}

	movieMap := make(map[int64]movieInfo)
	for _, m := range movies {
		movieMap[m.ID] = movieInfo{
			Title: m.Title,
			Year:  m.Year,
		}
	}

	return movieMap, nil
}
