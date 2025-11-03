package api

import (
	"fmt"
	"strings"
	"time"
)

// RadarrClient handles communication with Radarr
type RadarrClient struct {
	*ArrClient
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
		ArrClient: NewArrClient(baseURL, apiKey, "Radarr", "movie", timeout),
	}
}

// GetAllFiles retrieves all movie files tracked by Radarr
func (r *RadarrClient) GetAllFiles() ([]RadarrFile, error) {
	// First, get all movies
	movieMap, err := r.getAllMovies()
	if err != nil {
		return nil, fmt.Errorf("failed to get movies: %w", err)
	}

	// Then, get movie files for each movie
	var files []RadarrFile
	for movieID, movieInfo := range movieMap {
		var movieFiles []struct {
			ID      int64  `json:"id"`
			MovieID int64  `json:"movieId"`
			Path    string `json:"path"`
			Size    int64  `json:"size"`
		}

		// Query movie files for this specific movie
		endpoint := fmt.Sprintf("/api/v3/moviefile?movieId=%d", movieID)
		if err := r.doRequest(endpoint, &movieFiles); err != nil {
			return nil, fmt.Errorf("failed to get movie files for movie %d: %w", movieID, err)
		}

		// Add all movie files for this movie
		for _, mf := range movieFiles {
			files = append(files, RadarrFile{
				Path:       mf.Path,
				Size:       mf.Size,
				MovieTitle: movieInfo.Title,
				MovieYear:  movieInfo.Year,
				MovieID:    mf.MovieID,
			})
		}
	}

	return files, nil
}

// GetSampleFile retrieves a single sample file from Radarr that matches the path prefix
// This is optimized for path mapping validation - it stops as soon as it finds one matching file
func (r *RadarrClient) GetSampleFile(pathPrefix string) (string, error) {
	// Get all movies
	movieMap, err := r.getAllMovies()
	if err != nil {
		return "", fmt.Errorf("failed to get movies: %w", err)
	}

	// Try each movie until we find a matching file
	for movieID := range movieMap {
		var movieFiles []struct {
			Path string `json:"path"`
		}

		// Query movie files for this specific movie
		endpoint := fmt.Sprintf("/api/v3/moviefile?movieId=%d", movieID)
		if err := r.doRequest(endpoint, &movieFiles); err != nil {
			// Log and continue to next movie
			continue
		}

		// Check if any file matches the path prefix
		for _, mf := range movieFiles {
			if mf.Path != "" && (pathPrefix == "" || strings.HasPrefix(mf.Path, pathPrefix)) {
				return mf.Path, nil
			}
		}
	}

	// No matching file found
	return "", nil
}

type movieInfo struct {
	Title string
	Year  int
}

func (r *RadarrClient) getAllMovies() (map[int64]movieInfo, error) {
	var movies []struct {
		ID    int64  `json:"id"`
		Title string `json:"title"`
		Year  int    `json:"year"`
	}

	if err := r.doRequest("/api/v3/movie", &movies); err != nil {
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
