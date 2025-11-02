package api

import (
	"fmt"
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
