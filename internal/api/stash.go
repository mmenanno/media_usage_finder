package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// StashClient handles communication with Stash
type StashClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// StashFile represents a file tracked by Stash
type StashFile struct {
	Path     string
	Size     int64
	SceneID  string
	Title    string
	Studio   string
	Tags     []string
	PlayCount int
}

// NewStashClient creates a new Stash API client
func NewStashClient(baseURL, apiKey string, timeout time.Duration) *StashClient {
	return &StashClient{
		baseURL: baseURL,
		apiKey:  apiKey,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Test tests the connection to Stash
func (s *StashClient) Test() error {
	// Use a simple GraphQL query to test connectivity
	query := `query { version { version } }`

	reqBody := graphQLRequest{
		Query: query,
	}

	var resp struct {
		Data struct {
			Version struct {
				Version string `json:"version"`
			} `json:"version"`
		} `json:"data"`
		Errors []graphQLError `json:"errors"`
	}

	if err := s.doGraphQLRequest(reqBody, &resp); err != nil {
		return fmt.Errorf("failed to connect to Stash at %s: %w. Check the URL is reachable and the API key is valid", s.baseURL, err)
	}

	if len(resp.Errors) > 0 {
		return fmt.Errorf("stash query failed: %s", resp.Errors[0].Message)
	}

	if resp.Data.Version.Version == "" {
		return fmt.Errorf("stash returned empty version, check API key configuration")
	}

	log.Printf("Connected to Stash version %s", resp.Data.Version.Version)
	return nil
}

// GetAllFiles retrieves all files tracked by Stash
func (s *StashClient) GetAllFiles() ([]StashFile, error) {
	var allFiles []StashFile
	page := 1
	perPage := 100

	for {
		log.Printf("Fetching Stash scenes page %d (per_page: %d)", page, perPage)

		files, totalCount, err := s.getFilesPage(page, perPage)
		if err != nil {
			return nil, fmt.Errorf("failed to get scenes page %d: %w", page, err)
		}

		allFiles = append(allFiles, files...)

		log.Printf("Retrieved %d files from page %d (total so far: %d)", len(files), page, len(allFiles))

		// Check if we've retrieved all scenes
		if len(allFiles) >= totalCount || len(files) == 0 {
			break
		}

		page++
	}

	log.Printf("Total Stash files found: %d", len(allFiles))
	return allFiles, nil
}

// GetSampleFile retrieves a single sample file from Stash that matches the path prefix
// This is optimized for path mapping validation - it stops as soon as it finds one matching file
func (s *StashClient) GetSampleFile(pathPrefix string) (string, error) {
	// Just fetch first page with small page size for efficiency
	page := 1
	perPage := 10

	for page <= 10 { // Limit to checking first 10 pages (100 files max) to avoid long waits
		files, totalCount, err := s.getFilesPage(page, perPage)
		if err != nil {
			return "", fmt.Errorf("failed to get scenes page %d: %w", page, err)
		}

		// Check if any file matches
		for _, file := range files {
			if file.Path != "" && (pathPrefix == "" || strings.HasPrefix(file.Path, pathPrefix)) {
				return file.Path, nil
			}
		}

		// If we've checked all available files, stop
		if len(files) == 0 || len(files) < perPage || page*perPage >= totalCount {
			break
		}

		page++
	}

	// No matching file found
	return "", nil
}

type graphQLRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

type graphQLError struct {
	Message string `json:"message"`
	Path    []interface{} `json:"path,omitempty"`
}

type graphQLResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []graphQLError  `json:"errors"`
}

func (s *StashClient) getFilesPage(page, perPage int) ([]StashFile, int, error) {
	query := `
		query FindScenes($filter: FindFilterType) {
			findScenes(filter: $filter) {
				count
				scenes {
					id
					title
					play_count
					studio {
						name
					}
					tags {
						name
					}
					files {
						path
						size
					}
				}
			}
		}
	`

	variables := map[string]interface{}{
		"filter": map[string]interface{}{
			"per_page": perPage,
			"page":     page,
		},
	}

	reqBody := graphQLRequest{
		Query:     query,
		Variables: variables,
	}

	var resp struct {
		Data struct {
			FindScenes struct {
				Count  int `json:"count"`
				Scenes []struct {
					ID        string `json:"id"`
					Title     string `json:"title"`
					PlayCount int    `json:"play_count"`
					Studio    *struct {
						Name string `json:"name"`
					} `json:"studio"`
					Tags []struct {
						Name string `json:"name"`
					} `json:"tags"`
					Files []struct {
						Path string `json:"path"`
						Size int64  `json:"size"`
					} `json:"files"`
				} `json:"scenes"`
			} `json:"findScenes"`
		} `json:"data"`
		Errors []graphQLError `json:"errors"`
	}

	if err := s.doGraphQLRequest(reqBody, &resp); err != nil {
		return nil, 0, err
	}

	if len(resp.Errors) > 0 {
		return nil, 0, fmt.Errorf("stash query failed: %s", resp.Errors[0].Message)
	}

	var files []StashFile

	// Process each scene
	for _, scene := range resp.Data.FindScenes.Scenes {
		// Collect tag names
		var tagNames []string
		for _, tag := range scene.Tags {
			tagNames = append(tagNames, tag.Name)
		}

		// Get studio name (if present)
		studioName := ""
		if scene.Studio != nil {
			studioName = scene.Studio.Name
		}

		// Each scene can have multiple files
		for _, file := range scene.Files {
			if file.Path != "" {
				files = append(files, StashFile{
					Path:      file.Path,
					Size:      file.Size,
					SceneID:   scene.ID,
					Title:     scene.Title,
					Studio:    studioName,
					Tags:      tagNames,
					PlayCount: scene.PlayCount,
				})
			}
		}
	}

	return files, resp.Data.FindScenes.Count, nil
}

func (s *StashClient) doGraphQLRequest(reqBody graphQLRequest, respData interface{}) error {
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", s.baseURL+"/graphql", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("ApiKey", s.apiKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("stash authentication failed (401). Check your API key is valid")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stash returned status %d. Check your Stash URL and API key configuration", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(respData); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}
