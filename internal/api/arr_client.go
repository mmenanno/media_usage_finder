package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ArrClient is a generic client for Sonarr/Radarr (both use same API structure)
type ArrClient struct {
	baseURL  string
	apiKey   string
	client   *http.Client
	appName  string
	fileType string // "episode" or "movie"
}

// NewArrClient creates a new generic *arr API client
func NewArrClient(baseURL, apiKey, appName, fileType string, timeout time.Duration) *ArrClient {
	return &ArrClient{
		baseURL:  baseURL,
		apiKey:   apiKey,
		client:   &http.Client{Timeout: timeout},
		appName:  appName,
		fileType: fileType,
	}
}

// Test tests the connection
func (a *ArrClient) Test() error {
	req, err := http.NewRequest("GET", a.baseURL+"/api/v3/system/status", nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-Api-Key", a.apiKey)

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", a.appName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s returned status %d: %s", a.appName, resp.StatusCode, string(body))
	}

	return nil
}

// doRequest performs an API request
func (a *ArrClient) doRequest(endpoint string, result interface{}) error {
	req, err := http.NewRequest("GET", a.baseURL+endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-Api-Key", a.apiKey)

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get data from %s: %w", a.appName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s API returned status %d: %s", a.appName, resp.StatusCode, string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("failed to decode %s response: %w", a.appName, err)
	}

	return nil
}
