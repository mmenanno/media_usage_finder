package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/constants"
)

// QBittorrentClient handles communication with qBittorrent
type QBittorrentClient struct {
	baseURL     string
	username    string
	password    string
	quiProxyURL string
	client      *http.Client
	loginMu     sync.Mutex // Protects concurrent login attempts
}

// QBittorrentFile represents a file in a torrent
type QBittorrentFile struct {
	Path        string
	Size        int64
	TorrentHash string
	TorrentName string
}

// NewQBittorrentClient creates a new qBittorrent API client
func NewQBittorrentClient(baseURL, username, password, quiProxyURL string, timeout time.Duration) *QBittorrentClient {
	jar, _ := cookiejar.New(nil)

	return &QBittorrentClient{
		baseURL:     baseURL,
		username:    username,
		password:    password,
		quiProxyURL: quiProxyURL,
		client: &http.Client{
			Timeout: timeout,
			Jar:     jar,
		},
	}
}

// getEffectiveURL returns the URL to use (qui proxy if configured, otherwise direct)
func (q *QBittorrentClient) getEffectiveURL() string {
	if q.quiProxyURL != "" {
		return q.quiProxyURL
	}
	return q.baseURL
}

// login authenticates with qBittorrent (thread-safe)
func (q *QBittorrentClient) login() error {
	// If using qui proxy, no login needed
	if q.quiProxyURL != "" {
		return nil
	}

	// Protect concurrent login attempts with mutex
	q.loginMu.Lock()
	defer q.loginMu.Unlock()

	data := url.Values{}
	data.Set("username", q.username)
	data.Set("password", q.password)

	req, err := http.NewRequest("POST", q.baseURL+"/api/v2/auth/login", strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := q.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to login to qBittorrent: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("qBittorrent login failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) == "Fails." {
		return fmt.Errorf("qBittorrent authentication failed")
	}

	return nil
}

// Test tests the connection to qBittorrent
func (q *QBittorrentClient) Test() error {
	if err := q.login(); err != nil {
		return fmt.Errorf("qBittorrent login failed: %w. Check username and password", err)
	}

	req, err := http.NewRequest("GET", q.getEffectiveURL()+"/api/v2/app/version", nil)
	if err != nil {
		return fmt.Errorf("failed to create qBittorrent request: %w. Check URL format", err)
	}

	resp, err := q.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to qBittorrent at %s: %w. Check the URL is reachable", q.getEffectiveURL(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("qBittorrent returned status %d: %s. Check your configuration", resp.StatusCode, string(body))
	}

	return nil
}

// GetAllFiles retrieves all files from all torrents using concurrent workers
//
// Concurrency Strategy:
// - Uses a semaphore pattern to limit concurrent torrent processing to MaxConcurrentTorrentWorkers
// - For each torrent, fetches file list and properties concurrently in separate goroutines
// - Collects all results into a single slice protected by a mutex
// - If any API call fails for a torrent, that torrent is skipped (logged but doesn't fail entire operation)
// - This approach significantly improves performance for users with many torrents
func (q *QBittorrentClient) GetAllFiles() ([]QBittorrentFile, error) {
	if err := q.login(); err != nil {
		return nil, err
	}

	// Get list of all torrents
	torrents, err := q.getTorrents()
	if err != nil {
		return nil, fmt.Errorf("failed to get torrents: %w", err)
	}

	if len(torrents) == 0 {
		return []QBittorrentFile{}, nil
	}

	// Use concurrent workers to process torrents
	sem := make(chan struct{}, constants.MaxConcurrentTorrentWorkers) // Semaphore for concurrency control

	var mu sync.Mutex
	var allFiles []QBittorrentFile
	var wg sync.WaitGroup

	for _, torrent := range torrents {
		wg.Add(1)
		go func(t torrentInfo) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			// Get torrent files and properties concurrently with proper error handling
			type result struct {
				files []fileInfo
				props *torrentProperties
				err   error
			}
			resultCh := make(chan result, 1)

			go func() {
				var r result
				var wg sync.WaitGroup
				var filesErr, propsErr error

				wg.Add(2)
				go func() {
					defer wg.Done()
					r.files, filesErr = q.getTorrentFiles(t.Hash)
				}()

				go func() {
					defer wg.Done()
					r.props, propsErr = q.getTorrentProperties(t.Hash)
				}()

				wg.Wait()

				// Check for errors
				if filesErr != nil {
					r.err = filesErr
				} else if propsErr != nil {
					r.err = propsErr
				}

				resultCh <- r
			}()

			// Wait for result
			res := <-resultCh
			if res.err != nil || res.files == nil || res.props == nil {
				// Skip this torrent on error
				return
			}

			// Process files
			var torrentQBFiles []QBittorrentFile
			for _, f := range res.files {
				// Build full file path using filepath.Join for safety
				fullPath := filepath.Join(res.props.SavePath, f.Name)

				torrentQBFiles = append(torrentQBFiles, QBittorrentFile{
					Path:        fullPath,
					Size:        f.Size,
					TorrentHash: t.Hash,
					TorrentName: t.Name,
				})
			}

			// Append to results with mutex
			mu.Lock()
			allFiles = append(allFiles, torrentQBFiles...)
			mu.Unlock()
		}(torrent)
	}

	wg.Wait()
	return allFiles, nil
}

type torrentInfo struct {
	Hash string `json:"hash"`
	Name string `json:"name"`
}

func (q *QBittorrentClient) getTorrents() ([]torrentInfo, error) {
	req, err := http.NewRequest("GET", q.getEffectiveURL()+"/api/v2/torrents/info", nil)
	if err != nil {
		return nil, err
	}

	resp, err := q.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("qBittorrent API returned status %d: %s", resp.StatusCode, string(body))
	}

	var torrents []torrentInfo
	if err := json.NewDecoder(resp.Body).Decode(&torrents); err != nil {
		return nil, err
	}

	return torrents, nil
}

type fileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func (q *QBittorrentClient) getTorrentFiles(hash string) ([]fileInfo, error) {
	u, err := url.Parse(q.getEffectiveURL() + "/api/v2/torrents/files")
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Add("hash", hash)
	u.RawQuery = params.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := q.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("qBittorrent API returned status %d", resp.StatusCode)
	}

	var files []fileInfo
	if err := json.NewDecoder(resp.Body).Decode(&files); err != nil {
		return nil, err
	}

	return files, nil
}

type torrentProperties struct {
	SavePath string `json:"save_path"`
}

func (q *QBittorrentClient) getTorrentProperties(hash string) (*torrentProperties, error) {
	u, err := url.Parse(q.getEffectiveURL() + "/api/v2/torrents/properties")
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Add("hash", hash)
	u.RawQuery = params.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := q.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("qBittorrent API returned status %d", resp.StatusCode)
	}

	var props torrentProperties
	if err := json.NewDecoder(resp.Body).Decode(&props); err != nil {
		return nil, err
	}

	return &props, nil
}
