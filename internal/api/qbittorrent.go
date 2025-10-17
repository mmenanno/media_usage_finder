package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

// QBittorrentClient handles communication with qBittorrent
type QBittorrentClient struct {
	baseURL     string
	username    string
	password    string
	quiProxyURL string
	client      *http.Client
}

// QBittorrentFile represents a file in a torrent
type QBittorrentFile struct {
	Path         string
	Size         int64
	TorrentHash  string
	TorrentName  string
	TorrentFiles []TorrentFileInfo
}

// TorrentFileInfo represents a file within a torrent
type TorrentFileInfo struct {
	Name string
	Size int64
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

// login authenticates with qBittorrent
func (q *QBittorrentClient) login() error {
	// If using qui proxy, no login needed
	if q.quiProxyURL != "" {
		return nil
	}

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
		return err
	}

	req, err := http.NewRequest("GET", q.getEffectiveURL()+"/api/v2/app/version", nil)
	if err != nil {
		return err
	}

	resp, err := q.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to qBittorrent: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("qBittorrent returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetAllFiles retrieves all files from all torrents
func (q *QBittorrentClient) GetAllFiles() ([]QBittorrentFile, error) {
	if err := q.login(); err != nil {
		return nil, err
	}

	// Get list of all torrents
	torrents, err := q.getTorrents()
	if err != nil {
		return nil, fmt.Errorf("failed to get torrents: %w", err)
	}

	var allFiles []QBittorrentFile

	// For each torrent, get its files
	for _, torrent := range torrents {
		files, err := q.getTorrentFiles(torrent.Hash)
		if err != nil {
			// Log error but continue with other torrents
			continue
		}

		// Get torrent properties to find save path
		props, err := q.getTorrentProperties(torrent.Hash)
		if err != nil {
			continue
		}

		var torrentFiles []TorrentFileInfo
		for _, f := range files {
			torrentFiles = append(torrentFiles, TorrentFileInfo{
				Name: f.Name,
				Size: f.Size,
			})

			// Build full file path
			fullPath := props.SavePath + "/" + f.Name

			allFiles = append(allFiles, QBittorrentFile{
				Path:         fullPath,
				Size:         f.Size,
				TorrentHash:  torrent.Hash,
				TorrentName:  torrent.Name,
				TorrentFiles: torrentFiles,
			})
		}
	}

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
