package api

import (
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// PlexClient handles communication with Plex Media Server
type PlexClient struct {
	baseURL string
	token   string
	client  *http.Client
}

// PlexFile represents a file tracked by Plex
type PlexFile struct {
	Path string
	Size int64
}

// NewPlexClient creates a new Plex API client
func NewPlexClient(baseURL, token string, timeout time.Duration) *PlexClient {
	return &PlexClient{
		baseURL: baseURL,
		token:   token,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Test tests the connection to Plex
func (p *PlexClient) Test() error {
	req, err := http.NewRequest("GET", p.baseURL+"/identity", nil)
	if err != nil {
		return fmt.Errorf("failed to create request for Plex at %s: %w. Check the URL format", p.baseURL, err)
	}

	req.Header.Set("X-Plex-Token", p.token)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Plex at %s: %w. Check the URL is reachable and the token is valid", p.baseURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("plex authentication failed (401). Check your Plex token is valid")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("plex returned status %d. Check your Plex URL and token configuration", resp.StatusCode)
	}

	return nil
}

// GetAllFiles retrieves all files tracked by Plex
func (p *PlexClient) GetAllFiles() ([]PlexFile, error) {
	// First, get all library sections
	sections, err := p.getLibrarySections()
	if err != nil {
		return nil, fmt.Errorf("failed to get library sections: %w", err)
	}

	log.Printf("Found %d Plex library sections", len(sections))

	var allFiles []PlexFile

	// For each section, get all items
	for _, section := range sections {
		log.Printf("Processing Plex section: %s (type: %s, key: %s)", section.Title, section.Type, section.Key)
		files, err := p.getFilesForSection(section.Key, section.Type)
		if err != nil {
			log.Printf("ERROR: Failed to get files for section %s: %v", section.Title, err)
			return nil, fmt.Errorf("failed to get files for section %s: %w", section.Title, err)
		}
		log.Printf("Found %d files in section %s", len(files), section.Title)
		allFiles = append(allFiles, files...)
	}

	log.Printf("Total Plex files found: %d", len(allFiles))
	return allFiles, nil
}

// GetSampleFile retrieves a single sample file from Plex that matches the path prefix
// This is optimized for path mapping validation - it stops as soon as it finds one matching file
func (p *PlexClient) GetSampleFile(pathPrefix string) (string, error) {
	// Get library sections
	sections, err := p.getLibrarySections()
	if err != nil {
		return "", fmt.Errorf("failed to get library sections: %w", err)
	}

	// Try each section until we find a matching file
	for _, section := range sections {
		samplePath, err := p.getSampleFileFromSection(section.Key, section.Type, pathPrefix)
		if err != nil {
			// Log but continue to next section
			log.Printf("Failed to get sample from section %s: %v", section.Title, err)
			continue
		}
		if samplePath != "" {
			return samplePath, nil
		}
	}

	// No matching file found in any section
	return "", nil
}

type libraryResponse struct {
	Directory []struct {
		Key   string `xml:"key,attr"`
		Title string `xml:"title,attr"`
		Type  string `xml:"type,attr"`
	} `xml:"Directory"`
}

func (p *PlexClient) getLibrarySections() ([]struct {
	Key   string
	Title string
	Type  string
}, error) {
	req, err := http.NewRequest("GET", p.baseURL+"/library/sections", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Plex-Token", p.token)
	req.Header.Set("Accept", "application/xml")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex API returned status %d", resp.StatusCode)
	}

	// Use streaming XML decoder for better performance with large responses
	var libResp libraryResponse
	decoder := xml.NewDecoder(resp.Body)
	if err := decoder.Decode(&libResp); err != nil {
		return nil, fmt.Errorf("failed to parse library sections: %w", err)
	}

	var sections []struct {
		Key   string
		Title string
		Type  string
	}
	for _, dir := range libResp.Directory {
		sections = append(sections, struct {
			Key   string
			Title string
			Type  string
		}{
			Key:   dir.Key,
			Title: dir.Title,
			Type:  dir.Type,
		})
	}

	return sections, nil
}

type mediaContainerResponse struct {
	Video []struct {
		Media []struct {
			Part []struct {
				File string `xml:"file,attr"`
				Size int64  `xml:"size,attr"`
			} `xml:"Part"`
		} `xml:"Media"`
	} `xml:"Video"`
	Track []struct {
		Media []struct {
			Part []struct {
				File string `xml:"file,attr"`
				Size int64  `xml:"size,attr"`
			} `xml:"Part"`
		} `xml:"Media"`
	} `xml:"Track"`
	Photo []struct {
		Media []struct {
			Part []struct {
				File string `xml:"file,attr"`
				Size int64  `xml:"size,attr"`
			} `xml:"Part"`
		} `xml:"Media"`
	} `xml:"Photo"`
}

func (p *PlexClient) getFilesForSection(sectionKey, sectionType string) ([]PlexFile, error) {
	// For TV shows, we need to get episodes using a different endpoint
	if sectionType == "show" {
		return p.getFilesForTVSection(sectionKey)
	}

	// Build URL with all items in section
	u, err := url.Parse(p.baseURL + "/library/sections/" + sectionKey + "/all")
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Plex-Token", p.token)
	req.Header.Set("Accept", "application/xml")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex API returned status %d", resp.StatusCode)
	}

	// Use streaming XML decoder for better performance with large libraries
	var container mediaContainerResponse
	decoder := xml.NewDecoder(resp.Body)
	if err := decoder.Decode(&container); err != nil {
		return nil, fmt.Errorf("failed to parse media container: %w", err)
	}

	var files []PlexFile

	// Process Video elements (movies, TV shows)
	for _, video := range container.Video {
		for _, media := range video.Media {
			for _, part := range media.Part {
				if part.File != "" {
					files = append(files, PlexFile{
						Path: part.File,
						Size: part.Size,
					})
				}
			}
		}
	}

	// Process Track elements (music)
	for _, track := range container.Track {
		for _, media := range track.Media {
			for _, part := range media.Part {
				if part.File != "" {
					files = append(files, PlexFile{
						Path: part.File,
						Size: part.Size,
					})
				}
			}
		}
	}

	// Process Photo elements (photos)
	for _, photo := range container.Photo {
		for _, media := range photo.Media {
			for _, part := range media.Part {
				if part.File != "" {
					files = append(files, PlexFile{
						Path: part.File,
						Size: part.Size,
					})
				}
			}
		}
	}

	log.Printf("Section %s (type: %s): found %d videos, %d tracks, %d photos = %d total files",
		sectionKey, sectionType, len(container.Video), len(container.Track), len(container.Photo), len(files))

	return files, nil
}

// getFilesForTVSection gets all episode files for a TV show library section
func (p *PlexClient) getFilesForTVSection(sectionKey string) ([]PlexFile, error) {
	// For TV shows, we need to query all episodes using type=4 (episodes)
	u, err := url.Parse(p.baseURL + "/library/sections/" + sectionKey + "/all")
	if err != nil {
		return nil, err
	}

	// Add type=4 parameter to get episodes
	q := u.Query()
	q.Set("type", "4")
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Plex-Token", p.token)
	req.Header.Set("Accept", "application/xml")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex API returned status %d", resp.StatusCode)
	}

	// Use streaming XML decoder for better performance
	var container mediaContainerResponse
	decoder := xml.NewDecoder(resp.Body)
	if err := decoder.Decode(&container); err != nil {
		return nil, fmt.Errorf("failed to parse TV episodes: %w", err)
	}

	var files []PlexFile

	// TV episodes are returned as Video elements
	for _, video := range container.Video {
		for _, media := range video.Media {
			for _, part := range media.Part {
				if part.File != "" {
					files = append(files, PlexFile{
						Path: part.File,
						Size: part.Size,
					})
				}
			}
		}
	}

	log.Printf("TV Section %s: found %d episode files", sectionKey, len(files))

	return files, nil
}

// getSampleFileFromSection retrieves a single sample file from a Plex section that matches the path prefix
// Returns empty string if no matching file found
func (p *PlexClient) getSampleFileFromSection(sectionKey, sectionType, pathPrefix string) (string, error) {
	// For TV shows, use the TV-specific endpoint
	if sectionType == "show" {
		return p.getSampleFileFromTVSection(sectionKey, pathPrefix)
	}

	// Build URL to get items from section
	u, err := url.Parse(p.baseURL + "/library/sections/" + sectionKey + "/all")
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("X-Plex-Token", p.token)
	req.Header.Set("Accept", "application/xml")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("plex API returned status %d", resp.StatusCode)
	}

	// Parse response and find first matching file
	var container mediaContainerResponse
	decoder := xml.NewDecoder(resp.Body)
	if err := decoder.Decode(&container); err != nil {
		return "", fmt.Errorf("failed to parse media container: %w", err)
	}

	// Check Video elements (movies)
	for _, video := range container.Video {
		for _, media := range video.Media {
			for _, part := range media.Part {
				if part.File != "" && (pathPrefix == "" || strings.HasPrefix(part.File, pathPrefix)) {
					return part.File, nil
				}
			}
		}
	}

	// Check Track elements (music)
	for _, track := range container.Track {
		for _, media := range track.Media {
			for _, part := range media.Part {
				if part.File != "" && (pathPrefix == "" || strings.HasPrefix(part.File, pathPrefix)) {
					return part.File, nil
				}
			}
		}
	}

	// Check Photo elements
	for _, photo := range container.Photo {
		for _, media := range photo.Media {
			for _, part := range media.Part {
				if part.File != "" && (pathPrefix == "" || strings.HasPrefix(part.File, pathPrefix)) {
					return part.File, nil
				}
			}
		}
	}

	return "", nil
}

// getSampleFileFromTVSection retrieves a single sample TV episode file that matches the path prefix
func (p *PlexClient) getSampleFileFromTVSection(sectionKey, pathPrefix string) (string, error) {
	// Build URL to get all episodes in section
	u, err := url.Parse(p.baseURL + "/library/sections/" + sectionKey + "/all")
	if err != nil {
		return "", err
	}

	// Add query parameters to get episode-level data
	q := u.Query()
	q.Set("type", "4") // 4 = episodes in Plex
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("X-Plex-Token", p.token)
	req.Header.Set("Accept", "application/xml")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("plex API returned status %d", resp.StatusCode)
	}

	// Parse response and find first matching file
	var container mediaContainerResponse
	decoder := xml.NewDecoder(resp.Body)
	if err := decoder.Decode(&container); err != nil {
		return "", fmt.Errorf("failed to parse TV episodes: %w", err)
	}

	// TV episodes are returned as Video elements
	for _, video := range container.Video {
		for _, media := range video.Media {
			for _, part := range media.Part {
				if part.File != "" && (pathPrefix == "" || strings.HasPrefix(part.File, pathPrefix)) {
					return part.File, nil
				}
			}
		}
	}

	return "", nil
}
