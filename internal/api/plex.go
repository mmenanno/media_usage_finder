package api

import (
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"net/url"
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
