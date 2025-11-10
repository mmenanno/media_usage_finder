package api

import (
	"fmt"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/config"
)

// ClientFactory creates API clients for different services
type ClientFactory struct {
	config *config.Config
}

// NewClientFactory creates a new client factory
func NewClientFactory(cfg *config.Config) *ClientFactory {
	return &ClientFactory{config: cfg}
}

// CreateClient creates a service client by name
func (f *ClientFactory) CreateClient(serviceName string, timeout time.Duration) (ServiceClient, error) {
	switch serviceName {
	case "plex":
		return f.CreatePlexClient(timeout), nil
	case "sonarr":
		return f.CreateSonarrClient(timeout), nil
	case "radarr":
		return f.CreateRadarrClient(timeout), nil
	case "qbittorrent":
		return f.CreateQBittorrentClient(timeout), nil
	case "stash":
		return f.CreateStashClient(timeout), nil
	case "calibre":
		return f.CreateCalibreClient(timeout), nil
	default:
		return nil, fmt.Errorf("unknown service: %s", serviceName)
	}
}

// CreatePlexClient creates a Plex API client
func (f *ClientFactory) CreatePlexClient(timeout time.Duration) *PlexClient {
	return NewPlexClient(f.config.Services.Plex.URL, f.config.Services.Plex.Token, timeout)
}

// CreateSonarrClient creates a Sonarr API client
func (f *ClientFactory) CreateSonarrClient(timeout time.Duration) *SonarrClient {
	return NewSonarrClient(f.config.Services.Sonarr.URL, f.config.Services.Sonarr.APIKey, timeout)
}

// CreateRadarrClient creates a Radarr API client
func (f *ClientFactory) CreateRadarrClient(timeout time.Duration) *RadarrClient {
	return NewRadarrClient(f.config.Services.Radarr.URL, f.config.Services.Radarr.APIKey, timeout)
}

// CreateQBittorrentClient creates a qBittorrent API client
func (f *ClientFactory) CreateQBittorrentClient(timeout time.Duration) *QBittorrentClient {
	qbConfig := f.config.Services.QBittorrent
	return NewQBittorrentClient(qbConfig.URL, qbConfig.Username, qbConfig.Password, qbConfig.QuiProxyURL, timeout)
}

// CreateStashClient creates a Stash API client
func (f *ClientFactory) CreateStashClient(timeout time.Duration) *StashClient {
	return NewStashClient(f.config.Services.Stash.URL, f.config.Services.Stash.APIKey, timeout)
}

// CreateCalibreClient creates a Calibre API client
func (f *ClientFactory) CreateCalibreClient(timeout time.Duration) *CalibreClient {
	return NewCalibreClient(f.config.Services.Calibre.LibraryPath, f.config.Services.Calibre.DBPath, timeout)
}

// IsServiceConfigured checks if a service is configured with valid credentials
func (f *ClientFactory) IsServiceConfigured(serviceName string) bool {
	switch serviceName {
	case "plex":
		return f.config.Services.Plex.URL != "" && f.config.Services.Plex.Token != ""
	case "sonarr":
		return f.config.Services.Sonarr.URL != "" && f.config.Services.Sonarr.APIKey != ""
	case "radarr":
		return f.config.Services.Radarr.URL != "" && f.config.Services.Radarr.APIKey != ""
	case "qbittorrent":
		qbConfig := f.config.Services.QBittorrent
		// Valid if either direct URL with credentials OR qui proxy URL
		hasDirectAccess := qbConfig.URL != "" && qbConfig.Username != "" && qbConfig.Password != ""
		hasProxyAccess := qbConfig.QuiProxyURL != ""
		return hasDirectAccess || hasProxyAccess
	case "stash":
		return f.config.Services.Stash.URL != "" && f.config.Services.Stash.APIKey != ""
	case "calibre":
		return f.config.Services.Calibre.LibraryPath != "" && f.config.Services.Calibre.DBPath != ""
	default:
		return false
	}
}
