package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration
type Config struct {
	DatabasePath string        `yaml:"database_path"`
	ScanWorkers  int           `yaml:"scan_workers"`
	APITimeout   time.Duration `yaml:"api_timeout"`
	ServerPort   int           `yaml:"server_port"`

	LocalPathMappings   []PathMapping            `yaml:"local_path_mappings"`
	ServicePathMappings map[string][]PathMapping `yaml:"service_path_mappings"`
	ScanPaths           []string                 `yaml:"scan_paths"`
	Services            Services                 `yaml:"services"`
}

// PathMapping represents a container-to-host path mapping
type PathMapping struct {
	Container string `yaml:"container"`
	Host      string `yaml:"host"`
}

// Services contains configuration for all external services
type Services struct {
	Plex        PlexConfig        `yaml:"plex"`
	Sonarr      SonarrConfig      `yaml:"sonarr"`
	Radarr      RadarrConfig      `yaml:"radarr"`
	QBittorrent QBittorrentConfig `yaml:"qbittorrent"`
}

// PlexConfig contains Plex server configuration
type PlexConfig struct {
	URL   string `yaml:"url"`
	Token string `yaml:"token"`
}

// SonarrConfig contains Sonarr configuration
type SonarrConfig struct {
	URL    string `yaml:"url"`
	APIKey string `yaml:"api_key"`
}

// RadarrConfig contains Radarr configuration
type RadarrConfig struct {
	URL    string `yaml:"url"`
	APIKey string `yaml:"api_key"`
}

// QBittorrentConfig contains qBittorrent configuration
type QBittorrentConfig struct {
	URL         string `yaml:"url"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	QuiProxyURL string `yaml:"qui_proxy_url"`
}

// Default returns a default configuration
func Default() *Config {
	return &Config{
		DatabasePath: "/data/media-finder.db",
		ScanWorkers:  10,
		APITimeout:   30 * time.Second,
		ServerPort:   8080,
		LocalPathMappings: []PathMapping{
			{Container: "/media", Host: "/mnt/user/data/media"},
			{Container: "/downloads", Host: "/mnt/user/data/downloads/torrents"},
		},
		ServicePathMappings: map[string][]PathMapping{
			"plex": {
				{Container: "/media", Host: "/mnt/user/data/media"},
			},
			"sonarr": {
				{Container: "/tv", Host: "/mnt/user/data/media/tv"},
				{Container: "/downloads", Host: "/mnt/user/data/downloads/torrents"},
			},
			"radarr": {
				{Container: "/movies", Host: "/mnt/user/data/media/movies"},
				{Container: "/downloads", Host: "/mnt/user/data/downloads/torrents"},
			},
			"qbittorrent": {
				{Container: "/downloads", Host: "/mnt/user/data/downloads/torrents"},
			},
		},
		ScanPaths: []string{"/media", "/downloads"},
	}
}

// Load loads configuration from a YAML file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Return default config if file doesn't exist
			return Default(), nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return cfg, nil
}

// Save saves the configuration to a YAML file
func (c *Config) Save(path string) error {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// TranslatePathToHost translates a container path to a host path for a specific service
func (c *Config) TranslatePathToHost(servicePath, service string) string {
	// If service is empty, use local path mappings
	if service == "" {
		return c.translatePath(servicePath, c.LocalPathMappings)
	}

	// Use service-specific mappings
	mappings, ok := c.ServicePathMappings[service]
	if !ok {
		return servicePath
	}

	return c.translatePath(servicePath, mappings)
}

// TranslatePathToContainer translates a host path to a container path for media-finder
func (c *Config) TranslatePathToContainer(hostPath string) string {
	// Find the longest matching host path
	var bestMatch PathMapping
	maxLen := 0

	for _, mapping := range c.LocalPathMappings {
		if strings.HasPrefix(hostPath, mapping.Host) && len(mapping.Host) > maxLen {
			bestMatch = mapping
			maxLen = len(mapping.Host)
		}
	}

	if maxLen == 0 {
		return hostPath
	}

	// Replace host prefix with container prefix
	remainder := strings.TrimPrefix(hostPath, bestMatch.Host)
	return filepath.Join(bestMatch.Container, remainder)
}

// translatePath performs the actual path translation
func (c *Config) translatePath(sourcePath string, mappings []PathMapping) string {
	// Find the longest matching container path
	var bestMatch PathMapping
	maxLen := 0

	for _, mapping := range mappings {
		if strings.HasPrefix(sourcePath, mapping.Container) && len(mapping.Container) > maxLen {
			bestMatch = mapping
			maxLen = len(mapping.Container)
		}
	}

	if maxLen == 0 {
		return sourcePath
	}

	// Replace container prefix with host prefix
	remainder := strings.TrimPrefix(sourcePath, bestMatch.Container)
	return filepath.Join(bestMatch.Host, remainder)
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.DatabasePath == "" {
		return fmt.Errorf("database_path is required")
	}

	if c.ScanWorkers < 1 {
		return fmt.Errorf("scan_workers must be at least 1")
	}

	if c.APITimeout < time.Second {
		return fmt.Errorf("api_timeout must be at least 1 second")
	}

	if len(c.ScanPaths) == 0 {
		return fmt.Errorf("at least one scan path is required")
	}

	return nil
}
