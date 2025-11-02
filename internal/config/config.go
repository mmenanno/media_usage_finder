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
	DatabasePath      string        `yaml:"database_path"`
	ScanWorkers       int           `yaml:"scan_workers"`
	ScanBufferSize    int           `yaml:"scan_buffer_size"`
	APITimeout        time.Duration `yaml:"api_timeout"`
	CORSAllowedOrigin string        `yaml:"cors_allowed_origin"`
	StatsCacheTTL     time.Duration `yaml:"stats_cache_ttl"`

	// Database connection pool settings
	DBMaxOpenConns    int           `yaml:"db_max_open_conns"`
	DBMaxIdleConns    int           `yaml:"db_max_idle_conns"`
	DBConnMaxLifetime time.Duration `yaml:"db_conn_max_lifetime"`

	LocalPathMappings   []PathMapping            `yaml:"local_path_mappings"`
	ServicePathMappings map[string][]PathMapping `yaml:"service_path_mappings"`
	ScanPaths           []string                 `yaml:"scan_paths"`
	Services            Services                 `yaml:"services"`

	// Internal caching (not serialized)
	pathCache *PathCache `yaml:"-"`
}

// PathMapping represents a path mapping between two locations
// For service_path_mappings: Service = path from the external service, Local = path in media-finder's database
// For local_path_mappings: Service = path in media-finder's container, Local = path on the host filesystem
type PathMapping struct {
	Service string `yaml:"service"`
	Local   string `yaml:"local"`
}

// Services contains configuration for all external services
type Services struct {
	Plex        PlexConfig        `yaml:"plex"`
	Sonarr      SonarrConfig      `yaml:"sonarr"`
	Radarr      RadarrConfig      `yaml:"radarr"`
	QBittorrent QBittorrentConfig `yaml:"qbittorrent"`
	Stash       StashConfig       `yaml:"stash"`
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

// StashConfig contains Stash configuration
type StashConfig struct {
	URL    string `yaml:"url"`
	APIKey string `yaml:"api_key"`
}

// Default returns a default configuration
func Default() *Config {
	return &Config{
		DatabasePath:      "/appdata/data/media-finder.db",
		ScanWorkers:       10,
		ScanBufferSize:    100,
		APITimeout:        30 * time.Second,
		CORSAllowedOrigin: "http://localhost:8787",
		StatsCacheTTL:     30 * time.Second,
		DBMaxOpenConns:    25,
		DBMaxIdleConns:    5,
		DBConnMaxLifetime: 5 * time.Minute,
		LocalPathMappings: []PathMapping{
			{Service: "/media", Local: "/mnt/user/data/media"},
			{Service: "/downloads", Local: "/mnt/user/data/downloads/torrents"},
		},
		ServicePathMappings: map[string][]PathMapping{
			"plex": {
				{Service: "/media", Local: "/mnt/user/data/media"},
			},
			"sonarr": {
				{Service: "/tv", Local: "/mnt/user/data/media/tv"},
				{Service: "/downloads", Local: "/mnt/user/data/downloads/torrents"},
			},
			"radarr": {
				{Service: "/movies", Local: "/mnt/user/data/media/movies"},
				{Service: "/downloads", Local: "/mnt/user/data/downloads/torrents"},
			},
			"qbittorrent": {
				{Service: "/downloads", Local: "/mnt/user/data/downloads/torrents"},
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
			cfg := Default()
			cfg.pathCache = NewPathCache()
			return cfg, nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Initialize path cache
	cfg.pathCache = NewPathCache()

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
// Uses caching to improve performance for repeated translations
func (c *Config) TranslatePathToHost(servicePath, service string) string {
	// Check cache first
	cacheKey := service + ":" + servicePath
	if c.pathCache != nil {
		if cached, ok := c.pathCache.Get(cacheKey); ok {
			return cached
		}
	}

	// Perform translation
	var result string
	if service == "" {
		result = c.translatePath(servicePath, c.LocalPathMappings)
	} else {
		mappings, ok := c.ServicePathMappings[service]
		if !ok {
			result = servicePath
		} else {
			result = c.translatePath(servicePath, mappings)
		}
	}

	// Cache the result
	if c.pathCache != nil {
		c.pathCache.Set(cacheKey, result)
	}

	return result
}

// TranslatePathToContainer translates a host path to a container path for media-finder
// Uses caching to improve performance for repeated translations
func (c *Config) TranslatePathToContainer(hostPath string) string {
	// Check cache first
	cacheKey := "local:" + hostPath
	if c.pathCache != nil {
		if cached, ok := c.pathCache.Get(cacheKey); ok {
			return cached
		}
	}

	// Find the longest matching local path
	var bestMatch PathMapping
	maxLen := 0

	for _, mapping := range c.LocalPathMappings {
		if strings.HasPrefix(hostPath, mapping.Local) && len(mapping.Local) > maxLen {
			bestMatch = mapping
			maxLen = len(mapping.Local)
		}
	}

	result := hostPath
	if maxLen > 0 {
		// Replace local prefix with service prefix
		remainder := strings.TrimPrefix(hostPath, bestMatch.Local)
		result = filepath.Join(bestMatch.Service, remainder)
	}

	// Cache the result
	if c.pathCache != nil {
		c.pathCache.Set(cacheKey, result)
	}

	return result
}

// ClearPathCache clears the path translation cache
func (c *Config) ClearPathCache() {
	if c.pathCache != nil {
		c.pathCache.Clear()
	}
}

// GetPathCacheStats returns cache statistics for monitoring
func (c *Config) GetPathCacheStats() (hits, total uint64, hitRate float64) {
	if c.pathCache != nil {
		return c.pathCache.Stats()
	}
	return 0, 0, 0
}

// translatePath performs the actual path translation
func (c *Config) translatePath(sourcePath string, mappings []PathMapping) string {
	// Find the longest matching service path
	var bestMatch PathMapping
	maxLen := 0

	for _, mapping := range mappings {
		if strings.HasPrefix(sourcePath, mapping.Service) && len(mapping.Service) > maxLen {
			bestMatch = mapping
			maxLen = len(mapping.Service)
		}
	}

	if maxLen == 0 {
		return sourcePath
	}

	// Replace service prefix with local prefix
	remainder := strings.TrimPrefix(sourcePath, bestMatch.Service)
	return filepath.Join(bestMatch.Local, remainder)
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.DatabasePath == "" {
		return fmt.Errorf("database_path is required")
	}

	if c.ScanWorkers < 1 {
		return fmt.Errorf("scan_workers must be at least 1")
	}

	if c.ScanBufferSize < 1 {
		return fmt.Errorf("scan_buffer_size must be at least 1")
	}

	if c.APITimeout < time.Second {
		return fmt.Errorf("api_timeout must be at least 1 second")
	}

	if len(c.ScanPaths) == 0 {
		return fmt.Errorf("at least one scan path is required")
	}

	if c.DBMaxOpenConns < 1 {
		return fmt.Errorf("db_max_open_conns must be at least 1")
	}

	if c.DBMaxIdleConns < 0 {
		return fmt.Errorf("db_max_idle_conns cannot be negative")
	}

	if c.StatsCacheTTL < 0 {
		return fmt.Errorf("stats_cache_ttl cannot be negative")
	}

	// Validate CORS origin if provided
	if c.CORSAllowedOrigin != "" && c.CORSAllowedOrigin != "*" {
		// Basic validation: should start with http:// or https://
		if !strings.HasPrefix(c.CORSAllowedOrigin, "http://") && !strings.HasPrefix(c.CORSAllowedOrigin, "https://") {
			return fmt.Errorf("cors_allowed_origin must start with http:// or https:// (or be * for all origins)")
		}
	}

	// Validate path mappings
	if err := c.validatePathMappings(); err != nil {
		return fmt.Errorf("invalid path mappings: %w", err)
	}

	return nil
}

// validatePathMappings validates all path mappings
func (c *Config) validatePathMappings() error {
	// Validate local path mappings
	for i, mapping := range c.LocalPathMappings {
		if err := validatePathMapping(mapping, fmt.Sprintf("local_path_mappings[%d]", i)); err != nil {
			return err
		}
	}

	// Validate service path mappings
	for service, mappings := range c.ServicePathMappings {
		for i, mapping := range mappings {
			if err := validatePathMapping(mapping, fmt.Sprintf("service_path_mappings.%s[%d]", service, i)); err != nil {
				return err
			}
		}
	}

	return nil
}

// validatePathMapping validates a single path mapping
func validatePathMapping(mapping PathMapping, context string) error {
	if mapping.Service == "" {
		return fmt.Errorf("%s: service path cannot be empty", context)
	}

	if mapping.Local == "" {
		return fmt.Errorf("%s: local path cannot be empty", context)
	}

	if !filepath.IsAbs(mapping.Service) {
		return fmt.Errorf("%s: service path must be absolute (got: %s)", context, mapping.Service)
	}

	if !filepath.IsAbs(mapping.Local) {
		return fmt.Errorf("%s: local path must be absolute (got: %s)", context, mapping.Local)
	}

	// Check for directory traversal attempts
	if strings.Contains(mapping.Service, "..") || strings.Contains(mapping.Local, "..") {
		return fmt.Errorf("%s: paths cannot contain '..' (directory traversal)", context)
	}

	return nil
}
