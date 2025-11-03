package api

import (
	"context"
	"time"
)

// ServiceClient is a common interface for all media service clients
// This enables polymorphic usage and simplifies testing
type ServiceClient interface {
	// Test checks the connection to the service
	Test() error
}

// FileProvider is an interface for services that can provide file listings
type FileProvider interface {
	ServiceClient

	// GetAllFiles retrieves all files tracked by the service
	// The return type varies by service, but all implement basic file info
	// Context allows for cancellation during long-running operations
	GetAllFiles(ctx context.Context) (interface{}, error)
}

// Ensure all clients implement ServiceClient
var (
	_ ServiceClient = (*PlexClient)(nil)
	_ ServiceClient = (*SonarrClient)(nil)
	_ ServiceClient = (*RadarrClient)(nil)
	_ ServiceClient = (*QBittorrentClient)(nil)
	_ ServiceClient = (*ArrClient)(nil)
)

// TestableClient is a helper interface that combines all client capabilities
type TestableClient interface {
	ServiceClient
	SetTimeout(timeout time.Duration)
}

// ServiceInfo contains metadata about a service
type ServiceInfo struct {
	Name    string
	Type    string // "media-server", "download-client", "pvr"
	Enabled bool
	URL     string
}

// ServiceHealthStatus represents the health of a service
type ServiceHealthStatus struct {
	ServiceName  string
	Healthy      bool
	Error        string
	ResponseTime time.Duration
}
