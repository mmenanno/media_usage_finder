package constants

// Pagination constants
const (
	// DefaultFilesPerPage is the default number of files to show per page
	DefaultFilesPerPage = 50

	// DefaultHardlinkGroupsPerPage is the default number of hardlink groups per page
	DefaultHardlinkGroupsPerPage = 50

	// DefaultScansPerPage is the default number of scans per page
	DefaultScansPerPage = 20

	// MaxFilesPerPage is the maximum number of files that can be requested per page
	MaxFilesPerPage = 1000

	// MaxExportFiles is the maximum number of files to export at once
	MaxExportFiles = 100000

	// ExportBatchSize is the batch size for streaming exports
	ExportBatchSize = 1000
)

// Progress tracking constants
const (
	// MaxStoredErrors is the maximum number of errors to keep in memory
	MaxStoredErrors = 1000

	// ErrorSliceCapacity is the initial capacity for error slices
	ErrorSliceCapacity = 100

	// LogChannelBuffer is the buffer size for log channels
	LogChannelBuffer = 100

	// LogListenerBuffer is the buffer size for individual log listeners
	LogListenerBuffer = 50
)

// Worker pool constants
const (
	// DefaultWorkerCount is the default number of workers
	DefaultWorkerCount = 10

	// DefaultBufferSize is the default channel buffer size
	DefaultBufferSize = 100
)

// Cache constants
const (
	// DefaultStatsCacheTTL is the default TTL for stats cache (30 seconds)
	DefaultStatsCacheTTL = 30
)

// API and concurrency constants
const (
	// MaxConcurrentTorrentWorkers limits concurrent torrent processing in qBittorrent
	MaxConcurrentTorrentWorkers = 20

	// DefaultAPITimeoutSeconds is the default timeout for API requests in seconds
	DefaultAPITimeoutSeconds = 30

	// MaxAPITimeoutMultiplier is the maximum multiplier for API timeout
	MaxAPITimeoutMultiplier = 2

	// ProgressPollIntervalMS is the interval for polling progress updates (milliseconds)
	ProgressPollIntervalMS = 2000

	// PeriodicCleanupIntervalSeconds is the interval for cleaning up stale listeners
	PeriodicCleanupIntervalSeconds = 30
)

// Rate limiting constants
const (
	// DefaultRequestsPerSecond is the default rate limit for API endpoints
	DefaultRequestsPerSecond = 10

	// DefaultBurstSize is the default burst size for rate limiting
	DefaultBurstSize = 20
)

// Frontend constants
const (
	// DefaultBatchConcurrency is the default concurrency for batch operations
	DefaultBatchConcurrency = 10
)

// Path translation cache constants
const (
	// PathCacheSize is the maximum number of cached path translations
	// Increased from 10k to 50k for better performance with large datasets (90TB+)
	PathCacheSize = 50000

	// PathCacheCleanupThreshold is the percentage at which to trigger cleanup
	PathCacheCleanupThreshold = 0.9
)
