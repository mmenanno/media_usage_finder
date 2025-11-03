package stats

import (
	"fmt"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/database"
)

// Stats contains statistical information about the media files
// Note: Size fields use int64 and have a theoretical limit of ~9 exabytes.
// For filesystems with total sizes exceeding this, overflow may occur.
type Stats struct {
	TotalFiles          int64
	TotalSize           int64 // In bytes, max ~9 EB before overflow
	OrphanedFiles       int64
	OrphanedSize        int64 // In bytes, max ~9 EB before overflow
	HardlinkGroups      int64
	ServiceBreakdown    map[string]ServiceStats
	HardlinkSavings     int64 // In bytes, max ~9 EB before overflow
	ActiveServiceCount  int64 // Number of services with files
	OrphanedExtensions  []ExtensionStats
	OrphanedByAge       []AgeGroupStats
	MultiServiceUsage   []ServiceUsageCount
	LargestOrphanedPath string // Path of largest orphaned file
	LargestOrphanedSize int64  // Size of largest orphaned file
}

// ServiceStats contains statistics for a specific service
type ServiceStats struct {
	FileCount   int64
	TotalSize   int64   // In bytes, max ~9 EB before overflow
	AverageSize float64 // Average file size in bytes
}

// ExtensionStats contains statistics for a specific file extension
type ExtensionStats struct {
	Extension string
	FileCount int64
	TotalSize int64 // In bytes, max ~9 EB before overflow
}

// AgeGroupStats contains statistics for files grouped by age
type AgeGroupStats struct {
	AgeGroup  string
	FileCount int64
	TotalSize int64 // In bytes, max ~9 EB before overflow
}

// ServiceUsageCount contains statistics for multi-service file usage
type ServiceUsageCount struct {
	ServiceCount int64 // Number of services using the files
	FileCount    int64
	TotalSize    int64 // In bytes, max ~9 EB before overflow
}

// Calculator calculates statistics from the database
type Calculator struct {
	db *database.DB
}

// NewCalculator creates a new stats calculator
func NewCalculator(db *database.DB) *Calculator {
	return &Calculator{db: db}
}

// Calculate calculates all statistics efficiently using combined queries
func (c *Calculator) Calculate() (*Stats, error) {
	stats := &Stats{
		ServiceBreakdown: make(map[string]ServiceStats),
	}

	// Calculate basic stats in a single query using CTE
	if err := c.calculateBasicStats(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate basic stats: %w", err)
	}

	// Get service breakdown (requires separate queries per service)
	if err := c.calculateServiceBreakdown(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate service breakdown: %w", err)
	}

	// Calculate orphaned file extensions breakdown
	if err := c.calculateOrphanedExtensions(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate orphaned extensions: %w", err)
	}

	// Calculate orphaned files by age
	if err := c.calculateOrphanedByAge(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate orphaned by age: %w", err)
	}

	// Calculate multi-service usage
	if err := c.calculateMultiServiceUsage(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate multi-service usage: %w", err)
	}

	// Find largest orphaned file
	if err := c.calculateLargestOrphaned(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate largest orphaned: %w", err)
	}

	return stats, nil
}

func (c *Calculator) calculateBasicStats(stats *Stats) error {
	// Combined query using CTE for efficiency
	query := `
		WITH basic AS (
			SELECT
				COUNT(*) as total_files,
				COALESCE(SUM(size), 0) as total_size,
				COALESCE(SUM(CASE WHEN is_orphaned = 1 THEN 1 ELSE 0 END), 0) as orphaned_files,
				COALESCE(SUM(CASE WHEN is_orphaned = 1 THEN size ELSE 0 END), 0) as orphaned_size
			FROM files
		),
		hardlinks AS (
			SELECT
				COUNT(*) as hardlink_groups,
				COALESCE(SUM(savings), 0) as hardlink_savings
			FROM (
				SELECT (COUNT(*) - 1) * MAX(size) as savings
				FROM files
				GROUP BY device_id, inode
				HAVING COUNT(*) > 1
			)
		)
		SELECT
			b.total_files, b.total_size, b.orphaned_files, b.orphaned_size,
			h.hardlink_groups, h.hardlink_savings
		FROM basic b, hardlinks h
	`

	return c.db.Conn().QueryRow(query).Scan(
		&stats.TotalFiles,
		&stats.TotalSize,
		&stats.OrphanedFiles,
		&stats.OrphanedSize,
		&stats.HardlinkGroups,
		&stats.HardlinkSavings,
	)
}

func (c *Calculator) calculateServiceBreakdown(stats *Stats) error {
	// Optimized: Single query instead of 5 separate queries
	query := `
		SELECT u.service, COUNT(DISTINCT f.id), COALESCE(SUM(f.size), 0)
		FROM files f
		INNER JOIN usage u ON f.id = u.file_id
		WHERE u.service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash')
		GROUP BY u.service
	`

	rows, err := c.db.Conn().Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Initialize all services with zero stats
	services := []string{"plex", "sonarr", "radarr", "qbittorrent", "stash"}
	for _, service := range services {
		stats.ServiceBreakdown[service] = ServiceStats{FileCount: 0, TotalSize: 0}
	}

	// Update with actual values from query
	for rows.Next() {
		var service string
		var serviceStats ServiceStats
		if err := rows.Scan(&service, &serviceStats.FileCount, &serviceStats.TotalSize); err != nil {
			return err
		}
		// Calculate average file size
		if serviceStats.FileCount > 0 {
			serviceStats.AverageSize = float64(serviceStats.TotalSize) / float64(serviceStats.FileCount)
		}
		stats.ServiceBreakdown[service] = serviceStats
	}

	// Count active services (services with files)
	activeCount := int64(0)
	for _, serviceStats := range stats.ServiceBreakdown {
		if serviceStats.FileCount > 0 {
			activeCount++
		}
	}
	stats.ActiveServiceCount = activeCount

	return rows.Err()
}

func (c *Calculator) calculateOrphanedExtensions(stats *Stats) error {
	// Query to get top 10 file extensions among orphaned files
	// Extract extension using a simple heuristic: get last 10 chars, find first dot, extract from there
	// This works for all reasonable file extensions (up to 9 characters)
	query := `
		SELECT
			LOWER(
				CASE
					WHEN INSTR(SUBSTR(path, -10), '.') > 0
					THEN SUBSTR(path, LENGTH(path) - 10 + INSTR(SUBSTR(path, -10), '.'))
					ELSE ''
				END
			) as extension,
			COUNT(*) as count,
			COALESCE(SUM(size), 0) as total_size
		FROM files
		WHERE is_orphaned = 1
			AND path LIKE '%.%'
			AND INSTR(path, '.') > 0
		GROUP BY extension
		HAVING extension != ''
		ORDER BY total_size DESC
		LIMIT 10
	`

	rows, err := c.db.Conn().Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	stats.OrphanedExtensions = []ExtensionStats{}
	for rows.Next() {
		var ext ExtensionStats
		if err := rows.Scan(&ext.Extension, &ext.FileCount, &ext.TotalSize); err != nil {
			return err
		}
		stats.OrphanedExtensions = append(stats.OrphanedExtensions, ext)
	}

	return rows.Err()
}

func (c *Calculator) calculateOrphanedByAge(stats *Stats) error {
	// Query to group orphaned files by age
	query := `
		SELECT
			CASE
				WHEN modified_time >= strftime('%s', 'now', '-30 days') THEN 'Last 30 days'
				WHEN modified_time >= strftime('%s', 'now', '-90 days') THEN '30-90 days ago'
				WHEN modified_time >= strftime('%s', 'now', '-1 year') THEN '90 days - 1 year ago'
				ELSE 'Over 1 year ago'
			END as age_group,
			COUNT(*) as count,
			COALESCE(SUM(size), 0) as total_size
		FROM files
		WHERE is_orphaned = 1
		GROUP BY age_group
		ORDER BY
			CASE age_group
				WHEN 'Last 30 days' THEN 1
				WHEN '30-90 days ago' THEN 2
				WHEN '90 days - 1 year ago' THEN 3
				ELSE 4
			END
	`

	rows, err := c.db.Conn().Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	stats.OrphanedByAge = []AgeGroupStats{}
	for rows.Next() {
		var age AgeGroupStats
		if err := rows.Scan(&age.AgeGroup, &age.FileCount, &age.TotalSize); err != nil {
			return err
		}
		stats.OrphanedByAge = append(stats.OrphanedByAge, age)
	}

	return rows.Err()
}

func (c *Calculator) calculateMultiServiceUsage(stats *Stats) error {
	// Query to count files by number of services using them
	query := `
		SELECT
			service_count,
			COUNT(*) as file_count,
			COALESCE(SUM(size), 0) as total_size
		FROM (
			SELECT f.id, f.size, COUNT(u.service) as service_count
			FROM files f
			INNER JOIN usage u ON f.id = u.file_id
			GROUP BY f.id
		)
		GROUP BY service_count
		ORDER BY service_count DESC
	`

	rows, err := c.db.Conn().Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	stats.MultiServiceUsage = []ServiceUsageCount{}
	for rows.Next() {
		var usage ServiceUsageCount
		if err := rows.Scan(&usage.ServiceCount, &usage.FileCount, &usage.TotalSize); err != nil {
			return err
		}
		stats.MultiServiceUsage = append(stats.MultiServiceUsage, usage)
	}

	return rows.Err()
}

func (c *Calculator) calculateLargestOrphaned(stats *Stats) error {
	// Query to find the largest orphaned file
	query := `
		SELECT path, size
		FROM files
		WHERE is_orphaned = 1
		ORDER BY size DESC
		LIMIT 1
	`

	err := c.db.Conn().QueryRow(query).Scan(&stats.LargestOrphanedPath, &stats.LargestOrphanedSize)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return err
	}

	// It's okay if there are no orphaned files
	return nil
}

var sizeUnits = []string{"B", "KB", "MB", "GB", "TB", "PB"}

// FormatSize formats a size in bytes to a human-readable string
func FormatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.2f %s", float64(bytes)/float64(div), sizeUnits[exp+1])
}

// FormatDuration formats a duration to a human-readable string
func FormatDuration(d time.Duration) string {
	if d == 0 {
		return "calculating..."
	}

	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm", h, m)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
