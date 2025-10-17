package stats

import (
	"fmt"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/database"
)

// Stats contains statistical information about the media files
type Stats struct {
	TotalFiles       int64
	TotalSize        int64
	OrphanedFiles    int64
	OrphanedSize     int64
	HardlinkGroups   int64
	ServiceBreakdown map[string]ServiceStats
	HardlinkSavings  int64
}

// ServiceStats contains statistics for a specific service
type ServiceStats struct {
	FileCount int64
	TotalSize int64
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

	return stats, nil
}

func (c *Calculator) calculateBasicStats(stats *Stats) error {
	// Combined query using CTE for efficiency
	query := `
		WITH basic AS (
			SELECT
				COUNT(*) as total_files,
				COALESCE(SUM(size), 0) as total_size,
				SUM(CASE WHEN is_orphaned = 1 THEN 1 ELSE 0 END) as orphaned_files,
				SUM(CASE WHEN is_orphaned = 1 THEN size ELSE 0 END) as orphaned_size
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
	// Optimized: Single query instead of 4 separate queries
	query := `
		SELECT u.service, COUNT(DISTINCT f.id), COALESCE(SUM(f.size), 0)
		FROM files f
		INNER JOIN usage u ON f.id = u.file_id
		WHERE u.service IN ('plex', 'sonarr', 'radarr', 'qbittorrent')
		GROUP BY u.service
	`

	rows, err := c.db.Conn().Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Initialize all services with zero stats
	services := []string{"plex", "sonarr", "radarr", "qbittorrent"}
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
		stats.ServiceBreakdown[service] = serviceStats
	}

	return rows.Err()
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
