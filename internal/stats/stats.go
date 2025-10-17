package stats

import (
	"fmt"

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

// Calculate calculates all statistics
func (c *Calculator) Calculate() (*Stats, error) {
	stats := &Stats{
		ServiceBreakdown: make(map[string]ServiceStats),
	}

	// Get total files and size
	if err := c.calculateTotals(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate totals: %w", err)
	}

	// Get orphaned files and size
	if err := c.calculateOrphaned(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate orphaned: %w", err)
	}

	// Get hardlink statistics
	if err := c.calculateHardlinks(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate hardlinks: %w", err)
	}

	// Get service breakdown
	if err := c.calculateServiceBreakdown(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate service breakdown: %w", err)
	}

	return stats, nil
}

func (c *Calculator) calculateTotals(stats *Stats) error {
	query := `SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files`
	return c.db.Conn().QueryRow(query).Scan(&stats.TotalFiles, &stats.TotalSize)
}

func (c *Calculator) calculateOrphaned(stats *Stats) error {
	query := `SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files WHERE is_orphaned = 1`
	return c.db.Conn().QueryRow(query).Scan(&stats.OrphanedFiles, &stats.OrphanedSize)
}

func (c *Calculator) calculateHardlinks(stats *Stats) error {
	// Count distinct (device_id, inode) pairs with more than one file
	query := `
		SELECT COUNT(*), COALESCE(SUM(savings), 0)
		FROM (
			SELECT
				device_id,
				inode,
				(COUNT(*) - 1) * MAX(size) as savings
			FROM files
			GROUP BY device_id, inode
			HAVING COUNT(*) > 1
		)
	`
	return c.db.Conn().QueryRow(query).Scan(&stats.HardlinkGroups, &stats.HardlinkSavings)
}

func (c *Calculator) calculateServiceBreakdown(stats *Stats) error {
	services := []string{"plex", "sonarr", "radarr", "qbittorrent"}

	for _, service := range services {
		query := `
			SELECT COUNT(DISTINCT f.id), COALESCE(SUM(f.size), 0)
			FROM files f
			INNER JOIN usage u ON f.id = u.file_id
			WHERE u.service = ?
		`

		var serviceStats ServiceStats
		err := c.db.Conn().QueryRow(query, service).Scan(&serviceStats.FileCount, &serviceStats.TotalSize)
		if err != nil {
			return err
		}

		stats.ServiceBreakdown[service] = serviceStats
	}

	return nil
}

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

	units := []string{"KB", "MB", "GB", "TB", "PB"}
	return fmt.Sprintf("%.2f %s", float64(bytes)/float64(div), units[exp])
}
