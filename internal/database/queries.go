package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// File represents a file in the database
type File struct {
	ID           int64
	Path         string
	Size         int64
	Inode        int64
	DeviceID     int64
	ModifiedTime time.Time
	ScanID       int64
	LastVerified time.Time
	IsOrphaned   bool
	Extension    string
	CreatedAt    time.Time
}

// scanFileRow scans a single file row from a query result
func scanFileRow(scanner interface {
	Scan(dest ...interface{}) error
}) (*File, error) {
	file := &File{}
	var modTime, lastVerified, createdAt int64
	var scanID sql.NullInt64

	err := scanner.Scan(
		&file.ID,
		&file.Path,
		&file.Size,
		&file.Inode,
		&file.DeviceID,
		&modTime,
		&scanID,
		&lastVerified,
		&file.IsOrphaned,
		&file.Extension,
		&createdAt,
	)
	if err != nil {
		return nil, err
	}

	file.ModifiedTime = time.Unix(modTime, 0)
	file.LastVerified = time.Unix(lastVerified, 0)
	file.CreatedAt = time.Unix(createdAt, 0)

	// Handle NULL scan_id (can occur if scans are deleted)
	if scanID.Valid {
		file.ScanID = scanID.Int64
	} else {
		file.ScanID = 0 // Default to 0 for orphaned/deleted scan references
	}

	return file, nil
}

// Usage represents a service using a file
type Usage struct {
	ID            int64                  `json:"id"`
	FileID        int64                  `json:"file_id"`
	Service       string                 `json:"service"`
	ReferencePath string                 `json:"reference_path"`
	Metadata      map[string]interface{} `json:"metadata"`
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
}

// Scan represents a scan operation
type Scan struct {
	ID                int64
	StartedAt         time.Time
	CompletedAt       *time.Time
	Status            string
	FilesScanned      int64
	Errors            *string
	ScanType          string
	CurrentPhase      *string
	LastProcessedPath *string
	ResumeFromScanID  *int64
	DeletedFilesCount int64
	CreatedAt         time.Time
}

// FileDiskLocation represents a file's location on a specific disk
// This enables tracking files across multiple physical disks (Unraid support)
// while keeping FUSE paths as the canonical identifier for service matching
type FileDiskLocation struct {
	ID           int64
	FileID       int64
	DiskName     string
	DiskDeviceID int64
	DiskPath     string
	Size         int64
	Inode        int64
	ModifiedTime time.Time
	LastVerified time.Time
	CreatedAt    time.Time
}

// ScanLog represents a log entry for a scan operation
type ScanLog struct {
	ID        int64
	ScanID    int64
	Timestamp time.Time
	Level     string
	Phase     *string
	Message   string
	CreatedAt time.Time
}

// MissingFile represents a file that a service reports but doesn't exist in the filesystem
type MissingFile struct {
	ID             int64                  `json:"id"`
	ScanID         int64                  `json:"scan_id"`
	Service        string                 `json:"service"`
	ServicePath    string                 `json:"service_path"`
	TranslatedPath string                 `json:"translated_path"`
	Size           int64                  `json:"size"`
	ServiceGroup   string                 `json:"service_group"`
	ServiceGroupID string                 `json:"service_group_id"`
	Metadata       map[string]interface{} `json:"metadata"`
	CreatedAt      time.Time              `json:"created_at"`
}

// LogFilters defines filters for querying scan logs
type LogFilters struct {
	ScanID     *int64
	Level      string
	Phase      string
	SearchText string
	StartTime  *time.Time
	EndTime    *time.Time
	Limit      int
	Offset     int
}

// AuditLogFilters contains filters for querying audit log entries
type AuditLogFilters struct {
	Action     string
	EntityType string
	SearchText string
	ScanID     *int64
	Limit      int
	Offset     int
}

// buildInClause builds an IN clause with placeholders for SQL queries
func buildInClause(count int) string {
	if count == 0 {
		return ""
	}
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "?"
	}
	return strings.Join(placeholders, ",")
}

// GetFilesByPaths retrieves multiple files by their paths in one query (batch lookup)
// Handles SQLite variable limit by batching queries into chunks of 900 parameters

// special characters like dots, hyphens, or other FTS5 operators.
func sanitizeFTS5Query(query string) string {
	// Escape any existing double quotes by doubling them (FTS5 convention)
	escaped := strings.ReplaceAll(query, `"`, `""`)
	// Wrap in double quotes to make it a phrase search (literal match)
	return `"` + escaped + `"`
}

// SearchFiles searches for files by path using FTS

func ValidateOrderBy(orderBy string) string {
	validColumns := map[string]bool{
		"path":          true,
		"size":          true,
		"modified_time": true,
		"last_verified": true,
		"id":            true,
	}

	if validColumns[orderBy] {
		return orderBy
	}
	return "path" // default
}

// ValidateDirection ensures only valid SQL direction keywords are used
func ValidateDirection(direction string) string {
	if direction == "asc" || direction == "ASC" {
		return "ASC"
	}
	if direction == "desc" || direction == "DESC" {
		return "DESC"
	}
	return "ASC" // default
}

// ValidateHardlinkOrderBy validates orderBy parameter for hardlink queries
func ValidateHardlinkOrderBy(orderBy string) string {
	validColumns := map[string]bool{
		"link_count":  true, // Number of files in group
		"space_saved": true, // Space saved by hardlinks
		"first_path":  true, // Alphabetical by first path
	}

	if validColumns[orderBy] {
		return orderBy
	}
	return "space_saved" // default (most useful for users)
}

// GetFileExtensions returns a list of distinct file extensions in the database
// Optionally filtered by orphaned status and service
// Uses the extension column for efficient querying with index support

func (db *DB) SetConfig(key, value string) error {
	query := `
		INSERT INTO config (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = strftime('%s', 'now')
	`
	_, err := db.conn.Exec(query, key, value)
	return err
}

// GetConfig retrieves a configuration value
func (db *DB) GetConfig(key string) (string, error) {
	var value string
	err := db.conn.QueryRow(`SELECT value FROM config WHERE key = ?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

// Admin/Maintenance Operations

// ClearAllFiles deletes all file records (cascades to usage via foreign key)

func (db *DB) VacuumDatabase() error {
	// VACUUM must be run outside a transaction
	if _, err := db.conn.Exec(`VACUUM`); err != nil {
		return fmt.Errorf("failed to vacuum database: %w", err)
	}

	if _, err := db.conn.Exec(`ANALYZE`); err != nil {
		return fmt.Errorf("failed to analyze database: %w", err)
	}

	// Record timestamp so the Advanced page can show staleness
	_ = db.MarkFullVacuum()

	// Log the action
	_, _ = db.conn.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('config_change', 'database', 0, 'Vacuumed and analyzed database')`,
	)

	return nil
}

// RebuildFTSIndex rebuilds the full-text search index
func (db *DB) RebuildFTSIndex() error {
	// Rebuild by inserting into the special fts table with 'rebuild' command
	if _, err := db.conn.Exec(`INSERT INTO files_fts(files_fts) VALUES('rebuild')`); err != nil {
		return fmt.Errorf("failed to rebuild FTS index: %w", err)
	}

	// Log the action
	_, _ = db.conn.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('config_change', 'database', 0, 'Rebuilt FTS search index')`,
	)

	return nil
}

// CleanStaleScansOnStartup marks all running scans as interrupted on application startup
// This handles cases where the app was restarted while scans were running

// DatabaseStats holds database statistics
type DatabaseStats struct {
	FileCount       int64
	OrphanedCount   int64
	UsageCount      int64
	ScanCount       int64
	AuditLogCount   int64
	HardlinkGroups  int64
	TotalSize       int64
	OrphanedSize    int64
	HardlinkSavings int64
	DatabaseSizeKB  int64
	LastVacuum      *time.Time
}

// GetDatabaseStats retrieves comprehensive database statistics
// Uses a single CTE query for better performance
func (db *DB) GetDatabaseStats() (*DatabaseStats, error) {
	stats := &DatabaseStats{}

	// Combined query using CTE for most stats
	query := `
		WITH file_stats AS (
			SELECT
				COUNT(*) as file_count,
				COALESCE(SUM(CASE WHEN is_orphaned = 1 THEN 1 ELSE 0 END), 0) as orphaned_count,
				COALESCE(SUM(size), 0) as total_size,
				COALESCE(SUM(CASE WHEN is_orphaned = 1 THEN size ELSE 0 END), 0) as orphaned_size
			FROM files
		),
		hardlink_stats AS (
			SELECT
				COUNT(*) as hardlink_groups,
				COALESCE(SUM((cnt - 1) * size), 0) as hardlink_savings
			FROM (
				SELECT device_id, inode, size, COUNT(*) as cnt
				FROM files
				GROUP BY device_id, inode
				HAVING COUNT(*) > 1
			)
		),
		table_counts AS (
			SELECT
				(SELECT COUNT(*) FROM usage) as usage_count,
				(SELECT COUNT(*) FROM scans) as scan_count,
				(SELECT COUNT(*) FROM audit_log) as audit_log_count
		)
		SELECT
			f.file_count, f.orphaned_count, f.total_size, f.orphaned_size,
			h.hardlink_groups, h.hardlink_savings,
			t.usage_count, t.scan_count, t.audit_log_count
		FROM file_stats f, hardlink_stats h, table_counts t
	`

	err := db.conn.QueryRow(query).Scan(
		&stats.FileCount,
		&stats.OrphanedCount,
		&stats.TotalSize,
		&stats.OrphanedSize,
		&stats.HardlinkGroups,
		&stats.HardlinkSavings,
		&stats.UsageCount,
		&stats.ScanCount,
		&stats.AuditLogCount,
	)
	if err != nil {
		return nil, err
	}

	// Database size (requires separate PRAGMA queries)
	var pageCount, pageSize int64
	if err := db.conn.QueryRow(`PRAGMA page_count`).Scan(&pageCount); err != nil {
		return nil, err
	}
	if err := db.conn.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		return nil, err
	}
	stats.DatabaseSizeKB = (pageCount * pageSize) / 1024

	return stats, nil
}

func (db *DB) ClearConfig() (int64, error) {
	result, err := db.conn.Exec(`DELETE FROM config`)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	// Log the action
	_, _ = db.conn.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('config_change', 'config', 0, ?)`,
		fmt.Sprintf("Cleared all configuration (%d records)", count),
	)

	return count, nil
}

// ClearAuditLog deletes old audit log entries (older than specified days)

func ExtractExtension(path string) string {
	// Find the last two dots after the last slash
	lastSlash := -1
	lastDot := -1
	secondLastDot := -1

	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' && lastSlash == -1 {
			lastSlash = i
			break // Stop after finding the last slash
		}
		if path[i] == '.' {
			if lastDot == -1 {
				lastDot = i
			} else if secondLastDot == -1 {
				secondLastDot = i
				// We found both dots, no need to continue
				break
			}
		}
	}

	// If we didn't find a dot after the last slash, no extension
	if lastDot <= lastSlash {
		return ""
	}

	// Extract the last extension part (lowercase)
	lastExt := toLower(path[lastDot:])

	// If there's no second dot, just return the last extension
	if secondLastDot <= lastSlash {
		return lastExt
	}

	// Extract the part between the two dots (without dots, lowercase)
	middlePart := toLower(path[secondLastDot+1 : lastDot])

	// Check if this is a compound extension we should keep
	if isCompoundExtension(middlePart, lastExt) {
		// Return the full compound extension
		return toLower(path[secondLastDot:])
	}

	// Not a compound extension, just return the last part
	return lastExt
}

// isCompoundExtension determines if two extension parts form a compound extension
func isCompoundExtension(middle, lastExt string) bool {
	// qBittorrent incomplete download marker
	if lastExt == ".!qb" {
		return true
	}

	// Calibre backup files (.original_epub, .original_mobi, etc.)
	if middle == "original" {
		return true
	}

	// Common archive compound extensions
	if middle == "tar" {
		// .tar.gz, .tar.bz2, .tar.xz, .tar.zst, .tar.lz, .tar.lz4, .tar.z
		archiveExts := []string{".gz", ".bz2", ".xz", ".zst", ".lz", ".lz4", ".z"}
		for _, ext := range archiveExts {
			if lastExt == ext {
				return true
			}
		}
	}

	// Common compound extensions in media/backup contexts
	compoundMiddles := []string{"backup", "tmp", "part", "old"}
	for _, cm := range compoundMiddles {
		if middle == cm {
			return true
		}
	}

	return false
}

// toLower converts a string to lowercase (simple ASCII version)
func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			result[i] = s[i] + 32
		} else {
			result[i] = s[i]
		}
	}
	return string(result)
}

// ===== File Disk Location Functions =====

// UpsertFileDiskLocation inserts or updates a file disk location record
