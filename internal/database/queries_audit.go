package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// AuditLogEntry represents a single audit log entry
type AuditLogEntry struct {
	ID         int64
	Action     string
	EntityType string
	EntityID   *int64
	ScanID     *int64
	Details    string
	CreatedAt  time.Time
}

// GetAuditLog retrieves paginated audit log entries with optional filters
func (db *DB) GetAuditLog(filters AuditLogFilters) ([]*AuditLogEntry, int, error) {
	// Build WHERE clause based on filters
	var conditions []string
	var args []interface{}

	if filters.Action != "" {
		conditions = append(conditions, "action = ?")
		args = append(args, filters.Action)
	}

	if filters.EntityType != "" {
		conditions = append(conditions, "entity_type = ?")
		args = append(args, filters.EntityType)
	}

	if filters.ScanID != nil {
		conditions = append(conditions, "scan_id = ?")
		args = append(args, *filters.ScanID)
	}

	if filters.SearchText != "" {
		conditions = append(conditions, "details LIKE ?")
		args = append(args, "%"+filters.SearchText+"%")
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Count total with filters
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM audit_log %s", whereClause)
	var total int
	err := db.conn.QueryRow(countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	// Get entries with filters
	query := fmt.Sprintf(`
		SELECT id, action, entity_type, entity_id, scan_id, details, created_at
		FROM audit_log
		%s
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`, whereClause)

	// Add limit and offset to args
	queryArgs := append(args, filters.Limit, filters.Offset)

	rows, err := db.conn.Query(query, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var entries []*AuditLogEntry
	for rows.Next() {
		entry := &AuditLogEntry{}
		var createdAt int64
		var entityID sql.NullInt64
		var scanID sql.NullInt64

		err := rows.Scan(
			&entry.ID,
			&entry.Action,
			&entry.EntityType,
			&entityID,
			&scanID,
			&entry.Details,
			&createdAt,
		)
		if err != nil {
			return nil, 0, err
		}

		if entityID.Valid {
			entry.EntityID = &entityID.Int64
		}
		if scanID.Valid {
			entry.ScanID = &scanID.Int64
		}
		entry.CreatedAt = time.Unix(createdAt, 0)

		entries = append(entries, entry)
	}

	return entries, total, rows.Err()
}

// ClearConfig deletes all configuration values
func (db *DB) ClearAuditLog(olderThanDays int) (int64, error) {
	cutoff := time.Now().AddDate(0, 0, -olderThanDays).Unix()

	result, err := db.conn.Exec(`DELETE FROM audit_log WHERE created_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if count > 0 {
		// Log the action
		_, _ = db.conn.Exec(
			`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('delete', 'audit_log', 0, ?)`,
			fmt.Sprintf("Cleared old audit log entries (%d records older than %d days)", count, olderThanDays),
		)
	}

	return count, nil
}

// ExtractExtension extracts the file extension from a path
// Returns lowercase extension including the dot (e.g., ".mkv")
// Handles compound extensions like ".mkv.!qb", ".original_epub", ".tar.gz"
// Returns empty string if no extension found
func (db *DB) LogConsolidation(keepFile, deleteFile *DuplicateFile, reason string) error {
	query := `
		INSERT INTO audit_log (action, entity_type, entity_id, details, created_at)
		VALUES (?, ?, ?, ?, ?)
	`

	details := map[string]interface{}{
		"operation":    "cross_disk_consolidation",
		"kept_file":    keepFile.Path,
		"kept_disk":    keepFile.DiskName,
		"deleted_file": deleteFile.Path,
		"deleted_disk": deleteFile.DiskName,
		"file_hash":    keepFile.ID, // Use ID as a reference
		"size":         deleteFile.Size,
		"reason":       reason,
	}

	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("failed to marshal details: %w", err)
	}

	_, err = db.conn.Exec(query, "consolidate", "file", deleteFile.ID, string(detailsJSON), time.Now().Unix())
	return err
}

// LogHardlinkCreation logs a hardlink creation operation to the audit log
func (db *DB) LogHardlinkCreation(primaryFile, duplicateFile *DuplicateFile, reason string) error {
	query := `
		INSERT INTO audit_log (action, entity_type, entity_id, details, created_at)
		VALUES (?, ?, ?, ?, ?)
	`

	details := map[string]interface{}{
		"operation":      "hardlink_creation",
		"primary_file":   primaryFile.Path,
		"duplicate_file": duplicateFile.Path,
		"disk":           primaryFile.DiskName,
		"size_saved":     duplicateFile.Size,
		"reason":         reason,
	}

	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("failed to marshal details: %w", err)
	}

	_, err = db.conn.Exec(query, "hardlink", "file", duplicateFile.ID, string(detailsJSON), time.Now().Unix())
	return err
}

// UpdateFileInode updates the device_id and inode for a file after hardlinking
// This ensures the database reflects the actual filesystem state after hardlink operations
