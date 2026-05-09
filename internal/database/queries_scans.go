package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

func (db *DB) CreateScan(scanType string) (*Scan, error) {
	query := `
		INSERT INTO scans (started_at, status, scan_type)
		VALUES (?, 'running', ?)
		RETURNING id
	`

	scan := &Scan{
		StartedAt: time.Now(),
		Status:    "running",
		ScanType:  scanType,
	}

	err := db.conn.QueryRow(query, scan.StartedAt.Unix(), scanType).Scan(&scan.ID)
	if err != nil {
		return nil, err
	}

	return scan, nil
}

// CreateResumeScan creates a new scan that resumes from an interrupted scan
func (db *DB) CreateResumeScan(scanType string, resumeFromScanID int64) (*Scan, error) {
	query := `
		INSERT INTO scans (started_at, status, scan_type, resume_from_scan_id)
		VALUES (?, 'running', ?, ?)
		RETURNING id
	`

	scan := &Scan{
		StartedAt:        time.Now(),
		Status:           "running",
		ScanType:         scanType,
		ResumeFromScanID: &resumeFromScanID,
	}

	err := db.conn.QueryRow(query, scan.StartedAt.Unix(), scanType, resumeFromScanID).Scan(&scan.ID)
	if err != nil {
		return nil, err
	}

	return scan, nil
}

// UpdateScan updates a scan record
func (db *DB) UpdateScan(scanID int64, status string, filesScanned int64, errors *string) error {
	completedAt := time.Now().Unix()
	query := `
		UPDATE scans
		SET completed_at = ?, status = ?, files_scanned = ?, errors = ?
		WHERE id = ?
	`

	_, err := db.conn.Exec(query, completedAt, status, filesScanned, errors, scanID)
	return err
}

// IncrementScanFiles increments the files_scanned counter
func (db *DB) IncrementScanFiles(scanID int64, count int64) error {
	query := `UPDATE scans SET files_scanned = files_scanned + ? WHERE id = ?`
	_, err := db.conn.Exec(query, count, scanID)
	return err
}

// UpdateScanPhase updates the current_phase of a scan
func (db *DB) UpdateScanPhase(scanID int64, phase string) error {
	query := `UPDATE scans SET current_phase = ? WHERE id = ?`
	_, err := db.conn.Exec(query, phase, scanID)
	return err
}

// UpdateScanStatus updates only the status and optionally errors of a scan
func (db *DB) UpdateScanStatus(scanID int64, status string, errors string) error {
	query := `UPDATE scans SET status = ?, errors = ?, completed_at = ? WHERE id = ?`
	completedAt := time.Now().Unix()
	_, err := db.conn.Exec(query, status, errors, completedAt, scanID)
	return err
}

// CompleteScan marks a scan as completed with a specific status
func (db *DB) CompleteScan(scanID int64, status string, errors string) error {
	query := `UPDATE scans SET status = ?, errors = ?, completed_at = ? WHERE id = ?`
	completedAt := time.Now().Unix()
	_, err := db.conn.Exec(query, status, errors, completedAt, scanID)
	return err
}

// UpdateScanCheckpoint updates the last_processed_path checkpoint for resume functionality
func (db *DB) UpdateScanCheckpoint(scanID int64, lastPath string) error {
	query := `UPDATE scans SET last_processed_path = ? WHERE id = ?`
	_, err := db.conn.Exec(query, lastPath, scanID)
	return err
}

// UpdateScanFilesProcessed updates only the files_scanned count for a scan
// Used to persist progress when scans are interrupted, cancelled, or crash
func (db *DB) UpdateScanFilesProcessed(scanID int64, filesProcessed int64) error {
	query := `UPDATE scans SET files_scanned = ? WHERE id = ?`
	_, err := db.conn.Exec(query, filesProcessed, scanID)
	return err
}

// UpdateScanDeletedCount updates the deleted files count for a scan
func (db *DB) UpdateScanDeletedCount(scanID int64, deletedCount int64) error {
	query := `UPDATE scans SET deleted_files_count = ? WHERE id = ?`
	_, err := db.conn.Exec(query, deletedCount, scanID)
	return err
}

// GetLastInterruptedScan returns the most recent interrupted scan that can be resumed
func (db *DB) GetLastInterruptedScan() (*Scan, error) {
	query := `
		SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, created_at
		FROM scans
		WHERE status = 'interrupted' AND current_phase = 'Scanning filesystem'
		ORDER BY started_at DESC
		LIMIT 1
	`

	scan := &Scan{}
	var startedAt, createdAt int64
	var completedAt sql.NullInt64
	var errors sql.NullString
	var currentPhase sql.NullString
	var lastProcessedPath sql.NullString
	var resumeFromScanID sql.NullInt64

	err := db.conn.QueryRow(query).Scan(
		&scan.ID,
		&startedAt,
		&completedAt,
		&scan.Status,
		&scan.FilesScanned,
		&errors,
		&scan.ScanType,
		&currentPhase,
		&lastProcessedPath,
		&resumeFromScanID,
		&createdAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	scan.StartedAt = time.Unix(startedAt, 0)
	scan.CreatedAt = time.Unix(createdAt, 0)

	if completedAt.Valid {
		t := time.Unix(completedAt.Int64, 0)
		scan.CompletedAt = &t
	}
	if errors.Valid {
		scan.Errors = &errors.String
	}
	if currentPhase.Valid {
		scan.CurrentPhase = &currentPhase.String
	}
	if lastProcessedPath.Valid {
		scan.LastProcessedPath = &lastProcessedPath.String
	}
	if resumeFromScanID.Valid {
		scan.ResumeFromScanID = &resumeFromScanID.Int64
	}

	return scan, nil
}

// GetCurrentScan returns the currently running scan, if any
func (db *DB) GetCurrentScan() (*Scan, error) {
	query := `
		SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, created_at
		FROM scans
		WHERE status = 'running'
		ORDER BY started_at DESC
		LIMIT 1
	`

	scan := &Scan{}
	var startedAt, createdAt int64
	var completedAt sql.NullInt64
	var errors sql.NullString
	var currentPhase sql.NullString
	var lastProcessedPath sql.NullString
	var resumeFromScanID sql.NullInt64

	err := db.conn.QueryRow(query).Scan(
		&scan.ID,
		&startedAt,
		&completedAt,
		&scan.Status,
		&scan.FilesScanned,
		&errors,
		&scan.ScanType,
		&currentPhase,
		&lastProcessedPath,
		&resumeFromScanID,
		&createdAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	scan.StartedAt = time.Unix(startedAt, 0)
	scan.CreatedAt = time.Unix(createdAt, 0)

	if completedAt.Valid {
		t := time.Unix(completedAt.Int64, 0)
		scan.CompletedAt = &t
	}
	if errors.Valid {
		scan.Errors = &errors.String
	}
	if currentPhase.Valid {
		scan.CurrentPhase = &currentPhase.String
	}
	if lastProcessedPath.Valid {
		scan.LastProcessedPath = &lastProcessedPath.String
	}
	if resumeFromScanID.Valid {
		scan.ResumeFromScanID = &resumeFromScanID.Int64
	}

	return scan, nil
}

// GetLastCompletedScanFileCount returns the file count from the most recent completed scan
// Returns 0 if no completed scans exist (first scan ever)
// Used to estimate total files for subsequent scans
func (db *DB) GetLastCompletedScanFileCount() (int64, error) {
	var count int64
	err := db.conn.QueryRow(`
		SELECT files_scanned
		FROM scans
		WHERE status = 'completed' AND files_scanned > 0
		ORDER BY started_at DESC
		LIMIT 1
	`).Scan(&count)

	if err == sql.ErrNoRows {
		return 0, nil // First scan ever - no previous data
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get last completed scan file count: %w", err)
	}

	return count, nil
}

// GetCurrentFileCount returns the number of files currently stored in the database
func (db *DB) GetCurrentFileCount() (int64, error) {
	var count int64
	err := db.conn.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get current file count: %w", err)
	}
	return count, nil
}

// ListScans retrieves recent scans with pagination
func (db *DB) ListScans(limit, offset int) ([]*Scan, int, error) {
	// Count total
	var total int
	err := db.conn.QueryRow(`SELECT COUNT(*) FROM scans`).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	// Get scans
	query := `
		SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, deleted_files_count, created_at
		FROM scans
		ORDER BY started_at DESC
		LIMIT ? OFFSET ?
	`

	rows, err := db.conn.Query(query, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var scans []*Scan
	for rows.Next() {
		scan := &Scan{}
		var startedAt, createdAt int64
		var completedAt sql.NullInt64
		var errors sql.NullString
		var currentPhase sql.NullString
		var lastProcessedPath sql.NullString
		var resumeFromScanID sql.NullInt64

		err := rows.Scan(
			&scan.ID,
			&startedAt,
			&completedAt,
			&scan.Status,
			&scan.FilesScanned,
			&errors,
			&scan.ScanType,
			&currentPhase,
			&lastProcessedPath,
			&resumeFromScanID,
			&scan.DeletedFilesCount,
			&createdAt,
		)
		if err != nil {
			return nil, 0, err
		}

		scan.StartedAt = time.Unix(startedAt, 0)
		scan.CreatedAt = time.Unix(createdAt, 0)

		if completedAt.Valid {
			t := time.Unix(completedAt.Int64, 0)
			scan.CompletedAt = &t
		}
		if errors.Valid {
			scan.Errors = &errors.String
		}
		if currentPhase.Valid {
			scan.CurrentPhase = &currentPhase.String
		}
		if lastProcessedPath.Valid {
			scan.LastProcessedPath = &lastProcessedPath.String
		}
		if resumeFromScanID.Valid {
			scan.ResumeFromScanID = &resumeFromScanID.Int64
		}

		scans = append(scans, scan)
	}

	return scans, total, rows.Err()
}

// GetScanFileCount returns the count of files associated with a specific scan
func (db *DB) GetScanFileCount(scanID int64) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM files WHERE scan_id = ?`
	err := db.conn.QueryRow(query, scanID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// CreateScanLog creates a new scan log entry
func (db *DB) CreateScanLog(scanID int64, level, phase, message string) error {
	query := `
		INSERT INTO scan_logs (scan_id, timestamp, level, phase, message)
		VALUES (?, ?, ?, ?, ?)
	`

	timestamp := time.Now().Unix()
	var phasePtr *string
	if phase != "" {
		phasePtr = &phase
	}

	_, err := db.conn.Exec(query, scanID, timestamp, level, phasePtr, message)
	return err
}

// GetScanLogs retrieves scan logs with filtering and pagination
func (db *DB) GetScanLogs(filters LogFilters) ([]*ScanLog, error) {
	query := `
		SELECT id, scan_id, timestamp, level, phase, message, created_at
		FROM scan_logs
		WHERE 1=1
	`
	args := []interface{}{}

	// Apply filters
	if filters.ScanID != nil {
		query += " AND scan_id = ?"
		args = append(args, *filters.ScanID)
	}

	if filters.Level != "" && filters.Level != "all" {
		query += " AND level = ?"
		args = append(args, filters.Level)
	}

	if filters.Phase != "" && filters.Phase != "all" {
		query += " AND phase = ?"
		args = append(args, filters.Phase)
	}

	if filters.SearchText != "" {
		query += " AND message LIKE ?"
		args = append(args, "%"+filters.SearchText+"%")
	}

	if filters.StartTime != nil {
		query += " AND timestamp >= ?"
		args = append(args, filters.StartTime.Unix())
	}

	if filters.EndTime != nil {
		query += " AND timestamp <= ?"
		args = append(args, filters.EndTime.Unix())
	}

	// Order by timestamp descending (newest first)
	query += " ORDER BY timestamp DESC"

	// Apply pagination
	if filters.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filters.Limit)
	}

	if filters.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, filters.Offset)
	}

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		log.Printf("ERROR: GetScanLogs query failed: %v", err)
		return nil, err
	}
	defer rows.Close()

	var logs []*ScanLog
	for rows.Next() {
		var log ScanLog
		var timestampUnix int64
		var createdAtUnix int64

		err := rows.Scan(
			&log.ID,
			&log.ScanID,
			&timestampUnix,
			&log.Level,
			&log.Phase,
			&log.Message,
			&createdAtUnix,
		)
		if err != nil {
			return nil, err
		}

		log.Timestamp = time.Unix(timestampUnix, 0)
		log.CreatedAt = time.Unix(createdAtUnix, 0)
		logs = append(logs, &log)
	}

	return logs, rows.Err()
}

// GetScanLogCount returns the total count of logs matching the filters
func (db *DB) GetScanLogCount(filters LogFilters) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM scan_logs
		WHERE 1=1
	`
	args := []interface{}{}

	// Apply same filters as GetScanLogs (without pagination)
	if filters.ScanID != nil {
		query += " AND scan_id = ?"
		args = append(args, *filters.ScanID)
	}

	if filters.Level != "" && filters.Level != "all" {
		query += " AND level = ?"
		args = append(args, filters.Level)
	}

	if filters.Phase != "" && filters.Phase != "all" {
		query += " AND phase = ?"
		args = append(args, filters.Phase)
	}

	if filters.SearchText != "" {
		query += " AND message LIKE ?"
		args = append(args, "%"+filters.SearchText+"%")
	}

	if filters.StartTime != nil {
		query += " AND timestamp >= ?"
		args = append(args, filters.StartTime.Unix())
	}

	if filters.EndTime != nil {
		query += " AND timestamp <= ?"
		args = append(args, filters.EndTime.Unix())
	}

	var count int
	err := db.conn.QueryRow(query, args...).Scan(&count)
	return count, err
}

// DeleteOldScanLogs deletes scan logs older than the specified number of days
// Returns the number of logs deleted. If retentionDays is 0, no logs are deleted.
// If retentionDays is -1, logging is disabled and all logs are deleted.
func (db *DB) DeleteOldScanLogs(retentionDays int) (int64, error) {
	if retentionDays == 0 {
		// Keep logs indefinitely
		return 0, nil
	}

	var query string
	var args []interface{}

	if retentionDays == -1 {
		// Delete all logs (logging disabled)
		query = "DELETE FROM scan_logs"
	} else {
		// Delete logs older than retention period
		cutoffTime := time.Now().AddDate(0, 0, -retentionDays).Unix()
		query = "DELETE FROM scan_logs WHERE created_at < ?"
		args = append(args, cutoffTime)
	}

	result, err := db.conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// UpsertUsage inserts or updates a usage record
func (db *DB) ClearScans() (int64, error) {
	// First, clear scan_id references in files table to avoid foreign key constraint
	_, err := db.conn.Exec(`
		UPDATE files
		SET scan_id = NULL
		WHERE scan_id IN (SELECT id FROM scans WHERE status != 'running')
	`)
	if err != nil {
		return 0, fmt.Errorf("failed to clear scan references: %w", err)
	}

	// Now delete the scans
	result, err := db.conn.Exec(`DELETE FROM scans WHERE status != 'running'`)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	// Log the action
	_, _ = db.conn.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('delete', 'scans', 0, ?)`,
		fmt.Sprintf("Cleared scan history (%d records)", count),
	)

	return count, nil
}

// ClearAllUsage deletes all service usage records
func (db *DB) CleanStaleScansOnStartup() (int64, error) {
	errMsg := "Scan interrupted - application restarted"

	result, err := db.conn.Exec(
		`UPDATE scans SET status = 'interrupted', errors = ?, completed_at = ? WHERE status = 'running'`,
		errMsg, time.Now().Unix(),
	)
	if err != nil {
		return 0, err
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if count > 0 {
		log.Printf("Marked %d running scans as interrupted on startup", count)
		// Log the action
		_, _ = db.conn.Exec(
			`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('config_change', 'scans', 0, ?)`,
			fmt.Sprintf("Marked %d running scans as interrupted on startup", count),
		)
	}

	return count, nil
}

// CleanStaleScans marks old running scans as interrupted
// This is a safety mechanism for scans that have been running for over 1 hour
func (db *DB) CleanStaleScans() (int64, error) {
	oneHourAgo := time.Now().Add(-1 * time.Hour).Unix()
	errMsg := "Scan interrupted - exceeded maximum runtime"

	result, err := db.conn.Exec(
		`UPDATE scans SET status = 'interrupted', errors = ?, completed_at = ? WHERE status = 'running' AND started_at < ?`,
		errMsg, time.Now().Unix(), oneHourAgo,
	)
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
			`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('config_change', 'scans', 0, ?)`,
			fmt.Sprintf("Cleaned %d stale running scans", count),
		)
	}

	return count, nil
}
func (db *DB) DeleteUnverifiedFiles(ctx context.Context, scanID int64) (int64, error) {
	tx, err := db.BeginTx()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// Find all files that weren't updated in this scan
	var fileCount int64
	err = tx.QueryRow(`SELECT COUNT(*) FROM files WHERE scan_id != ? OR scan_id IS NULL`, scanID).Scan(&fileCount)
	if err != nil {
		return 0, fmt.Errorf("failed to count unverified files: %w", err)
	}

	// Log the cleanup action to audit log
	if fileCount > 0 {
		_, err = tx.Exec(
			`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('cleanup', 'scan', ?, ?)`,
			scanID, fmt.Sprintf("Removed %d files that no longer exist on disk", fileCount),
		)
		if err != nil {
			return 0, fmt.Errorf("failed to log cleanup: %w", err)
		}
	}

	// Delete files not updated in this scan (usage records will be cascade deleted)
	result, err := tx.Exec(`DELETE FROM files WHERE scan_id != ? OR scan_id IS NULL`, scanID)
	if err != nil {
		return 0, fmt.Errorf("failed to delete unverified files: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, nil
}

// GetAllFilePaths returns all file paths from the database
// This is used by the manual cleanup scan to check which files still exist
func (db *DB) ClearMissingFiles(ctx context.Context, scanID int64) error {
	_, err := db.conn.ExecContext(ctx, `DELETE FROM service_missing_files WHERE scan_id = ?`, scanID)
	return err
}

// InsertMissingFile inserts a missing file record into the database
func (db *DB) InsertMissingFile(ctx context.Context, missing *MissingFile) error {
	// Serialize metadata to JSON
	var metadataJSON []byte
	var err error
	if missing.Metadata != nil {
		metadataJSON, err = json.Marshal(missing.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}
	}

	_, err = db.conn.ExecContext(ctx, `
		INSERT INTO service_missing_files (
			scan_id, service, service_path, translated_path,
			size, service_group, service_group_id, metadata
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, missing.ScanID, missing.Service, missing.ServicePath, missing.TranslatedPath,
		missing.Size, missing.ServiceGroup, missing.ServiceGroupID, metadataJSON)

	return err
}

// GetMissingFilesByScan retrieves all missing files for a specific scan
func (db *DB) GetMissingFilesByScan(ctx context.Context, scanID int64) ([]*MissingFile, error) {
	rows, err := db.conn.QueryContext(ctx, `
		SELECT id, scan_id, service, service_path, translated_path,
		       size, service_group, service_group_id, metadata, created_at
		FROM service_missing_files
		WHERE scan_id = ?
		ORDER BY service, size DESC
	`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var missingFiles []*MissingFile
	for rows.Next() {
		var missing MissingFile
		var createdAt int64
		var metadataJSON sql.NullString

		err := rows.Scan(
			&missing.ID, &missing.ScanID, &missing.Service, &missing.ServicePath,
			&missing.TranslatedPath, &missing.Size, &missing.ServiceGroup,
			&missing.ServiceGroupID, &metadataJSON, &createdAt,
		)
		if err != nil {
			return nil, err
		}

		missing.CreatedAt = time.Unix(createdAt, 0)

		// Deserialize metadata
		if metadataJSON.Valid && metadataJSON.String != "" {
			if err := json.Unmarshal([]byte(metadataJSON.String), &missing.Metadata); err != nil {
				log.Printf("Warning: failed to unmarshal metadata for missing file %d: %v", missing.ID, err)
			}
		}

		missingFiles = append(missingFiles, &missing)
	}

	return missingFiles, rows.Err()
}

// GetLatestMissingFiles retrieves missing files from the most recent scan
func (db *DB) GetLatestMissingFiles(ctx context.Context) ([]*MissingFile, error) {
	// First, get the most recent scan ID
	var latestScanID int64
	err := db.conn.QueryRowContext(ctx, `
		SELECT id FROM scans
		ORDER BY started_at DESC
		LIMIT 1
	`).Scan(&latestScanID)
	if err != nil {
		if err == sql.ErrNoRows {
			return []*MissingFile{}, nil
		}
		return nil, err
	}

	return db.GetMissingFilesByScan(ctx, latestScanID)
}
