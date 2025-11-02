package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
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
	CreatedAt    time.Time
}

// scanFileRow scans a single file row from a query result
func scanFileRow(scanner interface {
	Scan(dest ...interface{}) error
}) (*File, error) {
	file := &File{}
	var modTime, lastVerified, createdAt int64

	err := scanner.Scan(
		&file.ID,
		&file.Path,
		&file.Size,
		&file.Inode,
		&file.DeviceID,
		&modTime,
		&file.ScanID,
		&lastVerified,
		&file.IsOrphaned,
		&createdAt,
	)
	if err != nil {
		return nil, err
	}

	file.ModifiedTime = time.Unix(modTime, 0)
	file.LastVerified = time.Unix(lastVerified, 0)
	file.CreatedAt = time.Unix(createdAt, 0)

	return file, nil
}

// Usage represents a service using a file
type Usage struct {
	ID            int64
	FileID        int64
	Service       string
	ReferencePath string
	Metadata      map[string]interface{}
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Scan represents a scan operation
type Scan struct {
	ID           int64
	StartedAt    time.Time
	CompletedAt  *time.Time
	Status       string
	FilesScanned int64
	Errors       *string
	ScanType     string
	CreatedAt    time.Time
}

// UpsertFile inserts or updates a file record
func (db *DB) UpsertFile(file *File) error {
	query := `
		INSERT INTO files (path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
			size = excluded.size,
			inode = excluded.inode,
			device_id = excluded.device_id,
			modified_time = excluded.modified_time,
			scan_id = excluded.scan_id,
			last_verified = excluded.last_verified,
			is_orphaned = excluded.is_orphaned
		RETURNING id
	`

	err := db.conn.QueryRow(
		query,
		file.Path,
		file.Size,
		file.Inode,
		file.DeviceID,
		file.ModifiedTime.Unix(),
		file.ScanID,
		file.LastVerified.Unix(),
		file.IsOrphaned,
	).Scan(&file.ID)

	if err != nil {
		return fmt.Errorf("failed to upsert file %s (scan_id=%d): %w", file.Path, file.ScanID, err)
	}

	return nil
}

// GetFileByID retrieves a file by its ID
func (db *DB) GetFileByID(id int64) (*File, error) {
	query := `
		SELECT id, path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, created_at
		FROM files
		WHERE id = ?
	`

	return scanFileRow(db.conn.QueryRow(query, id))
}

// GetFileByPath retrieves a file by its path
func (db *DB) GetFileByPath(path string) (*File, error) {
	query := `
		SELECT id, path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, created_at
		FROM files
		WHERE path = ?
	`

	return scanFileRow(db.conn.QueryRow(query, path))
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
func (db *DB) GetFilesByPaths(paths []string) (map[string]*File, error) {
	if len(paths) == 0 {
		return make(map[string]*File), nil
	}

	// Build IN clause with placeholders
	args := make([]interface{}, len(paths))
	for i, path := range paths {
		args[i] = path
	}

	query := fmt.Sprintf(`
		SELECT id, path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, created_at
		FROM files
		WHERE path IN (%s)
	`, buildInClause(len(paths)))

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fileMap := make(map[string]*File)
	for rows.Next() {
		file, err := scanFileRow(rows)
		if err != nil {
			return nil, err
		}
		fileMap[file.Path] = file
	}

	return fileMap, rows.Err()
}

// CreateScan creates a new scan record
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

// GetCurrentScan returns the currently running scan, if any
func (db *DB) GetCurrentScan() (*Scan, error) {
	query := `
		SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, created_at
		FROM scans
		WHERE status = 'running'
		ORDER BY started_at DESC
		LIMIT 1
	`

	scan := &Scan{}
	var startedAt, createdAt int64
	var completedAt sql.NullInt64
	var errors sql.NullString

	err := db.conn.QueryRow(query).Scan(
		&scan.ID,
		&startedAt,
		&completedAt,
		&scan.Status,
		&scan.FilesScanned,
		&errors,
		&scan.ScanType,
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

	// Check if scan is stale (running for more than 1 hour)
	// This handles cases where the app crashed before updating scan status
	if time.Since(scan.StartedAt) > 1*time.Hour {
		log.Printf("Found stale running scan (ID: %d, started: %v), marking as interrupted", scan.ID, scan.StartedAt)
		errMsg := "Scan interrupted - exceeded maximum runtime of 1 hour"
		if err := db.UpdateScan(scan.ID, "interrupted", scan.FilesScanned, &errMsg); err != nil {
			log.Printf("Failed to update stale scan status: %v", err)
		}
		return nil, nil // Return no current scan
	}

	return scan, nil
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
		SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, created_at
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

		err := rows.Scan(
			&scan.ID,
			&startedAt,
			&completedAt,
			&scan.Status,
			&scan.FilesScanned,
			&errors,
			&scan.ScanType,
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

		scans = append(scans, scan)
	}

	return scans, total, rows.Err()
}

// UpsertUsage inserts or updates a usage record
func (db *DB) UpsertUsage(usage *Usage) error {
	metadataJSON, err := json.Marshal(usage.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := `
		INSERT INTO usage (file_id, service, reference_path, metadata)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(file_id, service) DO UPDATE SET
			reference_path = excluded.reference_path,
			metadata = excluded.metadata,
			updated_at = strftime('%s', 'now')
		RETURNING id
	`

	err = db.conn.QueryRow(
		query,
		usage.FileID,
		usage.Service,
		usage.ReferencePath,
		string(metadataJSON),
	).Scan(&usage.ID)

	return err
}

// BatchUpsertUsage inserts or updates multiple usage records in a single transaction
func (db *DB) BatchUpsertUsage(usages []*Usage) error {
	if len(usages) == 0 {
		return nil
	}

	tx, err := db.BeginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO usage (file_id, service, reference_path, metadata)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(file_id, service) DO UPDATE SET
			reference_path = excluded.reference_path,
			metadata = excluded.metadata,
			updated_at = strftime('%s', 'now')
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, usage := range usages {
		metadataJSON, err := json.Marshal(usage.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}

		_, err = stmt.Exec(
			usage.FileID,
			usage.Service,
			usage.ReferencePath,
			string(metadataJSON),
		)
		if err != nil {
			return fmt.Errorf("failed to insert usage: %w", err)
		}
	}

	return tx.Commit()
}

// DeleteUsageByService deletes all usage records for a service
func (db *DB) DeleteUsageByService(service string) error {
	query := `DELETE FROM usage WHERE service = ?`
	_, err := db.conn.Exec(query, service)
	return err
}

// GetUsageByFileID retrieves all usage records for a file
func (db *DB) GetUsageByFileID(fileID int64) ([]*Usage, error) {
	query := `
		SELECT id, file_id, service, reference_path, metadata, created_at, updated_at
		FROM usage
		WHERE file_id = ?
	`

	rows, err := db.conn.Query(query, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var usages []*Usage
	for rows.Next() {
		usage := &Usage{}
		var metadataJSON string
		var createdAt, updatedAt int64

		err := rows.Scan(
			&usage.ID,
			&usage.FileID,
			&usage.Service,
			&usage.ReferencePath,
			&metadataJSON,
			&createdAt,
			&updatedAt,
		)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal([]byte(metadataJSON), &usage.Metadata); err != nil {
			usage.Metadata = make(map[string]interface{})
		}

		usage.CreatedAt = time.Unix(createdAt, 0)
		usage.UpdatedAt = time.Unix(updatedAt, 0)

		usages = append(usages, usage)
	}

	return usages, rows.Err()
}

// GetUsageByFileIDs retrieves all usage records for multiple files in one query (fixes N+1)
func (db *DB) GetUsageByFileIDs(fileIDs []int64) (map[int64][]*Usage, error) {
	if len(fileIDs) == 0 {
		return make(map[int64][]*Usage), nil
	}

	// Build IN clause with placeholders
	args := make([]interface{}, len(fileIDs))
	for i, id := range fileIDs {
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, file_id, service, reference_path, metadata, created_at, updated_at
		FROM usage
		WHERE file_id IN (%s)
		ORDER BY file_id, service
	`, buildInClause(len(fileIDs)))

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	usageMap := make(map[int64][]*Usage)
	for rows.Next() {
		usage := &Usage{}
		var metadataJSON string
		var createdAt, updatedAt int64

		err := rows.Scan(
			&usage.ID,
			&usage.FileID,
			&usage.Service,
			&usage.ReferencePath,
			&metadataJSON,
			&createdAt,
			&updatedAt,
		)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal([]byte(metadataJSON), &usage.Metadata); err != nil {
			usage.Metadata = make(map[string]interface{})
		}

		usage.CreatedAt = time.Unix(createdAt, 0)
		usage.UpdatedAt = time.Unix(updatedAt, 0)

		usageMap[usage.FileID] = append(usageMap[usage.FileID], usage)
	}

	return usageMap, rows.Err()
}

// UpdateOrphanedStatus updates the orphaned status of all files
func (db *DB) UpdateOrphanedStatus() error {
	query := `
		UPDATE files
		SET is_orphaned = CASE
			WHEN NOT EXISTS (SELECT 1 FROM usage WHERE usage.file_id = files.id)
			THEN 1
			ELSE 0
		END
	`
	_, err := db.conn.Exec(query)
	return err
}

// SearchFiles searches for files by path using FTS
func (db *DB) SearchFiles(searchQuery string, limit, offset int) ([]*File, int, error) {
	// Count total results
	countQuery := `
		SELECT COUNT(*)
		FROM files
		WHERE id IN (SELECT rowid FROM files_fts WHERE files_fts MATCH ?)
	`

	var total int
	err := db.conn.QueryRow(countQuery, searchQuery).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	// Get paginated results
	query := `
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
		       f.scan_id, f.last_verified, f.is_orphaned, f.created_at
		FROM files f
		WHERE f.id IN (SELECT rowid FROM files_fts WHERE files_fts MATCH ?)
		ORDER BY f.path
		LIMIT ? OFFSET ?
	`

	rows, err := db.conn.Query(query, searchQuery, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	files := []*File{}
	for rows.Next() {
		file, err := scanFileRow(rows)
		if err != nil {
			return nil, 0, err
		}
		files = append(files, file)
	}

	return files, total, rows.Err()
}

// ValidateOrderBy validates and returns a safe ORDER BY column name
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

// ListFiles retrieves files with filtering and pagination
func (db *DB) ListFiles(orphanedOnly bool, service string, hardlinksOnly bool, limit, offset int, orderBy string) ([]*File, int, error) {
	var conditions []string
	args := []interface{}{}

	if orphanedOnly {
		conditions = append(conditions, "f.is_orphaned = 1")
	}

	if service != "" {
		conditions = append(conditions, "EXISTS (SELECT 1 FROM usage u WHERE u.file_id = f.id AND u.service = ?)")
		args = append(args, service)
	}

	if hardlinksOnly {
		conditions = append(conditions, `(f.device_id, f.inode) IN (
			SELECT device_id, inode FROM files GROUP BY device_id, inode HAVING COUNT(*) > 1
		)`)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Count total
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM files f %s", whereClause)
	var total int
	err := db.conn.QueryRow(countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	// Validate and sanitize orderBy
	// SQL Injection Safety: ValidateOrderBy uses an allowlist to ensure only
	// valid column names are used. This is safe from SQL injection because the
	// value is validated against a fixed set of allowed column names.
	safeOrderBy := ValidateOrderBy(orderBy)

	query := fmt.Sprintf(`
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
		       f.scan_id, f.last_verified, f.is_orphaned, f.created_at
		FROM files f
		%s
		ORDER BY f.%s
		LIMIT ? OFFSET ?
	`, whereClause, safeOrderBy)

	args = append(args, limit, offset)
	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	files := []*File{}
	for rows.Next() {
		file, err := scanFileRow(rows)
		if err != nil {
			return nil, 0, err
		}
		files = append(files, file)
	}

	return files, total, rows.Err()
}

// GetHardlinkGroups returns groups of hardlinked files
func (db *DB) GetHardlinkGroups() (map[string][]*File, error) {
	query := `
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
		       f.scan_id, f.last_verified, f.is_orphaned, f.created_at
		FROM files f
		WHERE (f.device_id, f.inode) IN (
			SELECT device_id, inode
			FROM files
			GROUP BY device_id, inode
			HAVING COUNT(*) > 1
		)
		ORDER BY f.device_id, f.inode, f.path
	`

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	groups := make(map[string][]*File)

	for rows.Next() {
		file, err := scanFileRow(rows)
		if err != nil {
			return nil, err
		}

		key := fmt.Sprintf("%d-%d", file.DeviceID, file.Inode)
		groups[key] = append(groups[key], file)
	}

	return groups, rows.Err()
}

// GetHardlinksByInodeDevice returns all hardlinked files for a specific inode and device
func (db *DB) GetHardlinksByInodeDevice(inode, deviceID int64) ([]*File, error) {
	query := `
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
		       f.scan_id, f.last_verified, f.is_orphaned, f.created_at
		FROM files f
		WHERE f.device_id = ? AND f.inode = ?
		ORDER BY f.path
	`

	rows, err := db.conn.Query(query, deviceID, inode)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []*File
	for rows.Next() {
		file, err := scanFileRow(rows)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}

	return files, rows.Err()
}

// DeleteFile deletes a file and logs the action
func (db *DB) DeleteFile(fileID int64, details string) error {
	tx, err := db.BeginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Log the deletion
	_, err = tx.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('delete', 'file', ?, ?)`,
		fileID, details,
	)
	if err != nil {
		return err
	}

	// Delete the file (usage records will be cascade deleted)
	_, err = tx.Exec(`DELETE FROM files WHERE id = ?`, fileID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// MarkFilesForRescan marks files matching a predefined filter for rescan
// Only accepts predefined safe filter types to prevent SQL injection
func (db *DB) MarkFilesForRescan(filterType string) (int64, error) {
	// Allowlist of safe filters - no user input in SQL
	var whereClause string
	var filterDesc string

	switch filterType {
	case "orphaned":
		whereClause = "is_orphaned = 1"
		filterDesc = "orphaned files"
	case "all":
		whereClause = "1=1"
		filterDesc = "all files"
	default:
		return 0, fmt.Errorf("invalid filter type: %s (allowed: orphaned, all)", filterType)
	}

	tx, err := db.BeginTx()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`UPDATE files SET last_verified = 0 WHERE %s`, whereClause)
	result, err := tx.Exec(query)
	if err != nil {
		return 0, fmt.Errorf("failed to mark files for rescan: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	// Log the action
	_, err = tx.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('mark_rescan', 'files', 0, ?)`,
		fmt.Sprintf("Marked %d files for rescan (%s)", count, filterDesc),
	)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return count, nil
}

// MarkFileForRescan marks a single file for rescan by ID (safe from SQL injection)
func (db *DB) MarkFileForRescan(fileID int64) error {
	query := `UPDATE files SET last_verified = 0 WHERE id = ?`
	_, err := db.conn.Exec(query, fileID)
	if err != nil {
		return err
	}

	// Log the action
	_, err = db.conn.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('mark_rescan', 'file', ?, 'Marked for rescan')`,
		fileID,
	)
	return err
}

// DeleteFileByPath deletes a file by its path
func (db *DB) DeleteFileByPath(path string, details string) error {
	file, err := db.GetFileByPath(path)
	if err != nil {
		return err
	}
	return db.DeleteFile(file.ID, details)
}

// SetConfig sets a configuration value
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
func (db *DB) ClearAllFiles() (int64, error) {
	result, err := db.conn.Exec(`DELETE FROM files`)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	// Log the action
	_, _ = db.conn.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('delete', 'files', 0, ?)`,
		fmt.Sprintf("Cleared all files (%d records)", count),
	)

	return count, nil
}

// ClearOrphanedFiles deletes only orphaned file records
func (db *DB) ClearOrphanedFiles() (int64, error) {
	result, err := db.conn.Exec(`DELETE FROM files WHERE is_orphaned = 1`)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	// Log the action
	_, _ = db.conn.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('delete', 'files', 0, ?)`,
		fmt.Sprintf("Cleared orphaned files (%d records)", count),
	)

	return count, nil
}

// ClearScans deletes completed scan history (preserves running scans)
func (db *DB) ClearScans() (int64, error) {
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
func (db *DB) ClearAllUsage() (int64, error) {
	result, err := db.conn.Exec(`DELETE FROM usage`)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	// Log the action
	_, _ = db.conn.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('delete', 'usage', 0, ?)`,
		fmt.Sprintf("Cleared all usage records (%d records)", count),
	)

	return count, nil
}

// VacuumDatabase performs VACUUM and ANALYZE operations
func (db *DB) VacuumDatabase() error {
	// VACUUM must be run outside a transaction
	if _, err := db.conn.Exec(`VACUUM`); err != nil {
		return fmt.Errorf("failed to vacuum database: %w", err)
	}

	if _, err := db.conn.Exec(`ANALYZE`); err != nil {
		return fmt.Errorf("failed to analyze database: %w", err)
	}

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

// CleanStaleScans marks old running scans as interrupted
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
func (db *DB) GetDatabaseStats() (*DatabaseStats, error) {
	stats := &DatabaseStats{}

	// File counts
	if err := db.conn.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&stats.FileCount); err != nil {
		return nil, err
	}
	if err := db.conn.QueryRow(`SELECT COUNT(*) FROM files WHERE is_orphaned = 1`).Scan(&stats.OrphanedCount); err != nil {
		return nil, err
	}

	// Usage count
	if err := db.conn.QueryRow(`SELECT COUNT(*) FROM usage`).Scan(&stats.UsageCount); err != nil {
		return nil, err
	}

	// Scan count
	if err := db.conn.QueryRow(`SELECT COUNT(*) FROM scans`).Scan(&stats.ScanCount); err != nil {
		return nil, err
	}

	// Audit log count
	if err := db.conn.QueryRow(`SELECT COUNT(*) FROM audit_log`).Scan(&stats.AuditLogCount); err != nil {
		return nil, err
	}

	// Hardlink groups
	if err := db.conn.QueryRow(`
		SELECT COUNT(DISTINCT device_id || '-' || inode)
		FROM files
		WHERE (device_id, inode) IN (
			SELECT device_id, inode
			FROM files
			GROUP BY device_id, inode
			HAVING COUNT(*) > 1
		)
	`).Scan(&stats.HardlinkGroups); err != nil {
		return nil, err
	}

	// Total size
	if err := db.conn.QueryRow(`SELECT COALESCE(SUM(size), 0) FROM files`).Scan(&stats.TotalSize); err != nil {
		return nil, err
	}

	// Orphaned size
	if err := db.conn.QueryRow(`SELECT COALESCE(SUM(size), 0) FROM files WHERE is_orphaned = 1`).Scan(&stats.OrphanedSize); err != nil {
		return nil, err
	}

	// Hardlink savings
	var totalSizeWithDupes int64
	err := db.conn.QueryRow(`
		SELECT COALESCE(SUM(size * (cnt - 1)), 0)
		FROM (
			SELECT size, COUNT(*) as cnt
			FROM files
			GROUP BY device_id, inode
			HAVING COUNT(*) > 1
		)
	`).Scan(&totalSizeWithDupes)
	if err != nil {
		return nil, err
	}
	stats.HardlinkSavings = totalSizeWithDupes

	// Database size (page_count * page_size / 1024 for KB)
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

// AuditLogEntry represents a single audit log entry
type AuditLogEntry struct {
	ID         int64
	Action     string
	EntityType string
	EntityID   *int64
	Details    string
	CreatedAt  time.Time
}

// GetAuditLog retrieves paginated audit log entries
func (db *DB) GetAuditLog(limit, offset int) ([]*AuditLogEntry, int, error) {
	// Count total
	var total int
	err := db.conn.QueryRow(`SELECT COUNT(*) FROM audit_log`).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	// Get entries
	query := `
		SELECT id, action, entity_type, entity_id, details, created_at
		FROM audit_log
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`

	rows, err := db.conn.Query(query, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var entries []*AuditLogEntry
	for rows.Next() {
		entry := &AuditLogEntry{}
		var createdAt int64
		var entityID sql.NullInt64

		err := rows.Scan(
			&entry.ID,
			&entry.Action,
			&entry.EntityType,
			&entityID,
			&entry.Details,
			&createdAt,
		)
		if err != nil {
			return nil, 0, err
		}

		if entityID.Valid {
			entry.EntityID = &entityID.Int64
		}
		entry.CreatedAt = time.Unix(createdAt, 0)

		entries = append(entries, entry)
	}

	return entries, total, rows.Err()
}

// ClearConfig deletes all configuration values
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
