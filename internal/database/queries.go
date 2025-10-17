package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
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

	return err
}

// GetFileByPath retrieves a file by its path
func (db *DB) GetFileByPath(path string) (*File, error) {
	query := `
		SELECT id, path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, created_at
		FROM files
		WHERE path = ?
	`

	file := &File{}
	var modTime, lastVerified, createdAt int64

	err := db.conn.QueryRow(query, path).Scan(
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

	return scan, nil
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
		file := &File{}
		var modTime, lastVerified, createdAt int64

		err := rows.Scan(
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
			return nil, 0, err
		}

		file.ModifiedTime = time.Unix(modTime, 0)
		file.LastVerified = time.Unix(lastVerified, 0)
		file.CreatedAt = time.Unix(createdAt, 0)

		files = append(files, file)
	}

	return files, total, rows.Err()
}

// ListFiles retrieves files with filtering and pagination
func (db *DB) ListFiles(orphanedOnly bool, service string, limit, offset int, orderBy string) ([]*File, int, error) {
	whereClause := "WHERE 1=1"
	args := []interface{}{}

	if orphanedOnly {
		whereClause += " AND f.is_orphaned = 1"
	}

	if service != "" {
		whereClause += " AND EXISTS (SELECT 1 FROM usage u WHERE u.file_id = f.id AND u.service = ?)"
		args = append(args, service)
	}

	// Count total
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM files f %s", whereClause)
	var total int
	err := db.conn.QueryRow(countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	// Get results
	if orderBy == "" {
		orderBy = "path"
	}

	query := fmt.Sprintf(`
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
		       f.scan_id, f.last_verified, f.is_orphaned, f.created_at
		FROM files f
		%s
		ORDER BY f.%s
		LIMIT ? OFFSET ?
	`, whereClause, orderBy)

	args = append(args, limit, offset)
	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	files := []*File{}
	for rows.Next() {
		file := &File{}
		var modTime, lastVerified, createdAt int64

		err := rows.Scan(
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
			return nil, 0, err
		}

		file.ModifiedTime = time.Unix(modTime, 0)
		file.LastVerified = time.Unix(lastVerified, 0)
		file.CreatedAt = time.Unix(createdAt, 0)

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
		file := &File{}
		var modTime, lastVerified, createdAt int64

		err := rows.Scan(
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

		key := fmt.Sprintf("%d-%d", file.DeviceID, file.Inode)
		groups[key] = append(groups[key], file)
	}

	return groups, rows.Err()
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

// MarkFilesForRescan marks files matching a filter for rescan by updating their last_verified to 0
func (db *DB) MarkFilesForRescan(filter string) (int64, error) {
	tx, err := db.BeginTx()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// Update last_verified to epoch 0 for matched files
	query := fmt.Sprintf(`UPDATE files SET last_verified = 0 WHERE %s`, filter)
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
		fmt.Sprintf("Marked %d files for rescan with filter: %s", count, filter),
	)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return count, nil
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
