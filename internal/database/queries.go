package database

import (
	"context"
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
	Extension    string
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
		&file.Extension,
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

// UpsertFile inserts or updates a file record
func (db *DB) UpsertFile(file *File) error {
	query := `
		INSERT INTO files (path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, extension)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
			size = excluded.size,
			inode = excluded.inode,
			device_id = excluded.device_id,
			modified_time = excluded.modified_time,
			scan_id = excluded.scan_id,
			last_verified = excluded.last_verified,
			is_orphaned = excluded.is_orphaned,
			extension = excluded.extension
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
		file.Extension,
	).Scan(&file.ID)

	if err != nil {
		return fmt.Errorf("failed to upsert file %s (scan_id=%d): %w", file.Path, file.ScanID, err)
	}

	return nil
}

// BatchUpsertFiles inserts or updates multiple file records in a single transaction
// This is significantly faster than individual UpsertFile calls for large batches
func (db *DB) BatchUpsertFiles(ctx context.Context, files []*File) error {
	if len(files) == 0 {
		return nil
	}

	// SQLite has a parameter limit (default 999), with 9 params per file
	// we batch at most 100 files at a time to stay well under the limit
	const maxBatchSize = 100

	for i := 0; i < len(files); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(files) {
			end = len(files)
		}
		batch := files[i:end]

		if err := db.batchUpsertFilesChunk(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

// batchUpsertFilesChunk upserts a single chunk of files (≤100)
func (db *DB) batchUpsertFilesChunk(ctx context.Context, files []*File) error {
	// Check for context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Start transaction
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare the statement
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO files (path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, extension)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
			size = excluded.size,
			inode = excluded.inode,
			device_id = excluded.device_id,
			modified_time = excluded.modified_time,
			scan_id = excluded.scan_id,
			last_verified = excluded.last_verified,
			is_orphaned = excluded.is_orphaned,
			extension = excluded.extension
		RETURNING id
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Execute for each file
	for _, file := range files {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := stmt.QueryRowContext(
			ctx,
			file.Path,
			file.Size,
			file.Inode,
			file.DeviceID,
			file.ModifiedTime.Unix(),
			file.ScanID,
			file.LastVerified.Unix(),
			file.IsOrphaned,
			file.Extension,
		).Scan(&file.ID)

		if err != nil {
			return fmt.Errorf("failed to upsert file %s: %w", file.Path, err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetFileByID retrieves a file by its ID
func (db *DB) GetFileByID(id int64) (*File, error) {
	query := `
		SELECT id, path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, extension, created_at
		FROM files
		WHERE id = ?
	`

	return scanFileRow(db.conn.QueryRow(query, id))
}

// GetFileByPath retrieves a file by its path
func (db *DB) GetFileByPath(path string) (*File, error) {
	query := `
		SELECT id, path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, extension, created_at
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
// Handles SQLite variable limit by batching queries into chunks of 900 parameters
func (db *DB) GetFilesByPaths(ctx context.Context, paths []string) (map[string]*File, error) {
	if len(paths) == 0 {
		return make(map[string]*File), nil
	}

	const batchSize = 900 // SQLite default limit is 999, use 900 to be safe
	fileMap := make(map[string]*File)

	// Process paths in batches
	for i := 0; i < len(paths); i += batchSize {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		end := i + batchSize
		if end > len(paths) {
			end = len(paths)
		}
		batch := paths[i:end]

		// Build IN clause with placeholders for this batch
		args := make([]interface{}, len(batch))
		for j, path := range batch {
			args[j] = path
		}

		query := fmt.Sprintf(`
			SELECT id, path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, extension, created_at
			FROM files
			WHERE path IN (%s)
		`, buildInClause(len(batch)))

		rows, err := db.conn.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			file, err := scanFileRow(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			fileMap[file.Path] = file
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	return fileMap, nil
}

// GetAllFilesMap loads all files from the database into memory as a map
// This is optimized for incremental scans where we need fast lookups for every file
// WARNING: This loads the entire files table into memory - use only when appropriate
func (db *DB) GetAllFilesMap(ctx context.Context) (map[string]*File, error) {
	query := `
		SELECT id, path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, extension, created_at
		FROM files
	`

	rows, err := db.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all files: %w", err)
	}
	defer rows.Close()

	fileMap := make(map[string]*File)
	for rows.Next() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		file, err := scanFileRow(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file row: %w", err)
		}
		fileMap[file.Path] = file
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating files: %w", err)
	}

	return fileMap, nil
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
		SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, created_at
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
func (db *DB) BatchUpsertUsage(ctx context.Context, usages []*Usage) error {
	if len(usages) == 0 {
		return nil
	}

	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
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
		// Check for context cancellation in the loop
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		metadataJSON, err := json.Marshal(usage.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}

		_, err = stmt.ExecContext(ctx,
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
func (db *DB) DeleteUsageByService(ctx context.Context, service string) error {
	query := `DELETE FROM usage WHERE service = ?`
	_, err := db.conn.ExecContext(ctx, query, service)
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
// Handles SQLite variable limit by batching queries into chunks of 900 parameters
func (db *DB) GetUsageByFileIDs(fileIDs []int64) (map[int64][]*Usage, error) {
	if len(fileIDs) == 0 {
		return make(map[int64][]*Usage), nil
	}

	const batchSize = 900 // SQLite default limit is 999, use 900 to be safe
	usageMap := make(map[int64][]*Usage)

	// Process file IDs in batches
	for i := 0; i < len(fileIDs); i += batchSize {
		end := i + batchSize
		if end > len(fileIDs) {
			end = len(fileIDs)
		}
		batch := fileIDs[i:end]

		// Build IN clause with placeholders for this batch
		args := make([]interface{}, len(batch))
		for j, id := range batch {
			args[j] = id
		}

		query := fmt.Sprintf(`
			SELECT id, file_id, service, reference_path, metadata, created_at, updated_at
			FROM usage
			WHERE file_id IN (%s)
			ORDER BY file_id, service
		`, buildInClause(len(batch)))

		rows, err := db.conn.Query(query, args...)
		if err != nil {
			return nil, err
		}

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
				rows.Close()
				return nil, err
			}

			if err := json.Unmarshal([]byte(metadataJSON), &usage.Metadata); err != nil {
				usage.Metadata = make(map[string]interface{})
			}

			usage.CreatedAt = time.Unix(createdAt, 0)
			usage.UpdatedAt = time.Unix(updatedAt, 0)

			usageMap[usage.FileID] = append(usageMap[usage.FileID], usage)
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	return usageMap, nil
}

// UpdateOrphanedStatus updates the orphaned status of all files
func (db *DB) UpdateOrphanedStatus(ctx context.Context) error {
	query := `
		UPDATE files
		SET is_orphaned = CASE
			WHEN NOT EXISTS (SELECT 1 FROM usage WHERE usage.file_id = files.id)
			THEN 1
			ELSE 0
		END
	`
	_, err := db.conn.ExecContext(ctx, query)
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
		       f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
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
func (db *DB) GetFileExtensions(orphanedOnly bool, service string) ([]string, error) {
	var conditions []string
	args := []interface{}{}

	// Build WHERE clause
	if orphanedOnly {
		conditions = append(conditions, "f.is_orphaned = 1")
	}

	if service != "" {
		conditions = append(conditions, "EXISTS (SELECT 1 FROM usage u WHERE u.file_id = f.id AND u.service = ?)")
		args = append(args, service)
	}

	// Ensure we only get non-empty extensions
	conditions = append(conditions, "f.extension != ''")

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Query directly from the extension column - much more efficient!
	// Uses idx_files_extension or idx_files_orphaned_extension index
	query := `SELECT DISTINCT f.extension FROM files f ` + whereClause + ` ORDER BY f.extension`

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	extensions := []string{}
	for rows.Next() {
		var ext string
		if err := rows.Scan(&ext); err != nil {
			return nil, err
		}
		// Remove leading dot if present (extension column includes the dot)
		if len(ext) > 1 && ext[0] == '.' {
			ext = ext[1:]
		}
		extensions = append(extensions, ext)
	}

	return extensions, rows.Err()
}

// ListFiles retrieves files with filtering and pagination
func (db *DB) ListFiles(orphanedOnly bool, services []string, serviceFilterMode string, hardlinksOnly bool, extensions []string, deviceIDs []int64, limit, offset int, orderBy, direction string) ([]*File, int, error) {
	var conditions []string
	args := []interface{}{}

	if orphanedOnly {
		conditions = append(conditions, "f.is_orphaned = 1")
	}

	// Filter by device IDs (for disk-based filtering)
	// Use file_disk_locations table for accurate disk filtering (handles mergerfs setups)
	if len(deviceIDs) > 0 {
		placeholders := make([]string, len(deviceIDs))
		for i, deviceID := range deviceIDs {
			placeholders[i] = "?"
			args = append(args, deviceID)
		}
		conditions = append(conditions, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM file_disk_locations fdl WHERE fdl.file_id = f.id AND fdl.disk_device_id IN (%s))",
			strings.Join(placeholders, ", "),
		))
	}

	// Multi-service filtering with three modes
	if len(services) > 0 {
		switch serviceFilterMode {
		case "any":
			// File must be tracked by at least one of the selected services
			placeholders := make([]string, len(services))
			for i, svc := range services {
				placeholders[i] = "?"
				args = append(args, svc)
			}
			conditions = append(conditions, fmt.Sprintf(
				"EXISTS (SELECT 1 FROM usage u WHERE u.file_id = f.id AND u.service IN (%s))",
				strings.Join(placeholders, ", "),
			))

		case "all":
			// File must be tracked by ALL selected services (may have others too)
			placeholders := make([]string, len(services))
			for i, svc := range services {
				placeholders[i] = "?"
				args = append(args, svc)
			}
			args = append(args, len(services))
			conditions = append(conditions, fmt.Sprintf(
				"(SELECT COUNT(DISTINCT u.service) FROM usage u WHERE u.file_id = f.id AND u.service IN (%s)) = ?",
				strings.Join(placeholders, ", "),
			))

		case "exact":
			// File must be tracked by ONLY these services (exact match, no others)
			placeholders := make([]string, len(services))
			for i, svc := range services {
				placeholders[i] = "?"
				args = append(args, svc)
			}
			args = append(args, len(services)) // Total count must match
			args = append(args, len(services)) // Count of matching services must match
			conditions = append(conditions, fmt.Sprintf(
				"(SELECT COUNT(DISTINCT u.service) FROM usage u WHERE u.file_id = f.id) = ? AND "+
					"(SELECT COUNT(DISTINCT u.service) FROM usage u WHERE u.file_id = f.id AND u.service IN (%s)) = ?",
				strings.Join(placeholders, ", "),
			))
		}
	}

	if hardlinksOnly {
		conditions = append(conditions, `(f.device_id, f.inode) IN (
			SELECT device_id, inode FROM files GROUP BY device_id, inode HAVING COUNT(*) > 1
		)`)
	}

	// Filter by file extensions using the extension column (much faster than GLOB!)
	// Uses idx_files_extension or idx_files_orphaned_extension index
	if len(extensions) > 0 {
		placeholders := make([]string, len(extensions))
		for i, ext := range extensions {
			placeholders[i] = "?"
			// Ensure extension has leading dot for comparison with stored values
			if !strings.HasPrefix(ext, ".") {
				ext = "." + ext
			}
			args = append(args, strings.ToLower(ext))
		}
		conditions = append(conditions, fmt.Sprintf("f.extension IN (%s)", strings.Join(placeholders, ", ")))
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

	// Validate and sanitize orderBy and direction
	// SQL Injection Safety: ValidateOrderBy uses an allowlist to ensure only
	// valid column names are used. ValidateDirection ensures only ASC/DESC.
	// This is safe from SQL injection because both values are validated.
	safeOrderBy := ValidateOrderBy(orderBy)
	safeDirection := ValidateDirection(direction)

	query := fmt.Sprintf(`
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
		       f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
		FROM files f
		%s
		ORDER BY f.%s %s
		LIMIT ? OFFSET ?
	`, whereClause, safeOrderBy, safeDirection)

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
		       f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
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

// GetHardlinkGroupsFiltered returns hardlink groups with filtering, sorting, and pagination
func (db *DB) GetHardlinkGroupsFiltered(search, orderBy, direction string, limit, offset int) (map[string][]*File, int, error) {
	// Build search condition
	searchCondition := ""
	args := []interface{}{}
	if search != "" {
		searchCondition = "AND f.path LIKE ?"
		args = append(args, "%"+search+"%")
	}

	// Validate and sanitize orderBy and direction
	// SQL Injection Safety: ValidateHardlinkOrderBy and ValidateDirection use allowlists
	// to ensure only valid column names and directions are used.
	safeOrderBy := ValidateHardlinkOrderBy(orderBy)
	safeDirection := ValidateDirection(direction)

	// First, get total count of groups matching the search
	countQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT f.device_id || '-' || f.inode)
		FROM files f
		WHERE (f.device_id, f.inode) IN (
			SELECT device_id, inode
			FROM files
			GROUP BY device_id, inode
			HAVING COUNT(*) > 1
		)
		%s
	`, searchCondition)

	var total int
	err := db.conn.QueryRow(countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	// Build the main query with CTE for aggregation and sorting
	var query string
	var queryArgs []interface{}

	if safeOrderBy == "link_count" || safeOrderBy == "space_saved" {
		// For aggregate columns, we need to use a CTE
		query = fmt.Sprintf(`
			WITH hardlink_groups AS (
				SELECT
					f.device_id,
					f.inode,
					COUNT(*) as link_count,
					MAX(f.size) as file_size,
					MAX(f.size) * (COUNT(*) - 1) as space_saved,
					MIN(f.path) as first_path
				FROM files f
				WHERE (f.device_id, f.inode) IN (
					SELECT device_id, inode
					FROM files
					GROUP BY device_id, inode
					HAVING COUNT(*) > 1
				)
				%s
				GROUP BY f.device_id, f.inode
				ORDER BY %s %s
				LIMIT ? OFFSET ?
			)
			SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
			       f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
			FROM files f
			INNER JOIN hardlink_groups hg
				ON f.device_id = hg.device_id AND f.inode = hg.inode
			ORDER BY hg.%s %s, f.path
		`, searchCondition, safeOrderBy, safeDirection, safeOrderBy, safeDirection)

		queryArgs = append(args, limit, offset)
	} else {
		// For simple columns like path, we can use a simpler approach
		query = fmt.Sprintf(`
			WITH ranked_groups AS (
				SELECT DISTINCT
					f.device_id,
					f.inode,
					MIN(f.path) as first_path
				FROM files f
				WHERE (f.device_id, f.inode) IN (
					SELECT device_id, inode
					FROM files
					GROUP BY device_id, inode
					HAVING COUNT(*) > 1
				)
				%s
				GROUP BY f.device_id, f.inode
				ORDER BY %s %s
				LIMIT ? OFFSET ?
			)
			SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
			       f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
			FROM files f
			INNER JOIN ranked_groups rg
				ON f.device_id = rg.device_id AND f.inode = rg.inode
			ORDER BY rg.%s %s, f.path
		`, searchCondition, safeOrderBy, safeDirection, safeOrderBy, safeDirection)

		queryArgs = append(args, limit, offset)
	}

	rows, err := db.conn.Query(query, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	groups := make(map[string][]*File)

	for rows.Next() {
		file, err := scanFileRow(rows)
		if err != nil {
			return nil, 0, err
		}

		key := fmt.Sprintf("%d-%d", file.DeviceID, file.Inode)
		groups[key] = append(groups[key], file)
	}

	return groups, total, rows.Err()
}

// GetHardlinksByInodeDevice returns all hardlinked files for a specific inode and device
func (db *DB) GetHardlinksByInodeDevice(inode, deviceID int64) ([]*File, error) {
	query := `
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
		       f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
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

// CleanStaleScansOnStartup marks all running scans as interrupted on application startup
// This handles cases where the app was restarted while scans were running
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

// ExtractExtension extracts the file extension from a path
// Returns lowercase extension including the dot (e.g., ".mkv")
// Handles compound extensions like ".mkv.!qb", ".original_epub", ".tar.gz"
// Returns empty string if no extension found
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
func (db *DB) UpsertFileDiskLocation(loc *FileDiskLocation) error {
	query := `
		INSERT INTO file_disk_locations (file_id, disk_name, disk_device_id, disk_path, size, inode, modified_time, last_verified)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(file_id, disk_device_id) DO UPDATE SET
			disk_name = excluded.disk_name,
			disk_path = excluded.disk_path,
			size = excluded.size,
			inode = excluded.inode,
			modified_time = excluded.modified_time,
			last_verified = excluded.last_verified
	`

	_, err := db.conn.Exec(query,
		loc.FileID,
		loc.DiskName,
		loc.DiskDeviceID,
		loc.DiskPath,
		loc.Size,
		loc.Inode,
		loc.ModifiedTime.Unix(),
		loc.LastVerified.Unix(),
	)

	return err
}

// BatchUpsertFileDiskLocations batch inserts or updates file disk location records
func (db *DB) BatchUpsertFileDiskLocations(ctx context.Context, locs []*FileDiskLocation) error {
	if len(locs) == 0 {
		return nil
	}

	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO file_disk_locations (file_id, disk_name, disk_device_id, disk_path, size, inode, modified_time, last_verified)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(file_id, disk_device_id) DO UPDATE SET
			disk_name = excluded.disk_name,
			disk_path = excluded.disk_path,
			size = excluded.size,
			inode = excluded.inode,
			modified_time = excluded.modified_time,
			last_verified = excluded.last_verified
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, loc := range locs {
		_, err = stmt.ExecContext(ctx,
			loc.FileID,
			loc.DiskName,
			loc.DiskDeviceID,
			loc.DiskPath,
			loc.Size,
			loc.Inode,
			loc.ModifiedTime.Unix(),
			loc.LastVerified.Unix(),
		)
		if err != nil {
			return fmt.Errorf("failed to insert disk location: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetDiskLocationsForFile returns all disk locations for a file
func (db *DB) GetDiskLocationsForFile(fileID int64) ([]*FileDiskLocation, error) {
	query := `
		SELECT id, file_id, disk_name, disk_device_id, disk_path, size, inode, modified_time, last_verified, created_at
		FROM file_disk_locations
		WHERE file_id = ?
		ORDER BY disk_name
	`

	rows, err := db.conn.Query(query, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locations []*FileDiskLocation
	for rows.Next() {
		loc := &FileDiskLocation{}
		var modTime, lastVerified, createdAt int64

		err = rows.Scan(
			&loc.ID,
			&loc.FileID,
			&loc.DiskName,
			&loc.DiskDeviceID,
			&loc.DiskPath,
			&loc.Size,
			&loc.Inode,
			&modTime,
			&lastVerified,
			&createdAt,
		)
		if err != nil {
			return nil, err
		}

		loc.ModifiedTime = time.Unix(modTime, 0)
		loc.LastVerified = time.Unix(lastVerified, 0)
		loc.CreatedAt = time.Unix(createdAt, 0)

		locations = append(locations, loc)
	}

	return locations, rows.Err()
}

// GetDiskLocationsByFileIDs batch loads disk locations for multiple files
// Returns a map of file_id -> []*FileDiskLocation
func (db *DB) GetDiskLocationsByFileIDs(fileIDs []int64) (map[int64][]*FileDiskLocation, error) {
	if len(fileIDs) == 0 {
		return make(map[int64][]*FileDiskLocation), nil
	}

	// Build placeholders for IN clause
	placeholders := make([]string, len(fileIDs))
	args := make([]interface{}, len(fileIDs))
	for i, id := range fileIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, file_id, disk_name, disk_device_id, disk_path, size, inode, modified_time, last_verified, created_at
		FROM file_disk_locations
		WHERE file_id IN (%s)
		ORDER BY file_id, disk_name
	`, strings.Join(placeholders, ","))

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Group by file_id
	result := make(map[int64][]*FileDiskLocation)
	for rows.Next() {
		loc := &FileDiskLocation{}
		var modTime, lastVerified, createdAt int64

		err = rows.Scan(
			&loc.ID,
			&loc.FileID,
			&loc.DiskName,
			&loc.DiskDeviceID,
			&loc.DiskPath,
			&loc.Size,
			&loc.Inode,
			&modTime,
			&lastVerified,
			&createdAt,
		)
		if err != nil {
			return nil, err
		}

		loc.ModifiedTime = time.Unix(modTime, 0)
		loc.LastVerified = time.Unix(lastVerified, 0)
		loc.CreatedAt = time.Unix(createdAt, 0)

		result[loc.FileID] = append(result[loc.FileID], loc)
	}

	return result, rows.Err()
}

// GetFilesWithMultipleDiskLocations returns files that exist on multiple disks (cross-disk duplicates)
func (db *DB) GetFilesWithMultipleDiskLocations() ([]*File, error) {
	query := `
		SELECT DISTINCT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time, f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
		FROM files f
		JOIN file_disk_locations fdl ON f.id = fdl.file_id
		GROUP BY f.id
		HAVING COUNT(DISTINCT fdl.disk_device_id) > 1
		ORDER BY f.size DESC
	`

	rows, err := db.conn.Query(query)
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

// GetCrossDiskDuplicateCount returns the count of files on multiple disks
func (db *DB) GetCrossDiskDuplicateCount() (int64, error) {
	query := `
		SELECT COUNT(DISTINCT f.id)
		FROM files f
		JOIN file_disk_locations fdl ON f.id = fdl.file_id
		GROUP BY f.id
		HAVING COUNT(DISTINCT fdl.disk_device_id) > 1
	`

	var count int64
	err := db.conn.QueryRow(query).Scan(&count)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return count, err
}

// DeleteDiskLocationsByDisk deletes all disk locations for a specific disk device
func (db *DB) DeleteDiskLocationsByDisk(diskDeviceID int64) error {
	query := `DELETE FROM file_disk_locations WHERE disk_device_id = ?`
	_, err := db.conn.Exec(query, diskDeviceID)
	return err
}

// Hash Scanning Methods

// GetFilesNeedingHash returns files that need hashing (optionally filtered by size)
func (db *DB) GetFilesNeedingHash(minSize, maxSize int64) ([]File, error) {
	query := `
		SELECT id, path, size, inode, device_id, modified_time, last_verified, is_orphaned
		FROM files
		WHERE hash_calculated = 0
	`

	args := []interface{}{}
	if minSize > 0 {
		query += ` AND size >= ?`
		args = append(args, minSize)
	}
	if maxSize > 0 {
		query += ` AND size <= ?`
		args = append(args, maxSize)
	}

	query += ` ORDER BY size ASC` // Hash smaller files first for faster initial progress

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query files needing hash: %w", err)
	}
	defer rows.Close()

	var files []File
	for rows.Next() {
		var file File
		err := rows.Scan(
			&file.ID,
			&file.Path,
			&file.Size,
			&file.Inode,
			&file.DeviceID,
			&file.ModifiedTime,
			&file.LastVerified,
			&file.IsOrphaned,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file row: %w", err)
		}
		files = append(files, file)
	}

	return files, rows.Err()
}

// UpdateFileHash updates the hash for a file
func (db *DB) UpdateFileHash(fileID int64, hash, algorithm, hashType string) error {
	query := `
		UPDATE files
		SET file_hash = ?, hash_algorithm = ?, hash_type = ?, hash_calculated = 1
		WHERE id = ?
	`
	_, err := db.conn.Exec(query, hash, algorithm, hashType, fileID)
	if err != nil {
		return fmt.Errorf("failed to update file hash: %w", err)
	}
	return nil
}

// GetHashedFileCount returns the count of files that have been hashed
func (db *DB) GetHashedFileCount() (int64, error) {
	var count int64
	err := db.conn.QueryRow(`SELECT COUNT(*) FROM files WHERE hash_calculated = 1`).Scan(&count)
	return count, err
}

// GetTotalHashableFileCount returns the count of all files (for progress tracking)
func (db *DB) GetTotalHashableFileCount(minSize int64) (int64, error) {
	query := `SELECT COUNT(*) FROM files WHERE 1=1`
	args := []interface{}{}

	if minSize > 0 {
		query += ` AND size >= ?`
		args = append(args, minSize)
	}

	var count int64
	err := db.conn.QueryRow(query, args...).Scan(&count)
	return count, err
}

// ClearAllHashes resets all hash data (useful when changing algorithms)
func (db *DB) ClearAllHashes() error {
	query := `UPDATE files SET file_hash = NULL, hash_algorithm = NULL, hash_type = NULL, hash_calculated = 0`
	_, err := db.conn.Exec(query)
	return err
}

// GetFilesWithQuickHashDuplicates returns files that have quick-hash duplicates
// These are files where 2+ files share the same quick hash (same size + quick hash match)
// Used for verification workflow: find potential duplicates, then full-hash them
func (db *DB) GetFilesWithQuickHashDuplicates() ([]File, error) {
	query := `
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
		       f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
		FROM files f
		WHERE f.hash_type = 'quick'
		  AND f.file_hash IN (
		      SELECT file_hash
		      FROM files
		      WHERE hash_type = 'quick' AND file_hash IS NOT NULL
		      GROUP BY file_hash, size
		      HAVING COUNT(*) > 1
		  )
		ORDER BY f.size DESC, f.file_hash
	`

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query files with quick hash duplicates: %w", err)
	}
	defer rows.Close()

	var files []File
	for rows.Next() {
		file, err := scanFileRow(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file row: %w", err)
		}
		files = append(files, *file)
	}

	return files, rows.Err()
}

// GetQuickHashDuplicateCount returns the count of files with quick-hash duplicates
func (db *DB) GetQuickHashDuplicateCount() (int64, error) {
	query := `
		SELECT COUNT(DISTINCT f.id)
		FROM files f
		WHERE f.hash_type = 'quick'
		  AND f.file_hash IN (
		      SELECT file_hash
		      FROM files
		      WHERE hash_type = 'quick' AND file_hash IS NOT NULL
		      GROUP BY file_hash, size
		      HAVING COUNT(*) > 1
		  )
	`

	var count int64
	err := db.conn.QueryRow(query).Scan(&count)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return count, err
}

// GetFilesWithQuickHashes returns all files that have quick hashes (for upgrading to full)
func (db *DB) GetFilesWithQuickHashes() ([]File, error) {
	query := `
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
		       f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
		FROM files f
		WHERE f.hash_type = 'quick'
		ORDER BY f.size DESC
	`

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query files with quick hashes: %w", err)
	}
	defer rows.Close()

	var files []File
	for rows.Next() {
		file, err := scanFileRow(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file row: %w", err)
		}
		files = append(files, *file)
	}

	return files, rows.Err()
}

// GetQuickHashCount returns the count of all files with quick hashes
func (db *DB) GetQuickHashCount() (int64, error) {
	var count int64
	err := db.conn.QueryRow(`SELECT COUNT(*) FROM files WHERE hash_type = 'quick'`).Scan(&count)
	return count, err
}
