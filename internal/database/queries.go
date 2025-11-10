package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
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

// GetFilesByService retrieves all files that are used by a specific service
func (db *DB) GetFilesByService(ctx context.Context, service string) ([]*File, error) {
	query := `
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time, f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
		FROM files f
		INNER JOIN usage u ON f.id = u.file_id
		WHERE u.service = ?
		ORDER BY f.path
	`

	rows, err := db.conn.QueryContext(ctx, query, service)
	if err != nil {
		return nil, fmt.Errorf("failed to query files by service: %w", err)
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

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return files, nil
}

// GetFilesByExtensions retrieves all files with specific extensions (WITH leading dot, e.g., ".srt")
func (db *DB) GetFilesByExtensions(ctx context.Context, extensions []string) ([]*File, error) {
	if len(extensions) == 0 {
		return []*File{}, nil
	}

	// Build IN clause
	placeholders := make([]string, len(extensions))
	args := make([]interface{}, len(extensions))
	for i, ext := range extensions {
		placeholders[i] = "?"
		args[i] = ext
	}

	query := fmt.Sprintf(`
		SELECT id, path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, extension, created_at
		FROM files
		WHERE extension IN (%s)
		ORDER BY path
	`, strings.Join(placeholders, ","))

	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query files by extensions: %w", err)
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

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return files, nil
}

// GetFilesByExtensionSuffix retrieves all files where extension ends with the given suffix
// This is useful for compound extensions like .!qb which can be .mkv.!qb, .mp4.!qb, etc.
func (db *DB) GetFilesByExtensionSuffix(ctx context.Context, suffix string) ([]*File, error) {
	query := `
		SELECT id, path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, extension, created_at
		FROM files
		WHERE extension LIKE ?
		ORDER BY path
	`

	// Use LIKE with % wildcard for suffix matching
	pattern := "%" + suffix

	rows, err := db.conn.QueryContext(ctx, query, pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to query files by extension suffix: %w", err)
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

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return files, nil
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

// sanitizeFTS5Query escapes and quotes a search query for safe FTS5 usage.
// This prevents FTS5 syntax errors when users search for strings containing
// special characters like dots, hyphens, or other FTS5 operators.
func sanitizeFTS5Query(query string) string {
	// Escape any existing double quotes by doubling them (FTS5 convention)
	escaped := strings.ReplaceAll(query, `"`, `""`)
	// Wrap in double quotes to make it a phrase search (literal match)
	return `"` + escaped + `"`
}

// SearchFiles searches for files by path using FTS
func (db *DB) SearchFiles(searchQuery string, orphanedOnly bool, services []string, serviceFilterMode string, hardlinksOnly bool, extensions []string, deviceIDs []int64, limit, offset int, orderBy, direction string) ([]*File, int, error) {
	var conditions []string
	args := []interface{}{}

	// FTS search condition (always required for SearchFiles)
	// Sanitize the query to prevent FTS5 syntax errors from special characters
	conditions = append(conditions, "f.id IN (SELECT rowid FROM files_fts WHERE files_fts MATCH ?)")
	args = append(args, sanitizeFTS5Query(searchQuery))

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

	whereClause := "WHERE " + strings.Join(conditions, " AND ")

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
func (db *DB) DeleteFile(fileID int64, details string, deleteFromFilesystem bool) error {
	tx, err := db.BeginTx()
	if err != nil {
		log.Printf("ERROR: DeleteFile - failed to begin transaction for file ID %d: %v", fileID, err)
		return err
	}
	defer tx.Rollback()

	// Get file path before deletion (needed for filesystem deletion and audit log)
	var filePath string
	err = tx.QueryRow(`SELECT path FROM files WHERE id = ?`, fileID).Scan(&filePath)
	if err != nil {
		log.Printf("ERROR: DeleteFile - failed to get file path for ID %d: %v", fileID, err)
		return fmt.Errorf("failed to get file path: %w", err)
	}

	// Optionally delete from filesystem
	if deleteFromFilesystem {
		if err := os.Remove(filePath); err != nil {
			// If filesystem deletion fails, don't delete from DB
			return fmt.Errorf("failed to delete file from filesystem (%s): %w", filePath, err)
		}
	}

	// Log the deletion with filesystem status
	auditDetails := details
	if deleteFromFilesystem {
		auditDetails = fmt.Sprintf("%s (deleted from filesystem: %s)", details, filePath)
	} else {
		auditDetails = fmt.Sprintf("%s (DB only)", details)
	}

	_, err = tx.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('delete', 'file', ?, ?)`,
		fileID, auditDetails,
	)
	if err != nil {
		return err
	}

	// Delete the file record (usage records will be cascade deleted)
	_, err = tx.Exec(`DELETE FROM files WHERE id = ?`, fileID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// LogDeletionError logs a failed deletion attempt to the audit_log
func (db *DB) LogDeletionError(fileID int64, path string, err error) error {
	_, execErr := db.conn.Exec(
		`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES (?, ?, ?, ?)`,
		"delete_failed",
		"file",
		fileID,
		fmt.Sprintf("Failed to delete %s: %v", path, err),
	)
	return execErr
}

// DeleteFileByPath deletes a file by its path
func (db *DB) DeleteFileByPath(path string, details string, deleteFromFilesystem bool) error {
	file, err := db.GetFileByPath(path)
	if err != nil {
		return err
	}
	return db.DeleteFile(file.ID, details, deleteFromFilesystem)
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

// GetDiskLocationsByFileIDs returns disk locations for multiple files in a single query
// Returns a map of fileID -> []*FileDiskLocation for efficient batch loading
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

	// Group locations by file_id
	locationsByFileID := make(map[int64][]*FileDiskLocation)
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

		locationsByFileID[loc.FileID] = append(locationsByFileID[loc.FileID], loc)
	}

	return locationsByFileID, rows.Err()
}

// CountFilesMissingDiskLocations returns the count of files that don't have disk location data
func (db *DB) CountFilesMissingDiskLocations() (int64, error) {
	query := `
		SELECT COUNT(*)
		FROM files
		WHERE id NOT IN (SELECT DISTINCT file_id FROM file_disk_locations)
	`

	var count int64
	err := db.conn.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// CountFileDiskLocations returns the total count of file disk location records
func (db *DB) CountFileDiskLocations() (int64, error) {
	query := `SELECT COUNT(*) FROM file_disk_locations`

	var count int64
	err := db.conn.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
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

// DeleteDiskLocationsByDisk deletes all disk locations for a specific disk device
func (db *DB) DeleteDiskLocationsByDisk(diskDeviceID int64) error {
	query := `DELETE FROM file_disk_locations WHERE disk_device_id = ?`
	_, err := db.conn.Exec(query, diskDeviceID)
	return err
}

// Hash Scanning Methods

// GetFilesNeedingHash returns files that need hashing (optionally filtered by size)
func (db *DB) GetFilesNeedingHash(minSize, maxSize int64, order string) ([]File, error) {
	query := `
		SELECT id, path, size, inode, device_id, modified_time, scan_id, last_verified, is_orphaned, extension, created_at
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

	// Add ordering based on strategy
	switch order {
	case "largest_first":
		query += ` ORDER BY size DESC`
	case "random":
		query += ` ORDER BY RANDOM()`
	case "by_disk":
		// Extract disk from path and order by it
		// Assumes paths like /disk1/..., /disk2/..., etc.
		query += ` ORDER BY path, size ASC`
	case "by_duplicate_probability":
		// Group same-size files together (likely duplicates)
		query += ` ORDER BY size, path`
	case "by_modification_time_newest":
		query += ` ORDER BY modified_time DESC, size ASC`
	case "by_modification_time_oldest":
		query += ` ORDER BY modified_time ASC, size ASC`
	case "db_order":
		// No ORDER BY for maximum query speed
	default: // "smallest_first" or empty
		query += ` ORDER BY size ASC` // Hash smaller files first for faster initial progress
	}

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query files needing hash: %w", err)
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

// UpdateFileHashWithLevel updates a file's hash information including the hash level
// Used for progressive hashing to track which level of verification has been performed
func (db *DB) UpdateFileHashWithLevel(fileID int64, hash, algorithm string, level int) error {
	// Determine hash_type from level
	var hashType string
	switch level {
	case 1:
		hashType = "quick"
	case 6:
		hashType = "full"
	default:
		hashType = "partial"
	}

	query := `
		UPDATE files
		SET file_hash = ?, hash_algorithm = ?, hash_type = ?, hash_level = ?, hash_calculated = 1
		WHERE id = ?
	`
	_, err := db.conn.Exec(query, hash, algorithm, hashType, level, fileID)
	if err != nil {
		return fmt.Errorf("failed to update file hash with level: %w", err)
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
func (db *DB) GetTotalHashableFileCount(minSize int64, maxSize int64) (int64, error) {
	query := `SELECT COUNT(*) FROM files WHERE 1=1`
	args := []interface{}{}

	if minSize > 0 {
		query += ` AND size >= ?`
		args = append(args, minSize)
	}

	if maxSize > 0 {
		query += ` AND size <= ?`
		args = append(args, maxSize)
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
func (db *DB) GetFilesWithQuickHashDuplicates(minSize int64, maxSize int64) ([]File, error) {
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
	`
	args := []interface{}{}

	if minSize > 0 {
		query += ` AND f.size >= ?`
		args = append(args, minSize)
	}

	if maxSize > 0 {
		query += ` AND f.size <= ?`
		args = append(args, maxSize)
	}

	query += ` ORDER BY f.size DESC, f.file_hash`

	rows, err := db.conn.Query(query, args...)
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
func (db *DB) GetFilesWithQuickHashes(minSize int64, maxSize int64) ([]File, error) {
	query := `
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
		       f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
		FROM files f
		WHERE f.hash_type = 'quick'
	`
	args := []interface{}{}

	if minSize > 0 {
		query += ` AND f.size >= ?`
		args = append(args, minSize)
	}

	if maxSize > 0 {
		query += ` AND f.size <= ?`
		args = append(args, maxSize)
	}

	query += ` ORDER BY f.size DESC`

	rows, err := db.conn.Query(query, args...)
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

// GetFilesWithHashDuplicatesAtLevel returns files at a specific hash level that have duplicates
// Used for progressive hash verification to find which files need upgrading to next level
func (db *DB) GetFilesWithHashDuplicatesAtLevel(level int, minSize int64, maxSize int64) ([]File, error) {
	query := `
		SELECT f.id, f.path, f.size, f.inode, f.device_id, f.modified_time,
		       f.scan_id, f.last_verified, f.is_orphaned, f.extension, f.created_at
		FROM files f
		WHERE f.hash_level = ?
		  AND f.hash_calculated = 1
		  AND f.file_hash IN (
		      SELECT file_hash
		      FROM files
		      WHERE hash_level = ? AND file_hash IS NOT NULL AND hash_calculated = 1
		      GROUP BY file_hash, size
		      HAVING COUNT(*) > 1
		  )
	`
	args := []interface{}{level, level}

	if minSize > 0 {
		query += ` AND f.size >= ?`
		args = append(args, minSize)
	}

	if maxSize > 0 {
		query += ` AND f.size <= ?`
		args = append(args, maxSize)
	}

	query += ` ORDER BY f.size DESC, f.file_hash`

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query files with hash duplicates at level %d: %w", level, err)
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

// GetHashLevelDuplicateCount returns the count of files with duplicates at a specific level
func (db *DB) GetHashLevelDuplicateCount(level int) (int64, error) {
	query := `
		SELECT COUNT(DISTINCT f.id)
		FROM files f
		WHERE f.hash_level = ?
		  AND f.hash_calculated = 1
		  AND f.file_hash IN (
		      SELECT file_hash
		      FROM files
		      WHERE hash_level = ? AND file_hash IS NOT NULL AND hash_calculated = 1
		      GROUP BY file_hash, size
		      HAVING COUNT(*) > 1
		  )
	`

	var count int64
	err := db.conn.QueryRow(query, level, level).Scan(&count)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return count, err
}

// GetHashLevelStats returns statistics about files at each hash level
func (db *DB) GetHashLevelStats() (map[int]int64, error) {
	query := `
		SELECT hash_level, COUNT(*) as count
		FROM files
		WHERE hash_calculated = 1 AND file_hash IS NOT NULL
		GROUP BY hash_level
		ORDER BY hash_level
	`

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query hash level stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[int]int64)
	for rows.Next() {
		var level int
		var count int64
		if err := rows.Scan(&level, &count); err != nil {
			return nil, fmt.Errorf("failed to scan hash level stats: %w", err)
		}
		stats[level] = count
	}

	return stats, rows.Err()
}

// LogConsolidation logs a cross-disk consolidation operation to the audit log
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
func (db *DB) UpdateFileInode(path string, deviceID, inode uint64) error {
	query := `
		UPDATE files
		SET device_id = ?, inode = ?
		WHERE path = ?
	`

	result, err := db.conn.Exec(query, deviceID, inode, path)
	if err != nil {
		return fmt.Errorf("failed to update inode for %s: %w", path, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no file found with path: %s", path)
	}

	return nil
}

// DeleteUnverifiedFiles removes files that weren't updated during the current scan
// This is used during full scans to clean up files that no longer exist on disk
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
func (db *DB) GetAllFilePaths(ctx context.Context) ([]string, error) {
	rows, err := db.conn.QueryContext(ctx, `SELECT path FROM files ORDER BY path`)
	if err != nil {
		return nil, fmt.Errorf("failed to query file paths: %w", err)
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("failed to scan path: %w", err)
		}
		paths = append(paths, path)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating paths: %w", err)
	}

	return paths, nil
}

// DeleteFilesNotInSet removes files from the database that are not in the provided set
// This is used by the manual cleanup scan to remove files that no longer exist on disk
func (db *DB) DeleteFilesNotInSet(ctx context.Context, existingPaths map[string]bool, scanID int64) (int64, error) {
	tx, err := db.BeginTx()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// Get all file paths from database
	rows, err := tx.Query(`SELECT id, path FROM files`)
	if err != nil {
		return 0, fmt.Errorf("failed to query files: %w", err)
	}
	defer rows.Close()

	// Collect IDs of files to delete
	var toDelete []int64
	for rows.Next() {
		var id int64
		var path string
		if err := rows.Scan(&id, &path); err != nil {
			return 0, fmt.Errorf("failed to scan file: %w", err)
		}

		// If path doesn't exist in the filesystem set, mark for deletion
		if !existingPaths[path] {
			toDelete = append(toDelete, id)
		}
	}

	if err = rows.Err(); err != nil {
		return 0, fmt.Errorf("error iterating files: %w", err)
	}

	// Log the cleanup action if we're deleting files
	if len(toDelete) > 0 {
		_, err = tx.Exec(
			`INSERT INTO audit_log (action, entity_type, entity_id, details) VALUES ('cleanup', 'scan', ?, ?)`,
			scanID, fmt.Sprintf("Manual cleanup removed %d files that no longer exist on disk", len(toDelete)),
		)
		if err != nil {
			return 0, fmt.Errorf("failed to log cleanup: %w", err)
		}

		// Delete files in batches to avoid query length limits
		batchSize := 500
		for i := 0; i < len(toDelete); i += batchSize {
			end := i + batchSize
			if end > len(toDelete) {
				end = len(toDelete)
			}
			batch := toDelete[i:end]

			// Build placeholders for IN clause
			placeholders := make([]string, len(batch))
			args := make([]interface{}, len(batch))
			for j, id := range batch {
				placeholders[j] = "?"
				args[j] = id
			}

			query := fmt.Sprintf(`DELETE FROM files WHERE id IN (%s)`, strings.Join(placeholders, ","))
			_, err = tx.Exec(query, args...)
			if err != nil {
				return 0, fmt.Errorf("failed to delete file batch: %w", err)
			}
		}
	}

	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit: %w", err)
	}

	return int64(len(toDelete)), nil
}

// ClearMissingFiles deletes all missing file records for a specific scan
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
