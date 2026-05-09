package database

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
)

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
