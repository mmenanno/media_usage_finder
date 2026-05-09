package database

import (
	"database/sql"
	"fmt"
)

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
