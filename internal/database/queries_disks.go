package database

import (
	"context"
	"fmt"
	"strings"
	"time"
)

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
