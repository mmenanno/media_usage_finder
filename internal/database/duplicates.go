package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// HardlinkCluster represents a group of files that share the same inode (already hardlinked)
type HardlinkCluster struct {
	DeviceID     int64           // Device these files are on
	Inode        int64           // Shared inode
	Files        []DuplicateFile // Files in this cluster
	IsLinked     bool            // true if files already share an inode (LinkCount > 1)
	LinkCount    int             // Number of files in cluster
	SpaceSavings int64           // Actual savings if linked to primary (0 if already linked)
}

// DuplicateGroup represents a group of files with identical hashes
type DuplicateGroup struct {
	FileHash         string
	HashAlgorithm    string
	HashType         string // 'quick' or 'full'
	TotalCopies      int
	UniqueDiskCount  int
	TotalSize        int64
	WastedSpace      int64 // (copies - 1) * size - DEPRECATED: Use ActualSavings
	HardlinkClusters []HardlinkCluster
	ActualSavings    int64 // Savings accounting for existing hardlinks
	Files            []DuplicateFile
}

// DuplicateFile represents a single file within a duplicate group
type DuplicateFile struct {
	ID             int64
	Path           string
	Size           int64
	DeviceID       int64
	Inode          int64
	DiskName       string // Looked up from disk detector
	DiskUsedPercent float64
	IsOrphaned     bool
	ServiceUsage   []string
	ModifiedTime   time.Time
}

// GroupFilesByInode groups files that are already hardlinked together
// Files with the same (DeviceID, Inode) are already sharing physical disk space
func GroupFilesByInode(files []DuplicateFile, fileSize int64) []HardlinkCluster {
	if len(files) == 0 {
		return nil
	}

	// Group files by (DeviceID, Inode)
	clusterMap := make(map[string]*HardlinkCluster)

	for i := range files {
		key := fmt.Sprintf("%d:%d", files[i].DeviceID, files[i].Inode)

		if cluster, exists := clusterMap[key]; exists {
			cluster.Files = append(cluster.Files, files[i])
			cluster.LinkCount++
		} else {
			clusterMap[key] = &HardlinkCluster{
				DeviceID:     files[i].DeviceID,
				Inode:        files[i].Inode,
				Files:        []DuplicateFile{files[i]},
				LinkCount:    1,
				SpaceSavings: 0, // Will be calculated later
			}
		}
	}

	// Convert to slice and mark linked clusters
	clusters := make([]HardlinkCluster, 0, len(clusterMap))
	for _, cluster := range clusterMap {
		cluster.IsLinked = cluster.LinkCount > 1
		// If already linked (multiple files with same inode), no savings possible
		// If not linked yet, potential savings = fileSize (when linked to primary)
		if !cluster.IsLinked {
			cluster.SpaceSavings = fileSize
		}
		clusters = append(clusters, *cluster)
	}

	return clusters
}

// GetSameDiskDuplicates finds files with the same hash on the same disk
// These are candidates for hardlinking to save space
// If limit is 0 or negative, returns all results
func (db *DB) GetSameDiskDuplicates(limit int) ([]*DuplicateGroup, error) {
	query := `
		SELECT
			file_hash,
			hash_algorithm,
			hash_type,
			device_id,
			COUNT(*) as copies,
			MAX(size) as size
		FROM files
		WHERE file_hash IS NOT NULL
		  AND hash_calculated = 1
		  AND hash_type IS NOT NULL
		GROUP BY file_hash, hash_algorithm, hash_type, device_id
		HAVING COUNT(*) > 1
		ORDER BY size * (COUNT(*) - 1) DESC
	`

	// Add limit if specified
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query same-disk duplicates: %w", err)
	}
	defer rows.Close()

	var groups []*DuplicateGroup
	for rows.Next() {
		var hash, algorithm string
		var hashType sql.NullString
		var deviceID, copies, size int64

		if err := rows.Scan(&hash, &algorithm, &hashType, &deviceID, &copies, &size); err != nil {
			return nil, fmt.Errorf("failed to scan duplicate group: %w", err)
		}

		// Get files in this group first
		files, err := db.getFilesForDuplicateGroup(hash, &deviceID)
		if err != nil {
			return nil, fmt.Errorf("failed to get files for group: %w", err)
		}

		// Group files by inode to detect existing hardlinks
		clusters := GroupFilesByInode(files, size)

		// Calculate actual savings: only count unique inodes
		// If we have N clusters, we can link them all to one primary, saving (N-1) * size
		actualSavings := int64(0)
		if len(clusters) > 1 {
			actualSavings = int64(len(clusters)-1) * size
		}

		group := &DuplicateGroup{
			FileHash:         hash,
			HashAlgorithm:    algorithm,
			HashType:         hashType.String,
			TotalCopies:      int(copies),
			UniqueDiskCount:  1, // Same disk by definition
			TotalSize:        size,
			WastedSpace:      (copies - 1) * size, // Keep for backwards compat, but deprecated
			Files:            files,
			HardlinkClusters: clusters,
			ActualSavings:    actualSavings,
		}

		groups = append(groups, group)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating duplicate groups: %w", err)
	}

	return groups, nil
}

// GetCrossDiskDuplicates finds files with the same hash across different disks
// These are candidates for consolidation to the least full disk
// If limit is 0 or negative, returns all results
func (db *DB) GetCrossDiskDuplicates(limit int) ([]*DuplicateGroup, error) {
	query := `
		SELECT
			file_hash,
			hash_algorithm,
			hash_type,
			COUNT(*) as total_copies,
			COUNT(DISTINCT device_id) as disk_count,
			MAX(size) as size
		FROM files
		WHERE file_hash IS NOT NULL
		  AND hash_calculated = 1
		  AND hash_type IS NOT NULL
		GROUP BY file_hash, hash_algorithm, hash_type
		HAVING COUNT(DISTINCT device_id) > 1
		ORDER BY size * (COUNT(*) - 1) DESC
	`

	// Add limit if specified
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query cross-disk duplicates: %w", err)
	}
	defer rows.Close()

	var groups []*DuplicateGroup
	for rows.Next() {
		var hash, algorithm string
		var hashType sql.NullString
		var totalCopies, diskCount, size int64

		if err := rows.Scan(&hash, &algorithm, &hashType, &totalCopies, &diskCount, &size); err != nil {
			return nil, fmt.Errorf("failed to scan duplicate group: %w", err)
		}

		group := &DuplicateGroup{
			FileHash:        hash,
			HashAlgorithm:   algorithm,
			HashType:        hashType.String,
			TotalCopies:     int(totalCopies),
			UniqueDiskCount: int(diskCount),
			TotalSize:       size,
			WastedSpace:     (totalCopies - 1) * size,
		}

		// Get files in this group
		files, err := db.getFilesForDuplicateGroup(hash, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get files for group: %w", err)
		}
		group.Files = files

		groups = append(groups, group)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating duplicate groups: %w", err)
	}

	return groups, nil
}

// getFilesForDuplicateGroup retrieves all files with a given hash
// If deviceID is provided, filters to that device only
func (db *DB) getFilesForDuplicateGroup(hash string, deviceID *int64) ([]DuplicateFile, error) {
	query := `
		SELECT
			f.id,
			f.path,
			f.size,
			f.device_id,
			f.inode,
			f.is_orphaned,
			f.modified_time
		FROM files f
		WHERE f.file_hash = ?
	`

	args := []interface{}{hash}
	if deviceID != nil {
		query += " AND f.device_id = ?"
		args = append(args, *deviceID)
	}

	query += " ORDER BY f.path"

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query files for duplicate group: %w", err)
	}
	defer rows.Close()

	var files []DuplicateFile
	for rows.Next() {
		var file DuplicateFile
		var modTime int64

		if err := rows.Scan(
			&file.ID,
			&file.Path,
			&file.Size,
			&file.DeviceID,
			&file.Inode,
			&file.IsOrphaned,
			&modTime,
		); err != nil {
			return nil, fmt.Errorf("failed to scan file: %w", err)
		}

		file.ModifiedTime = time.Unix(modTime, 0)

		// Get service usage for this file
		services, err := db.getServicesUsingFile(file.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get services for file %d: %w", file.ID, err)
		}
		file.ServiceUsage = services

		files = append(files, file)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating files: %w", err)
	}

	return files, nil
}

// getServicesUsingFile returns a list of service names that use the given file
func (db *DB) getServicesUsingFile(fileID int64) ([]string, error) {
	query := `SELECT DISTINCT service FROM usage WHERE file_id = ? ORDER BY service`

	rows, err := db.conn.Query(query, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to query services: %w", err)
	}
	defer rows.Close()

	var services []string
	for rows.Next() {
		var service string
		if err := rows.Scan(&service); err != nil {
			return nil, fmt.Errorf("failed to scan service: %w", err)
		}
		services = append(services, service)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating services: %w", err)
	}

	return services, nil
}

// GetDuplicateStats returns summary statistics about duplicates
func (db *DB) GetDuplicateStats() (*DuplicateStats, error) {
	stats := &DuplicateStats{}

	// Count cross-disk duplicate groups
	err := db.conn.QueryRow(`
		SELECT COUNT(DISTINCT file_hash)
		FROM files
		WHERE file_hash IS NOT NULL
		  AND hash_calculated = 1
		  AND hash_type IS NOT NULL
		GROUP BY file_hash
		HAVING COUNT(DISTINCT device_id) > 1
	`).Scan(&stats.CrossDiskGroups)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to count cross-disk groups: %w", err)
	}

	// Count same-disk duplicate groups (only groups with multiple unique inodes)
	err = db.conn.QueryRow(`
		SELECT COUNT(*)
		FROM (
			SELECT file_hash, device_id
			FROM files
			WHERE file_hash IS NOT NULL
			  AND hash_calculated = 1
			  AND hash_type IS NOT NULL
			GROUP BY file_hash, device_id
			HAVING COUNT(DISTINCT inode) > 1
		)
	`).Scan(&stats.SameDiskGroups)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to count same-disk groups: %w", err)
	}

	// Calculate potential cross-disk savings
	err = db.conn.QueryRow(`
		SELECT COALESCE(SUM(size * (copies - 1)), 0)
		FROM (
			SELECT file_hash, COUNT(*) as copies, MAX(size) as size
			FROM files
			WHERE file_hash IS NOT NULL
			  AND hash_calculated = 1
			  AND hash_type IS NOT NULL
			GROUP BY file_hash
			HAVING COUNT(DISTINCT device_id) > 1
		)
	`).Scan(&stats.CrossDiskPotentialSavings)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate cross-disk savings: %w", err)
	}

	// Calculate potential same-disk savings (accounting for existing hardlinks)
	// We need to count unique inodes per hash group, not total files
	err = db.conn.QueryRow(`
		SELECT COALESCE(SUM(size * (unique_inodes - 1)), 0)
		FROM (
			SELECT file_hash, device_id,
			       COUNT(DISTINCT inode) as unique_inodes,
			       MAX(size) as size
			FROM files
			WHERE file_hash IS NOT NULL
			  AND hash_calculated = 1
			  AND hash_type IS NOT NULL
			GROUP BY file_hash, device_id
			HAVING COUNT(DISTINCT inode) > 1
		)
	`).Scan(&stats.SameDiskPotentialSavings)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate same-disk savings: %w", err)
	}

	stats.TotalPotentialSavings = stats.CrossDiskPotentialSavings + stats.SameDiskPotentialSavings

	return stats, nil
}

// DuplicateStats contains summary statistics about duplicates
type DuplicateStats struct {
	CrossDiskGroups           int64
	SameDiskGroups            int64
	CrossDiskPotentialSavings int64
	SameDiskPotentialSavings  int64
	TotalPotentialSavings     int64
}

// GetFilesWithHash returns all files that have a specific hash
func (db *DB) GetFilesWithHash(hash string) ([]DuplicateFile, error) {
	return db.getFilesForDuplicateGroup(hash, nil)
}

// GetDuplicateGroupByHash returns a complete duplicate group for a specific hash
func (db *DB) GetDuplicateGroupByHash(hash string) (*DuplicateGroup, error) {
	// First get the group metadata
	query := `
		SELECT
			file_hash,
			hash_algorithm,
			hash_type,
			COUNT(*) as total_copies,
			COUNT(DISTINCT device_id) as disk_count,
			MAX(size) as size
		FROM files
		WHERE file_hash = ?
		  AND hash_calculated = 1
		GROUP BY file_hash, hash_algorithm, hash_type
	`

	var group DuplicateGroup
	var hashType sql.NullString
	var totalCopies, diskCount, size int64

	err := db.conn.QueryRow(query, hash).Scan(
		&group.FileHash,
		&group.HashAlgorithm,
		&hashType,
		&totalCopies,
		&diskCount,
		&size,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("duplicate group not found for hash: %s", hash)
		}
		return nil, fmt.Errorf("failed to query duplicate group: %w", err)
	}

	group.HashType = hashType.String
	group.TotalCopies = int(totalCopies)
	group.UniqueDiskCount = int(diskCount)
	group.TotalSize = size
	group.WastedSpace = (totalCopies - 1) * size

	// Get files in this group
	files, err := db.getFilesForDuplicateGroup(hash, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get files for group: %w", err)
	}
	group.Files = files

	return &group, nil
}

// FilterDuplicatesByHashType filters duplicate groups by hash type (quick/full)
// hashTypes can be: "full", "quick", or "quick,full" for both
func (db *DB) FilterDuplicatesByHashType(crossDisk bool, hashTypes string) ([]*DuplicateGroup, error) {
	var query string

	// Parse hash types
	typeList := strings.Split(hashTypes, ",")
	placeholders := make([]string, len(typeList))
	args := make([]interface{}, len(typeList))
	for i, t := range typeList {
		placeholders[i] = "?"
		args = append(args, strings.TrimSpace(t))
	}

	if crossDisk {
		query = `
			SELECT
				file_hash,
				hash_algorithm,
				hash_type,
				COUNT(*) as total_copies,
				COUNT(DISTINCT device_id) as disk_count,
				MAX(size) as size
			FROM files
			WHERE file_hash IS NOT NULL
			  AND hash_calculated = 1
			  AND hash_type IN (` + strings.Join(placeholders, ",") + `)
			GROUP BY file_hash, hash_algorithm, hash_type
			HAVING COUNT(DISTINCT device_id) > 1
			ORDER BY size * (COUNT(*) - 1) DESC
		`
	} else {
		query = `
			SELECT
				file_hash,
				hash_algorithm,
				hash_type,
				device_id,
				COUNT(*) as copies,
				MAX(size) as size
			FROM files
			WHERE file_hash IS NOT NULL
			  AND hash_calculated = 1
			  AND hash_type IN (` + strings.Join(placeholders, ",") + `)
			GROUP BY file_hash, hash_algorithm, hash_type, device_id
			HAVING COUNT(*) > 1
			ORDER BY size * (COUNT(*) - 1) DESC
		`
	}

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query filtered duplicates: %w", err)
	}
	defer rows.Close()

	var groups []*DuplicateGroup
	for rows.Next() {
		var hash, algorithm string
		var hashType sql.NullString
		var copies, diskCount, size int64
		var deviceID *int64

		if crossDisk {
			if err := rows.Scan(&hash, &algorithm, &hashType, &copies, &diskCount, &size); err != nil {
				return nil, fmt.Errorf("failed to scan duplicate group: %w", err)
			}
		} else {
			var devID int64
			if err := rows.Scan(&hash, &algorithm, &hashType, &devID, &copies, &size); err != nil {
				return nil, fmt.Errorf("failed to scan duplicate group: %w", err)
			}
			deviceID = &devID
			diskCount = 1
		}

		group := &DuplicateGroup{
			FileHash:        hash,
			HashAlgorithm:   algorithm,
			HashType:        hashType.String,
			TotalCopies:     int(copies),
			UniqueDiskCount: int(diskCount),
			TotalSize:       size,
			WastedSpace:     (copies - 1) * size,
		}

		// Get files in this group
		files, err := db.getFilesForDuplicateGroup(hash, deviceID)
		if err != nil {
			return nil, fmt.Errorf("failed to get files for group: %w", err)
		}
		group.Files = files

		groups = append(groups, group)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating duplicate groups: %w", err)
	}

	return groups, nil
}
