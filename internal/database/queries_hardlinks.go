package database

import (
	"fmt"
)

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
