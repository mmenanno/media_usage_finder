package database

import (
	"fmt"
	"strings"
)

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
	// Allow exact mode even with empty services (to show orphaned files)
	if len(services) > 0 || serviceFilterMode == "exact" {
		switch serviceFilterMode {
		case "any":
			// File must be tracked by at least one of the selected services
			if len(services) == 0 {
				// If no services selected in any mode, don't filter by services
				break
			}
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
			if len(services) == 0 {
				// If no services selected in all mode, don't filter by services
				break
			}
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
			if len(services) == 0 {
				// If no services selected in exact mode, show only orphaned files (files with no services)
				conditions = append(conditions, "NOT EXISTS (SELECT 1 FROM usage u WHERE u.file_id = f.id)")
			} else {
				// File must have exactly the selected services and no others
				// Build placeholders for IN clause
				placeholders := make([]string, len(services))
				for i := range services {
					placeholders[i] = "?"
				}
				// Query structure:
				// 1. First ? = total count check
				// 2. Services for IN clause (one ? per service)
				// 3. Last ? = matching count check
				args = append(args, len(services)) // First ?: Total count must match
				for _, svc := range services {
					args = append(args, strings.ToLower(svc)) // Services for IN clause
				}
				args = append(args, len(services)) // Last ?: Count of matching services must match
				queryPart := fmt.Sprintf(
					"(SELECT COUNT(DISTINCT LOWER(u.service)) FROM usage u WHERE u.file_id = f.id) = ? AND "+
						"(SELECT COUNT(DISTINCT LOWER(u.service)) FROM usage u WHERE u.file_id = f.id AND LOWER(u.service) IN (%s)) = ?",
					strings.Join(placeholders, ", "),
				)
				conditions = append(conditions, queryPart)
			}
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
	// Allow exact mode even with empty services (to show orphaned files)
	if len(services) > 0 || serviceFilterMode == "exact" {
		switch serviceFilterMode {
		case "any":
			// File must be tracked by at least one of the selected services
			if len(services) == 0 {
				// If no services selected in any mode, don't filter by services
				break
			}
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
			if len(services) == 0 {
				// If no services selected in all mode, don't filter by services
				break
			}
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
			if len(services) == 0 {
				// If no services selected in exact mode, show only orphaned files (files with no services)
				conditions = append(conditions, "NOT EXISTS (SELECT 1 FROM usage u WHERE u.file_id = f.id)")
			} else {
				// File must have exactly the selected services and no others
				// Build placeholders for IN clause
				placeholders := make([]string, len(services))
				for i := range services {
					placeholders[i] = "?"
				}
				// Query structure:
				// 1. First ? = total count check
				// 2. Services for IN clause (one ? per service)
				// 3. Last ? = matching count check
				args = append(args, len(services)) // First ?: Total count must match
				for _, svc := range services {
					args = append(args, strings.ToLower(svc)) // Services for IN clause
				}
				args = append(args, len(services)) // Last ?: Count of matching services must match
				queryPart := fmt.Sprintf(
					"(SELECT COUNT(DISTINCT LOWER(u.service)) FROM usage u WHERE u.file_id = f.id) = ? AND "+
						"(SELECT COUNT(DISTINCT LOWER(u.service)) FROM usage u WHERE u.file_id = f.id AND LOWER(u.service) IN (%s)) = ?",
					strings.Join(placeholders, ", "),
				)
				conditions = append(conditions, queryPart)
			}
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

	queryArgs := append(args, limit, offset)
	rows, err := db.conn.Query(query, queryArgs...)
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
