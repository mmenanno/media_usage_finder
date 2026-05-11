package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// SavedView is a named bookmark for a /files page query — a JSON blob
// of filter parameters plus presentation metadata. System views are
// seeded on first migration and cannot be deleted (only renamed).
type SavedView struct {
	ID          int64
	Name        string
	Icon        string
	Description string
	// Filters is the decoded `filters` JSON. Keys map to /files
	// query-string parameter names (services, extensions, devices,
	// search, orphaned, hardlink, order, direction,
	// service_filter_mode).
	Filters   map[string]string
	SortOrder int
	IsSystem  bool
	CreatedAt time.Time
	UpdatedAt time.Time
	LastUsed  *time.Time
}

// ListSavedViews returns every saved view sorted by sort_order then
// name — matches the order chips appear on the Files page.
func (db *DB) ListSavedViews() ([]*SavedView, error) {
	rows, err := db.conn.Query(`
		SELECT id, name, icon, description, filters, sort_order,
		       is_system, created_at, updated_at, last_used
		FROM saved_views
		ORDER BY sort_order, name
	`)
	if err != nil {
		return nil, fmt.Errorf("list saved views: %w", err)
	}
	defer rows.Close()

	var out []*SavedView
	for rows.Next() {
		v, err := scanSavedViewRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// GetSavedView returns a single view by ID, or sql.ErrNoRows.
func (db *DB) GetSavedView(id int64) (*SavedView, error) {
	row := db.conn.QueryRow(`
		SELECT id, name, icon, description, filters, sort_order,
		       is_system, created_at, updated_at, last_used
		FROM saved_views WHERE id = ?
	`, id)
	return scanSavedViewRow(row)
}

// CreateSavedView inserts a new (non-system) view. The filters map is
// serialized to JSON; an empty map becomes "{}".
func (db *DB) CreateSavedView(v *SavedView) (int64, error) {
	filtersJSON, err := json.Marshal(v.Filters)
	if err != nil {
		return 0, fmt.Errorf("marshal filters: %w", err)
	}
	res, err := db.conn.Exec(`
		INSERT INTO saved_views (name, icon, description, filters, sort_order, is_system)
		VALUES (?, ?, ?, ?, ?, 0)
	`, v.Name, v.Icon, v.Description, string(filtersJSON), v.SortOrder)
	if err != nil {
		return 0, fmt.Errorf("insert saved view: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last insert id: %w", err)
	}
	return id, nil
}

// UpdateSavedView updates name/icon/description/filters/sort_order on
// any view (system views may be renamed but their is_system flag
// remains 1).
func (db *DB) UpdateSavedView(v *SavedView) error {
	filtersJSON, err := json.Marshal(v.Filters)
	if err != nil {
		return fmt.Errorf("marshal filters: %w", err)
	}
	_, err = db.conn.Exec(`
		UPDATE saved_views
		SET name = ?, icon = ?, description = ?, filters = ?,
		    sort_order = ?, updated_at = strftime('%s','now')
		WHERE id = ?
	`, v.Name, v.Icon, v.Description, string(filtersJSON), v.SortOrder, v.ID)
	return err
}

// DeleteSavedView removes a non-system view. Returns an error if the
// view is system-owned (caller should surface a clear message).
func (db *DB) DeleteSavedView(id int64) error {
	res, err := db.conn.Exec(`DELETE FROM saved_views WHERE id = ? AND is_system = 0`, id)
	if err != nil {
		return fmt.Errorf("delete saved view: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("view not found or system-owned")
	}
	return nil
}

// TouchSavedView updates last_used to now. Called when the operator
// applies a view (so future UI can surface "recently used" ordering).
func (db *DB) TouchSavedView(id int64) error {
	_, err := db.conn.Exec(`UPDATE saved_views SET last_used = strftime('%s','now') WHERE id = ?`, id)
	return err
}

// SeedDefaultSavedViews inserts the built-in starter views if they
// don't already exist. Called once on startup. The names are unique
// so re-runs are idempotent.
func (db *DB) SeedDefaultSavedViews() error {
	defaults := []SavedView{
		{
			Name:        "Orphans (largest first)",
			Icon:        "trash",
			Description: "Files not used by any service, sorted by size descending",
			Filters: map[string]string{
				"orphaned":  "true",
				"order":     "size",
				"direction": "desc",
			},
			SortOrder: 10,
			IsSystem:  true,
		},
		{
			Name:        "Hardlinked groups",
			Icon:        "link",
			Description: "Files that share an inode with at least one other file",
			Filters: map[string]string{
				"hardlink": "true",
			},
			SortOrder: 20,
			IsSystem:  true,
		},
		{
			Name:        ".nfo / metadata files",
			Icon:        "file-text",
			Description: "Sidecar metadata for media (NFO, SRT, etc.) — usually small",
			Filters: map[string]string{
				"extensions": "nfo,srt,sub,idx,jpg,png",
			},
			SortOrder: 30,
			IsSystem:  true,
		},
	}

	for _, v := range defaults {
		filtersJSON, err := json.Marshal(v.Filters)
		if err != nil {
			return fmt.Errorf("marshal filters for %q: %w", v.Name, err)
		}
		// INSERT OR IGNORE keeps re-runs idempotent without overwriting
		// any rename the user may have done.
		if _, err := db.conn.Exec(`
			INSERT OR IGNORE INTO saved_views
				(name, icon, description, filters, sort_order, is_system)
			VALUES (?, ?, ?, ?, ?, 1)
		`, v.Name, v.Icon, v.Description, string(filtersJSON), v.SortOrder); err != nil {
			return fmt.Errorf("seed view %q: %w", v.Name, err)
		}
	}
	return nil
}

// scanSavedViewRow handles both *sql.Row and *sql.Rows scanning so the
// same logic works for single-row and multi-row queries.
func scanSavedViewRow(scanner interface {
	Scan(dest ...interface{}) error
}) (*SavedView, error) {
	v := &SavedView{}
	var filtersJSON string
	var icon, description sql.NullString
	var lastUsed sql.NullInt64
	var createdAt, updatedAt int64
	var isSystem int

	err := scanner.Scan(
		&v.ID, &v.Name, &icon, &description, &filtersJSON,
		&v.SortOrder, &isSystem, &createdAt, &updatedAt, &lastUsed,
	)
	if err != nil {
		return nil, err
	}

	v.Icon = icon.String
	v.Description = description.String
	v.IsSystem = isSystem != 0
	v.CreatedAt = time.Unix(createdAt, 0)
	v.UpdatedAt = time.Unix(updatedAt, 0)
	if lastUsed.Valid {
		t := time.Unix(lastUsed.Int64, 0)
		v.LastUsed = &t
	}

	if filtersJSON == "" {
		v.Filters = map[string]string{}
	} else if err := json.Unmarshal([]byte(filtersJSON), &v.Filters); err != nil {
		// Don't fail the whole list on a single corrupt row — surface
		// an empty filter map and let the operator re-save.
		v.Filters = map[string]string{}
	}
	return v, nil
}
