package database

import (
	"database/sql"
	"fmt"
	"time"
)

// FreshnessSnapshot summarizes how recently each subsystem was updated.
// Used by the dashboard to surface stale data (e.g., a service whose
// usage hasn't been refreshed in days, or files missing hash coverage).
//
// All time fields are pointers so a never-run subsystem can be
// distinguished from one that ran at unix-epoch zero.
type FreshnessSnapshot struct {
	// Most recent successful full or incremental filesystem scan.
	LastFilesystemScan *time.Time
	// Most recent disk-location scan completion.
	LastDiskScan *time.Time
	// Most recent service usage update, per service name. Missing key
	// means that service has never recorded usage rows.
	LastUsagePerService map[string]*time.Time
	// Hash coverage at the time of the snapshot.
	TotalFiles      int64
	HashedFiles     int64
	MissingHashes   int64
	HashCoveragePct float64
}

// GetFreshness builds the freshness snapshot in a few cheap queries.
// Called from /api/freshness — runs each time the dashboard loads, so
// keep the queries indexed and bounded.
func (db *DB) GetFreshness() (*FreshnessSnapshot, error) {
	snap := &FreshnessSnapshot{
		LastUsagePerService: make(map[string]*time.Time),
	}

	if t, err := db.scanLatestUnix(`
		SELECT MAX(completed_at)
		FROM scans
		WHERE status IN ('completed', 'completed_with_errors')
		  AND scan_type IN ('full', 'incremental')
	`); err != nil {
		return nil, fmt.Errorf("last filesystem scan: %w", err)
	} else {
		snap.LastFilesystemScan = t
	}

	if t, err := db.scanLatestUnix(`
		SELECT MAX(completed_at)
		FROM scans
		WHERE status IN ('completed', 'completed_with_errors')
		  AND scan_type = 'disk_location'
	`); err != nil {
		return nil, fmt.Errorf("last disk scan: %w", err)
	} else {
		snap.LastDiskScan = t
	}

	rows, err := db.conn.Query(`
		SELECT service, MAX(updated_at)
		FROM usage
		GROUP BY service
	`)
	if err != nil {
		return nil, fmt.Errorf("usage timestamps: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var service string
		var ts sql.NullInt64
		if err := rows.Scan(&service, &ts); err != nil {
			return nil, fmt.Errorf("scan service row: %w", err)
		}
		if ts.Valid {
			t := time.Unix(ts.Int64, 0)
			snap.LastUsagePerService[service] = &t
		} else {
			snap.LastUsagePerService[service] = nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("usage rows: %w", err)
	}

	// Hash coverage: the hash column is `file_hash`, added by an
	// ALTER in schema.go (see "Migration to add hash columns").
	if err := db.conn.QueryRow(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN file_hash IS NOT NULL AND file_hash != '' THEN 1 ELSE 0 END), 0)
		FROM files
	`).Scan(&snap.TotalFiles, &snap.HashedFiles); err != nil {
		return nil, fmt.Errorf("hash coverage: %w", err)
	}
	snap.MissingHashes = snap.TotalFiles - snap.HashedFiles
	if snap.TotalFiles > 0 {
		snap.HashCoveragePct = 100 * float64(snap.HashedFiles) / float64(snap.TotalFiles)
	}

	return snap, nil
}

// scanLatestUnix runs a query that returns a single MAX(...) unix
// timestamp and returns it as *time.Time (nil for NULL results). Used
// for "most recent X" lookups in GetFreshness.
func (db *DB) scanLatestUnix(query string, args ...interface{}) (*time.Time, error) {
	var ts sql.NullInt64
	if err := db.conn.QueryRow(query, args...).Scan(&ts); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if !ts.Valid {
		return nil, nil
	}
	t := time.Unix(ts.Int64, 0)
	return &t, nil
}
