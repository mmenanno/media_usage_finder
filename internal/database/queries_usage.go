package database

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

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
