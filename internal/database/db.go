package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// DB wraps the SQLite database connection
type DB struct {
	conn *sql.DB
}

// DBConfig holds database connection configuration
type DBConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// New creates a new database connection and initializes the schema
func New(dbPath string) (*DB, error) {
	return NewWithConfig(dbPath, DBConfig{
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	})
}

// NewWithConfig creates a new database connection with custom pool settings
func NewWithConfig(dbPath string, cfg DBConfig) (*DB, error) {
	// Ensure the database directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	conn, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set connection pool settings from config
	conn.SetMaxOpenConns(cfg.MaxOpenConns)
	conn.SetMaxIdleConns(cfg.MaxIdleConns)
	conn.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	db := &DB{conn: conn}

	// Initialize schema
	if err := db.initSchema(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Clean up any orphaned running scans from previous app instances
	// This marks all scans with status='running' as 'interrupted' since
	// if the app is starting up, no scan can actually be running
	if _, err := db.CleanStaleScansOnStartup(); err != nil {
		// Log but don't fail startup
		fmt.Printf("Warning: failed to clean orphaned scans on startup: %v\n", err)
	}

	return db, nil
}

// initSchema creates all tables and indexes
func (db *DB) initSchema() error {
	_, err := db.conn.Exec(GetSchema())
	if err != nil {
		return fmt.Errorf("failed to execute schema: %w", err)
	}

	// Run migrations
	if err := db.runMigrations(); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

// runMigrations applies database migrations
func (db *DB) runMigrations() error {
	// Migration 1: Check if scan_id column is NOT NULL (needs migration)
	var notNull int
	err := db.conn.QueryRow(`
		SELECT "notnull"
		FROM pragma_table_info('files')
		WHERE name = 'scan_id'
	`).Scan(&notNull)

	if err != nil {
		// If there's an error, it might mean the column doesn't exist yet
		// or the table is brand new, so we can skip migration
		return nil
	}

	// If scan_id is NOT NULL (notNull == 1), we need to run the migration
	if notNull == 1 {
		_, err = db.conn.Exec(migrateScanIdNullable)
		if err != nil {
			return fmt.Errorf("failed to migrate scan_id to nullable: %w", err)
		}
	}

	// Migration 2: Add current_phase column to scans table if it doesn't exist
	var hasCurrentPhase int
	err = db.conn.QueryRow(`
		SELECT COUNT(*)
		FROM pragma_table_info('scans')
		WHERE name = 'current_phase'
	`).Scan(&hasCurrentPhase)

	if err != nil {
		return fmt.Errorf("failed to check for current_phase column: %w", err)
	}

	// If current_phase column doesn't exist, add it
	if hasCurrentPhase == 0 {
		_, err = db.conn.Exec(migrateAddCurrentPhase)
		if err != nil {
			return fmt.Errorf("failed to add current_phase column: %w", err)
		}
	}

	// Migration 3: Add resume tracking columns to scans table if they don't exist
	var hasLastProcessedPath int
	err = db.conn.QueryRow(`
		SELECT COUNT(*)
		FROM pragma_table_info('scans')
		WHERE name = 'last_processed_path'
	`).Scan(&hasLastProcessedPath)

	if err != nil {
		return fmt.Errorf("failed to check for last_processed_path column: %w", err)
	}

	// If last_processed_path column doesn't exist, add both resume tracking columns
	if hasLastProcessedPath == 0 {
		_, err = db.conn.Exec(migrateAddResumeTracking)
		if err != nil {
			return fmt.Errorf("failed to add resume tracking columns: %w", err)
		}
	}

	// Migration 4: Update usage table CHECK constraint to include 'stash'
	// We detect if migration is needed by trying to insert a test record with service='stash'
	// If it fails with CHECK constraint error, we need to run the migration
	needsUsageMigration := false

	// Start a transaction for the test
	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction for usage migration check: %w", err)
	}
	defer tx.Rollback() // Will be a no-op if we commit

	// Try to insert a test record with service='stash'
	// Use a file_id that's unlikely to exist (999999999)
	_, err = tx.Exec(`
		INSERT INTO usage (file_id, service, reference_path)
		VALUES (999999999, 'stash', '/migration-test')
	`)

	if err != nil {
		// Check if the error is a CHECK constraint failure
		if strings.Contains(err.Error(), "CHECK constraint failed") {
			needsUsageMigration = true
		}
		// Foreign key errors are expected and mean no migration is needed
	} else {
		// If insert succeeded, delete the test record
		_, _ = tx.Exec(`DELETE FROM usage WHERE file_id = 999999999 AND service = 'stash'`)
	}

	// Rollback the test transaction
	tx.Rollback()

	// Run migration if needed
	if needsUsageMigration {
		_, err = db.conn.Exec(migrateAddStashToUsageCheck)
		if err != nil {
			return fmt.Errorf("failed to update usage table CHECK constraint: %w", err)
		}
	}

	// Migration 5: Add extension column to files table if it doesn't exist
	var hasExtension int
	err = db.conn.QueryRow(`
		SELECT COUNT(*)
		FROM pragma_table_info('files')
		WHERE name = 'extension'
	`).Scan(&hasExtension)

	if err != nil {
		return fmt.Errorf("failed to check for extension column: %w", err)
	}

	// If extension column doesn't exist, add it
	if hasExtension == 0 {
		_, err = db.conn.Exec(migrateAddExtensionColumn)
		if err != nil {
			return fmt.Errorf("failed to add extension column: %w", err)
		}
	}

	// Migration 6: Add hash columns for duplicate detection if they don't exist
	var hasFileHash int
	err = db.conn.QueryRow(`
		SELECT COUNT(*)
		FROM pragma_table_info('files')
		WHERE name = 'file_hash'
	`).Scan(&hasFileHash)

	if err != nil {
		return fmt.Errorf("failed to check for file_hash column: %w", err)
	}

	// If file_hash column doesn't exist, add all hash columns
	if hasFileHash == 0 {
		_, err = db.conn.Exec(migrateAddHashColumns)
		if err != nil {
			return fmt.Errorf("failed to add hash columns: %w", err)
		}
	}

	// Migration 7: Update scans table CHECK constraint to include 'disk_location'
	// Test if migration is needed by trying to insert a test record with scan_type='disk_location'
	needsScanTypeMigration := false

	// Start a transaction for the test
	tx2, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction for scan_type migration check: %w", err)
	}
	defer tx2.Rollback()

	// Try to insert a test record with scan_type='disk_location'
	_, err = tx2.Exec(`
		INSERT INTO scans (started_at, status, scan_type)
		VALUES (?, 'completed', 'disk_location')
	`, time.Now().Unix())

	if err != nil {
		// Check if the error is a CHECK constraint failure
		if strings.Contains(err.Error(), "CHECK constraint failed") {
			needsScanTypeMigration = true
		}
	} else {
		// If insert succeeded, delete the test record
		_, _ = tx2.Exec(`DELETE FROM scans WHERE scan_type = 'disk_location'`)
	}

	// Rollback the test transaction
	tx2.Rollback()

	// Run migration if needed
	if needsScanTypeMigration {
		// Disable foreign key constraints for migration
		_, err = db.conn.Exec("PRAGMA foreign_keys = OFF")
		if err != nil {
			return fmt.Errorf("failed to disable foreign keys: %w", err)
		}

		// Run the migration
		_, err = db.conn.Exec(migrateAddDiskLocationToScanType)

		// Re-enable foreign key constraints
		_, fkErr := db.conn.Exec("PRAGMA foreign_keys = ON")

		// Check migration error first
		if err != nil {
			return fmt.Errorf("failed to update scans table CHECK constraint: %w", err)
		}

		// Then check if we could re-enable foreign keys
		if fkErr != nil {
			return fmt.Errorf("failed to re-enable foreign keys: %w", fkErr)
		}
	}

	// Migration 8: Update scans table CHECK constraint to include service_update scan types
	// Test if migration is needed by trying to insert a test record with scan_type='service_update_all'
	needsServiceUpdateMigration := false

	// Start a transaction for the test
	tx3, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction for service_update migration check: %w", err)
	}
	defer tx3.Rollback()

	// Try to insert a test record with scan_type='service_update_all'
	_, err = tx3.Exec(`
		INSERT INTO scans (started_at, status, scan_type)
		VALUES (?, 'completed', 'service_update_all')
	`, time.Now().Unix())

	if err != nil {
		// Check if the error is a CHECK constraint failure
		if strings.Contains(err.Error(), "CHECK constraint failed") {
			needsServiceUpdateMigration = true
		}
	} else {
		// If insert succeeded, delete the test record
		_, _ = tx3.Exec(`DELETE FROM scans WHERE scan_type = 'service_update_all'`)
	}

	// Rollback the test transaction
	tx3.Rollback()

	// Run migration if needed
	if needsServiceUpdateMigration {
		// Disable foreign key constraints for migration
		_, err = db.conn.Exec("PRAGMA foreign_keys = OFF")
		if err != nil {
			return fmt.Errorf("failed to disable foreign keys: %w", err)
		}

		// Run the migration
		_, err = db.conn.Exec(migrateAddServiceUpdateToScanType)

		// Re-enable foreign key constraints
		_, fkErr := db.conn.Exec("PRAGMA foreign_keys = ON")

		// Check migration error first
		if err != nil {
			return fmt.Errorf("failed to update scans table CHECK constraint for service updates: %w", err)
		}

		// Then check if we could re-enable foreign keys
		if fkErr != nil {
			return fmt.Errorf("failed to re-enable foreign keys: %w", fkErr)
		}
	}

	// Migration 9: Update scans table CHECK constraint to include hash_scan scan type
	// Test if migration is needed by trying to insert a test record with scan_type='hash_scan'
	needsHashScanMigration := false

	// Start a transaction for the test
	tx4, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction for hash_scan migration check: %w", err)
	}
	defer tx4.Rollback()

	// Try to insert a test record with scan_type='hash_scan'
	_, err = tx4.Exec(`
		INSERT INTO scans (started_at, status, scan_type)
		VALUES (?, 'completed', 'hash_scan')
	`, time.Now().Unix())

	if err != nil {
		// Check if the error is a CHECK constraint failure
		if strings.Contains(err.Error(), "CHECK constraint failed") {
			needsHashScanMigration = true
		}
	} else {
		// If insert succeeded, delete the test record
		_, _ = tx4.Exec(`DELETE FROM scans WHERE scan_type = 'hash_scan'`)
	}

	// Rollback the test transaction
	tx4.Rollback()

	// Run migration if needed
	if needsHashScanMigration {
		// Disable foreign key constraints for migration
		_, err = db.conn.Exec("PRAGMA foreign_keys = OFF")
		if err != nil {
			return fmt.Errorf("failed to disable foreign keys: %w", err)
		}

		// Run the migration
		_, err = db.conn.Exec(migrateAddHashScanToScanType)

		// Re-enable foreign key constraints
		_, fkErr := db.conn.Exec("PRAGMA foreign_keys = ON")

		// Check migration error first
		if err != nil {
			return fmt.Errorf("failed to update scans table CHECK constraint for hash_scan: %w", err)
		}

		// Then check if we could re-enable foreign keys
		if fkErr != nil {
			return fmt.Errorf("failed to re-enable foreign keys: %w", fkErr)
		}
	}

	// Migration 10: Add hash_type column if it's missing (for servers that had partial Migration 6)
	var hasHashType int
	err = db.conn.QueryRow(`
		SELECT COUNT(*)
		FROM pragma_table_info('files')
		WHERE name = 'hash_type'
	`).Scan(&hasHashType)

	if err != nil {
		return fmt.Errorf("failed to check for hash_type column: %w", err)
	}

	// If hash_type column doesn't exist, add it and the index
	if hasHashType == 0 {
		_, err = db.conn.Exec(`
			-- Add hash_type column to files table ('quick' or 'full')
			ALTER TABLE files ADD COLUMN hash_type TEXT DEFAULT NULL;

			-- Create index for finding files with quick hashes (for verification)
			CREATE INDEX IF NOT EXISTS idx_files_quick_hash ON files(hash_type) WHERE hash_type = 'quick';
		`)
		if err != nil {
			return fmt.Errorf("failed to add hash_type column: %w", err)
		}
	}

	return nil
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.conn.Close()
}

// Conn returns the underlying sql.DB connection
func (db *DB) Conn() *sql.DB {
	return db.conn
}

// BeginTx starts a new transaction
func (db *DB) BeginTx() (*sql.Tx, error) {
	return db.conn.Begin()
}

// Ping checks if the database connection is alive
func (db *DB) Ping() error {
	return db.conn.Ping()
}
