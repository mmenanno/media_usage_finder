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
	CacheSize       int // SQLite cache size in KB (0 = default)
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

	// Apply SQLite cache size if configured (negative value = KB)
	if cfg.CacheSize > 0 {
		_, err = conn.Exec(fmt.Sprintf("PRAGMA cache_size = -%d", cfg.CacheSize))
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to set cache size: %w", err)
		}
	}

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

	// Migration 6.5: Add hash_level column for progressive hashing
	// Check if hash_level column exists
	var hasHashLevel int
	err = db.conn.QueryRow(`
		SELECT COUNT(*)
		FROM pragma_table_info('files')
		WHERE name='hash_level'
	`).Scan(&hasHashLevel)
	if err != nil {
		return fmt.Errorf("failed to check for hash_level column: %w", err)
	}

	// If hash_level column doesn't exist, add it and populate from hash_type
	if hasHashLevel == 0 {
		_, err = db.conn.Exec(migrateAddHashLevel)
		if err != nil {
			return fmt.Errorf("failed to add hash_level column: %w", err)
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

	// Migration 11: Update audit_log table CHECK constraint to include 'consolidate' and 'hardlink'
	// Test if migration is needed by trying to insert a test record with action='hardlink'
	needsAuditLogMigration := false

	// Start a transaction for the test
	tx5, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction for audit_log migration check: %w", err)
	}
	defer tx5.Rollback()

	// Try to insert a test record with action='hardlink'
	_, err = tx5.Exec(`
		INSERT INTO audit_log (action, entity_type, entity_id, details)
		VALUES ('hardlink', 'test', 0, 'migration test')
	`)

	if err != nil {
		// Check if the error is a CHECK constraint failure
		if strings.Contains(err.Error(), "CHECK constraint failed") {
			needsAuditLogMigration = true
		}
	} else {
		// If insert succeeded, delete the test record
		_, _ = tx5.Exec(`DELETE FROM audit_log WHERE action = 'hardlink' AND details = 'migration test'`)
	}

	// Rollback the test transaction
	tx5.Rollback()

	// Run migration if needed
	if needsAuditLogMigration {
		_, err = db.conn.Exec(migrateAddConsolidateHardlinkToAuditLog)
		if err != nil {
			return fmt.Errorf("failed to update audit_log table CHECK constraint: %w", err)
		}
	}

	// Migration 10: Add deleted_files_count column to scans table if it doesn't exist
	var hasDeletedFilesCount int
	err = db.conn.QueryRow(`
		SELECT COUNT(*)
		FROM pragma_table_info('scans')
		WHERE name = 'deleted_files_count'
	`).Scan(&hasDeletedFilesCount)

	if err != nil {
		return fmt.Errorf("failed to check for deleted_files_count column: %w", err)
	}

	// If deleted_files_count column doesn't exist, add it
	if hasDeletedFilesCount == 0 {
		_, err = db.conn.Exec(migrateAddDeletedFilesCount)
		if err != nil {
			return fmt.Errorf("failed to add deleted_files_count column: %w", err)
		}
	}

	// Migration 11: Update scans table CHECK constraint to include 'cleanup'
	// Test if migration is needed by trying to insert a test record with scan_type='cleanup'
	needsCleanupScanTypeMigration := false

	// Start a transaction for the test
	tx6, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction for cleanup scan_type migration check: %w", err)
	}
	defer tx6.Rollback()

	// Try to insert a test record with scan_type='cleanup'
	_, err = tx6.Exec(`
		INSERT INTO scans (started_at, status, scan_type)
		VALUES (?, 'completed', 'cleanup')
	`, time.Now().Unix())

	if err != nil {
		// Check if the error is a CHECK constraint failure
		if strings.Contains(err.Error(), "CHECK constraint failed") {
			needsCleanupScanTypeMigration = true
		}
	} else {
		// If insert succeeded, delete the test record
		_, _ = tx6.Exec(`DELETE FROM scans WHERE scan_type = 'cleanup'`)
	}

	// Rollback the test transaction
	tx6.Rollback()

	// Run migration if needed
	if needsCleanupScanTypeMigration {
		// Disable foreign key constraints for migration
		_, err = db.conn.Exec("PRAGMA foreign_keys = OFF")
		if err != nil {
			return fmt.Errorf("failed to disable foreign keys for cleanup scan_type migration: %w", err)
		}

		_, err = db.conn.Exec(migrateAddCleanupToScanType)
		if err != nil {
			// Re-enable foreign keys before returning error
			db.conn.Exec("PRAGMA foreign_keys = ON")
			return fmt.Errorf("failed to update scans table CHECK constraint for cleanup: %w", err)
		}

		// Re-enable foreign key constraints
		_, err = db.conn.Exec("PRAGMA foreign_keys = ON")
		if err != nil {
			return fmt.Errorf("failed to re-enable foreign keys after cleanup scan_type migration: %w", err)
		}
	}

	// Migration 12: Add service_missing_files table if it doesn't exist
	var hasServiceMissingFilesTable int
	err = db.conn.QueryRow(`
		SELECT COUNT(*)
		FROM sqlite_master
		WHERE type='table' AND name='service_missing_files'
	`).Scan(&hasServiceMissingFilesTable)

	if err != nil {
		return fmt.Errorf("failed to check for service_missing_files table: %w", err)
	}

	// If service_missing_files table doesn't exist, create it
	if hasServiceMissingFilesTable == 0 {
		_, err = db.conn.Exec(migrateAddServiceMissingFilesTable)
		if err != nil {
			return fmt.Errorf("failed to create service_missing_files table: %w", err)
		}
	}

	// Migration 13: Update audit_log table CHECK constraint to include 'cleanup'
	// Test if migration is needed by trying to insert a test record with action='cleanup'
	needsCleanupAuditLogMigration := false
	tx13, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction for cleanup audit_log migration check: %w", err)
	}

	// Try to insert a test record with action='cleanup'
	_, err = tx13.Exec(`
		INSERT INTO audit_log (action, entity_type, entity_id, details)
		VALUES ('cleanup', 'test', 0, 'migration test')
	`)

	if err != nil {
		// If we get a CHECK constraint error, we need the migration
		if strings.Contains(err.Error(), "CHECK constraint failed") {
			needsCleanupAuditLogMigration = true
		}
	}

	// Clean up test record if it was inserted
	_, _ = tx13.Exec(`DELETE FROM audit_log WHERE action = 'cleanup' AND details = 'migration test'`)
	tx13.Rollback() // Always rollback since this is just a test

	// Run migration if needed
	if needsCleanupAuditLogMigration {
		_, err = db.conn.Exec(migrateAddCleanupToAuditLogAction)
		if err != nil {
			return fmt.Errorf("failed to add cleanup to audit_log action constraint: %w", err)
		}
	}

	// Migration 14: Add scan_id column to audit_log table if it doesn't exist
	var hasScanIdInAuditLog int
	err = db.conn.QueryRow(`
		SELECT COUNT(*)
		FROM pragma_table_info('audit_log')
		WHERE name = 'scan_id'
	`).Scan(&hasScanIdInAuditLog)

	if err != nil {
		return fmt.Errorf("failed to check for scan_id column in audit_log: %w", err)
	}

	// If scan_id column doesn't exist, add it
	if hasScanIdInAuditLog == 0 {
		_, err = db.conn.Exec(migrateAddScanIdToAuditLog)
		if err != nil {
			return fmt.Errorf("failed to add scan_id column to audit_log: %w", err)
		}
	}

	// Migration 15: Update scans table CHECK constraint to include 'file_rescan'
	// Test if migration is needed by trying to insert a test record with scan_type='file_rescan'
	needsFileRescanScanTypeMigration := false

	// Start a transaction for the test
	tx15, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction for file_rescan scan_type migration check: %w", err)
	}
	defer tx15.Rollback()

	// Try to insert a test record with scan_type='file_rescan'
	_, err = tx15.Exec(`
		INSERT INTO scans (started_at, status, scan_type)
		VALUES (?, 'completed', 'file_rescan')
	`, time.Now().Unix())

	if err != nil {
		// Check if the error is a CHECK constraint failure
		if strings.Contains(err.Error(), "CHECK constraint failed") {
			needsFileRescanScanTypeMigration = true
		}
	} else {
		// If insert succeeded, delete the test record
		_, _ = tx15.Exec(`DELETE FROM scans WHERE scan_type = 'file_rescan'`)
	}

	// Rollback the test transaction
	tx15.Rollback()

	// Run migration if needed
	if needsFileRescanScanTypeMigration {
		// Disable foreign key constraints for migration
		_, err = db.conn.Exec("PRAGMA foreign_keys = OFF")
		if err != nil {
			return fmt.Errorf("failed to disable foreign keys for file_rescan scan_type migration: %w", err)
		}

		_, err = db.conn.Exec(migrateAddFileRescanToScanType)
		if err != nil {
			// Re-enable foreign keys before returning error
			db.conn.Exec("PRAGMA foreign_keys = ON")
			return fmt.Errorf("failed to update scans table CHECK constraint for file_rescan: %w", err)
		}

		// Re-enable foreign key constraints
		_, err = db.conn.Exec("PRAGMA foreign_keys = ON")
		if err != nil {
			return fmt.Errorf("failed to re-enable foreign keys after file_rescan scan_type migration: %w", err)
		}
	}

	// Migration 16: Update scans table CHECK constraint to include 'service_update_calibre'
	// Test if migration is needed by trying to insert a test record with scan_type='service_update_calibre'
	needsCalibreServiceUpdateMigration := false

	// Start a transaction for the test
	tx16, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction for service_update_calibre scan_type migration check: %w", err)
	}
	defer tx16.Rollback()

	// Try to insert a test record with scan_type='service_update_calibre'
	_, err = tx16.Exec(`
		INSERT INTO scans (started_at, status, scan_type)
		VALUES (?, 'completed', 'service_update_calibre')
	`, time.Now().Unix())

	if err != nil {
		// Check if the error is a CHECK constraint failure
		if strings.Contains(err.Error(), "CHECK constraint failed") {
			needsCalibreServiceUpdateMigration = true
		}
	} else {
		// If insert succeeded, delete the test record
		_, _ = tx16.Exec(`DELETE FROM scans WHERE scan_type = 'service_update_calibre'`)
	}

	// Rollback the test transaction
	tx16.Rollback()

	// Run migration if needed
	if needsCalibreServiceUpdateMigration {
		// Disable foreign key constraints for migration
		_, err = db.conn.Exec("PRAGMA foreign_keys = OFF")
		if err != nil {
			return fmt.Errorf("failed to disable foreign keys for service_update_calibre scan_type migration: %w", err)
		}

		_, err = db.conn.Exec(migrateAddCalibreServiceUpdateToScanType)
		if err != nil {
			// Re-enable foreign keys before returning error
			db.conn.Exec("PRAGMA foreign_keys = ON")
			return fmt.Errorf("failed to update scans table CHECK constraint for service_update_calibre: %w", err)
		}

		// Re-enable foreign key constraints
		_, err = db.conn.Exec("PRAGMA foreign_keys = ON")
		if err != nil {
			return fmt.Errorf("failed to re-enable foreign keys after service_update_calibre scan_type migration: %w", err)
		}
	}

	// Migration 17: Add 'calibre' to usage and service_missing_files table CHECK constraints
	// Test if migration is needed by trying to insert a test record with service='calibre'
	needsCalibreServiceTablesMigration := false

	// Start a transaction for the test
	tx17, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction for calibre service tables migration check: %w", err)
	}
	defer tx17.Rollback()

	// Try to insert a test record with service='calibre' into usage table
	_, err = tx17.Exec(`
		INSERT INTO usage (file_id, service, reference_path)
		VALUES (999999999, 'calibre', '/migration-test')
	`)

	if err != nil {
		// Check if the error is a CHECK constraint failure
		if strings.Contains(err.Error(), "CHECK constraint failed") {
			needsCalibreServiceTablesMigration = true
		}
		// Foreign key errors are expected and mean no migration is needed
	} else {
		// If insert succeeded, delete the test record
		_, _ = tx17.Exec(`DELETE FROM usage WHERE file_id = 999999999 AND service = 'calibre'`)
	}

	// Rollback the test transaction
	tx17.Rollback()

	// Run migration if needed
	if needsCalibreServiceTablesMigration {
		// Disable foreign key constraints for migration
		_, err = db.conn.Exec("PRAGMA foreign_keys = OFF")
		if err != nil {
			return fmt.Errorf("failed to disable foreign keys for calibre service tables migration: %w", err)
		}

		_, err = db.conn.Exec(migrateAddCalibreToServiceTables)
		if err != nil {
			// Re-enable foreign keys before returning error
			db.conn.Exec("PRAGMA foreign_keys = ON")
			return fmt.Errorf("failed to update usage and service_missing_files tables for calibre: %w", err)
		}

		// Re-enable foreign key constraints
		_, err = db.conn.Exec("PRAGMA foreign_keys = ON")
		if err != nil {
			return fmt.Errorf("failed to re-enable foreign keys after calibre service tables migration: %w", err)
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
