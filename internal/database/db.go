package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
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
	// Check if scan_id column is NOT NULL (needs migration)
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
