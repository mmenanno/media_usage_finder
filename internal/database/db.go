package database

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// DB wraps the SQLite database connection
type DB struct {
	conn *sql.DB
}

// New creates a new database connection and initializes the schema
func New(dbPath string) (*DB, error) {
	conn, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set connection pool settings
	conn.SetMaxOpenConns(25)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(5 * time.Minute)

	db := &DB{conn: conn}

	// Initialize schema
	if err := db.initSchema(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return db, nil
}

// initSchema creates all tables and indexes
func (db *DB) initSchema() error {
	_, err := db.conn.Exec(GetSchema())
	if err != nil {
		return fmt.Errorf("failed to execute schema: %w", err)
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
