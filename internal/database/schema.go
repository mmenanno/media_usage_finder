package database

const schema = `
-- Files table
CREATE TABLE IF NOT EXISTS files (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	path TEXT NOT NULL UNIQUE,
	size INTEGER NOT NULL,
	inode INTEGER NOT NULL,
	device_id INTEGER NOT NULL,
	modified_time INTEGER NOT NULL,
	scan_id INTEGER,
	last_verified INTEGER NOT NULL,
	is_orphaned INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (scan_id) REFERENCES scans(id)
);

-- Performance indexes for files table
-- Note: path has UNIQUE constraint which creates index automatically
CREATE INDEX IF NOT EXISTS idx_files_hardlink ON files(device_id, inode);
CREATE INDEX IF NOT EXISTS idx_files_size ON files(size);
CREATE INDEX IF NOT EXISTS idx_files_modified_time ON files(modified_time);
CREATE INDEX IF NOT EXISTS idx_files_last_verified ON files(last_verified);
CREATE INDEX IF NOT EXISTS idx_files_is_orphaned ON files(is_orphaned);
CREATE INDEX IF NOT EXISTS idx_files_scan_id ON files(scan_id);
-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_files_orphaned_size ON files(is_orphaned, size);
CREATE INDEX IF NOT EXISTS idx_files_scan_verified ON files(scan_id, last_verified);

-- Full-text search virtual table for file paths
CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
	path,
	content='files',
	content_rowid='id'
);

-- Triggers to keep FTS table in sync
CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
	INSERT INTO files_fts(rowid, path) VALUES (new.id, new.path);
END;

CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
	INSERT INTO files_fts(files_fts, rowid, path) VALUES('delete', old.id, old.path);
END;

CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
	INSERT INTO files_fts(files_fts, rowid, path) VALUES('delete', old.id, old.path);
	INSERT INTO files_fts(rowid, path) VALUES (new.id, new.path);
END;

-- Usage table to track which services use each file
CREATE TABLE IF NOT EXISTS usage (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	file_id INTEGER NOT NULL,
	service TEXT NOT NULL CHECK(service IN ('plex', 'sonarr', 'radarr', 'qbittorrent')),
	reference_path TEXT NOT NULL,
	metadata TEXT,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
	UNIQUE(file_id, service)
);

CREATE INDEX IF NOT EXISTS idx_usage_file_id ON usage(file_id);
CREATE INDEX IF NOT EXISTS idx_usage_service ON usage(service);
CREATE INDEX IF NOT EXISTS idx_usage_reference_path ON usage(reference_path);

-- Scans table to track scan history
CREATE TABLE IF NOT EXISTS scans (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	started_at INTEGER NOT NULL,
	completed_at INTEGER,
	status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'interrupted')),
	files_scanned INTEGER NOT NULL DEFAULT 0,
	errors TEXT,
	scan_type TEXT NOT NULL DEFAULT 'full' CHECK(scan_type IN ('full', 'incremental')),
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_scans_status ON scans(status);
CREATE INDEX IF NOT EXISTS idx_scans_started_at ON scans(started_at);

-- Config table for storing configuration as key-value pairs
CREATE TABLE IF NOT EXISTS config (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL,
	updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

-- Audit log for tracking deletions and modifications
CREATE TABLE IF NOT EXISTS audit_log (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	action TEXT NOT NULL CHECK(action IN ('delete', 'mark_rescan', 'config_change')),
	entity_type TEXT NOT NULL,
	entity_id INTEGER,
	details TEXT,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action);
`

// GetSchema returns the database schema
func GetSchema() string {
	return schema
}

// Migration to make scan_id nullable in files table
const migrateScanIdNullable = `
-- Create new files table with nullable scan_id
CREATE TABLE IF NOT EXISTS files_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	path TEXT NOT NULL UNIQUE,
	size INTEGER NOT NULL,
	inode INTEGER NOT NULL,
	device_id INTEGER NOT NULL,
	modified_time INTEGER NOT NULL,
	scan_id INTEGER,
	last_verified INTEGER NOT NULL,
	is_orphaned INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (scan_id) REFERENCES scans(id)
);

-- Copy data from old table
INSERT INTO files_new SELECT * FROM files;

-- Drop old table and indexes
DROP TABLE files;

-- Rename new table
ALTER TABLE files_new RENAME TO files;

-- Recreate indexes
CREATE INDEX idx_files_hardlink ON files(device_id, inode);
CREATE INDEX idx_files_size ON files(size);
CREATE INDEX idx_files_modified_time ON files(modified_time);
CREATE INDEX idx_files_last_verified ON files(last_verified);
CREATE INDEX idx_files_is_orphaned ON files(is_orphaned);
CREATE INDEX idx_files_scan_id ON files(scan_id);
CREATE INDEX idx_files_orphaned_size ON files(is_orphaned, size);
CREATE INDEX idx_files_scan_verified ON files(scan_id, last_verified);

-- Recreate FTS table
DROP TABLE IF EXISTS files_fts;
CREATE VIRTUAL TABLE files_fts USING fts5(
	path,
	content='files',
	content_rowid='id'
);

-- Populate FTS table
INSERT INTO files_fts(rowid, path) SELECT id, path FROM files;

-- Recreate triggers
DROP TRIGGER IF EXISTS files_ai;
CREATE TRIGGER files_ai AFTER INSERT ON files BEGIN
	INSERT INTO files_fts(rowid, path) VALUES (new.id, new.path);
END;

DROP TRIGGER IF EXISTS files_ad;
CREATE TRIGGER files_ad AFTER DELETE ON files BEGIN
	INSERT INTO files_fts(files_fts, rowid, path) VALUES('delete', old.id, old.path);
END;

DROP TRIGGER IF EXISTS files_au;
CREATE TRIGGER files_au AFTER UPDATE ON files BEGIN
	INSERT INTO files_fts(files_fts, rowid, path) VALUES('delete', old.id, old.path);
	INSERT INTO files_fts(rowid, path) VALUES (new.id, new.path);
END;
`
