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
	service TEXT NOT NULL CHECK(service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash', 'calibre')),
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
	status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'interrupted', 'completed_with_errors')),
	files_scanned INTEGER NOT NULL DEFAULT 0,
	errors TEXT,
	scan_type TEXT NOT NULL DEFAULT 'full' CHECK(scan_type IN ('full', 'incremental', 'disk_location', 'service_update_all', 'service_update_plex', 'service_update_sonarr', 'service_update_radarr', 'service_update_qbittorrent', 'service_update_stash', 'service_update_calibre', 'hash_scan', 'cleanup')),
	current_phase TEXT,
	last_processed_path TEXT,
	resume_from_scan_id INTEGER,
	deleted_files_count INTEGER DEFAULT 0,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (resume_from_scan_id) REFERENCES scans(id)
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
	action TEXT NOT NULL CHECK(action IN ('delete', 'mark_rescan', 'config_change', 'consolidate', 'hardlink', 'cleanup', 'delete_failed')),
	entity_type TEXT NOT NULL,
	entity_id INTEGER,
	details TEXT,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action);

-- Scan logs table for persistent logging of scan activity
CREATE TABLE IF NOT EXISTS scan_logs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	scan_id INTEGER NOT NULL,
	timestamp INTEGER NOT NULL,
	level TEXT NOT NULL CHECK(level IN ('info', 'warning', 'error', 'debug')),
	phase TEXT,
	message TEXT NOT NULL,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_scan_logs_scan_id ON scan_logs(scan_id);
CREATE INDEX IF NOT EXISTS idx_scan_logs_timestamp ON scan_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_scan_logs_level ON scan_logs(level);
CREATE INDEX IF NOT EXISTS idx_scan_logs_scan_timestamp ON scan_logs(scan_id, timestamp);

-- File disk locations table for tracking files on specific disks (Unraid support)
-- This table maps canonical FUSE paths (files.path) to disk-specific paths
-- Enables cross-disk duplicate detection while maintaining service path matching
CREATE TABLE IF NOT EXISTS file_disk_locations (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	file_id INTEGER NOT NULL,
	disk_name TEXT NOT NULL,
	disk_device_id INTEGER NOT NULL,
	disk_path TEXT NOT NULL,
	size INTEGER NOT NULL,
	inode INTEGER NOT NULL,
	modified_time INTEGER NOT NULL,
	last_verified INTEGER NOT NULL,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
	UNIQUE(file_id, disk_device_id)
);

CREATE INDEX IF NOT EXISTS idx_disk_locations_file_id ON file_disk_locations(file_id);
CREATE INDEX IF NOT EXISTS idx_disk_locations_disk_device ON file_disk_locations(disk_device_id);
CREATE INDEX IF NOT EXISTS idx_disk_locations_disk_path ON file_disk_locations(disk_path);
CREATE INDEX IF NOT EXISTS idx_disk_locations_inode ON file_disk_locations(disk_device_id, inode);

-- Service missing files table for tracking files services report but don't exist in filesystem
-- Cleared at each scan start to only show current scan's missing files
CREATE TABLE IF NOT EXISTS service_missing_files (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	scan_id INTEGER NOT NULL,
	service TEXT NOT NULL CHECK(service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash', 'calibre')),
	service_path TEXT NOT NULL,
	translated_path TEXT NOT NULL,
	size INTEGER,
	service_group TEXT,
	service_group_id TEXT,
	metadata TEXT,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (scan_id) REFERENCES scans(id)
);

CREATE INDEX IF NOT EXISTS idx_missing_files_scan_id ON service_missing_files(scan_id);
CREATE INDEX IF NOT EXISTS idx_missing_files_service ON service_missing_files(service);
CREATE INDEX IF NOT EXISTS idx_missing_files_size ON service_missing_files(size DESC);
CREATE INDEX IF NOT EXISTS idx_missing_files_scan_service ON service_missing_files(scan_id, service);
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

// Migration to add current_phase column to scans table
const migrateAddCurrentPhase = `
-- Add current_phase column to scans table
ALTER TABLE scans ADD COLUMN current_phase TEXT;
`

// Migration to add resume tracking columns to scans table
const migrateAddResumeTracking = `
-- Add last_processed_path column to scans table
ALTER TABLE scans ADD COLUMN last_processed_path TEXT;

-- Add resume_from_scan_id column to scans table
ALTER TABLE scans ADD COLUMN resume_from_scan_id INTEGER REFERENCES scans(id);
`

// Migration to update usage table CHECK constraint to include 'stash'
const migrateAddStashToUsageCheck = `
-- Create new usage table with updated CHECK constraint
CREATE TABLE IF NOT EXISTS usage_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	file_id INTEGER NOT NULL,
	service TEXT NOT NULL CHECK(service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash', 'calibre')),
	reference_path TEXT NOT NULL,
	metadata TEXT,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
	UNIQUE(file_id, service)
);

-- Copy data from old table
INSERT INTO usage_new SELECT * FROM usage;

-- Drop old table and indexes
DROP INDEX IF EXISTS idx_usage_file_id;
DROP INDEX IF EXISTS idx_usage_service;
DROP INDEX IF EXISTS idx_usage_reference_path;
DROP TABLE usage;

-- Rename new table
ALTER TABLE usage_new RENAME TO usage;

-- Recreate indexes
CREATE INDEX idx_usage_file_id ON usage(file_id);
CREATE INDEX idx_usage_service ON usage(service);
CREATE INDEX idx_usage_reference_path ON usage(reference_path);
`

// Migration to add extension column to files table
const migrateAddExtensionColumn = `
-- Add extension column to files table
ALTER TABLE files ADD COLUMN extension TEXT DEFAULT '';

-- Create index on extension for fast filtering
CREATE INDEX IF NOT EXISTS idx_files_extension ON files(extension);

-- Create composite index for orphaned files by extension (common query pattern)
CREATE INDEX IF NOT EXISTS idx_files_orphaned_extension ON files(is_orphaned, extension);
`

// Migration to add hash columns for duplicate detection
const migrateAddHashColumns = `
-- Add file_hash column to files table
ALTER TABLE files ADD COLUMN file_hash TEXT DEFAULT NULL;

-- Add hash_algorithm column to files table
ALTER TABLE files ADD COLUMN hash_algorithm TEXT DEFAULT NULL;

-- Add hash_calculated flag to files table
ALTER TABLE files ADD COLUMN hash_calculated INTEGER DEFAULT 0;

-- Add hash_type column to files table ('quick' or 'full')
ALTER TABLE files ADD COLUMN hash_type TEXT DEFAULT NULL;

-- Create index on file_hash for duplicate detection
CREATE INDEX IF NOT EXISTS idx_files_hash ON files(file_hash) WHERE file_hash IS NOT NULL;

-- Create composite index for duplicate candidates (same size + hash)
CREATE INDEX IF NOT EXISTS idx_files_duplicate_candidates ON files(size, file_hash) WHERE file_hash IS NOT NULL;

-- Create index for finding files that need hashing
CREATE INDEX IF NOT EXISTS idx_files_needs_hash ON files(hash_calculated, size) WHERE hash_calculated = 0;

-- Create composite index for cross-disk duplicate detection (hash + device_id)
CREATE INDEX IF NOT EXISTS idx_files_hash_device ON files(file_hash, device_id) WHERE file_hash IS NOT NULL;

-- Create optimized index for duplicate detection queries (includes hash_calculated filter)
CREATE INDEX IF NOT EXISTS idx_files_duplicate_detection ON files(hash_calculated, file_hash, device_id)
  WHERE hash_calculated = 1 AND file_hash IS NOT NULL;

-- Create index for finding files with quick hashes (for verification)
CREATE INDEX IF NOT EXISTS idx_files_quick_hash ON files(hash_type) WHERE hash_type = 'quick';
`

// Migration to add hash_level column for progressive hashing
const migrateAddHashLevel = `
-- Add hash_level column to files table
-- Levels: 0=no hash, 1=1MB quick, 2=10MB, 3=100MB, 4=1GB, 5=10GB, 6=full
ALTER TABLE files ADD COLUMN hash_level INTEGER DEFAULT 0;

-- Populate hash_level from existing hash_type values
-- quick = level 1, full = level 6
UPDATE files
SET hash_level = CASE
    WHEN hash_type = 'quick' THEN 1
    WHEN hash_type = 'full' THEN 6
    ELSE 0
END
WHERE hash_calculated = 1 AND file_hash IS NOT NULL;

-- Create index for hash level queries (progressive verification)
CREATE INDEX IF NOT EXISTS idx_files_hash_level ON files(hash_level, file_hash)
  WHERE hash_calculated = 1 AND file_hash IS NOT NULL;

-- Create composite index for finding duplicates at specific level
CREATE INDEX IF NOT EXISTS idx_files_level_duplicates ON files(hash_level, file_hash, size)
  WHERE hash_calculated = 1 AND file_hash IS NOT NULL;
`

// Migration to add 'disk_location' to scans table CHECK constraint
const migrateAddDiskLocationToScanType = `
-- Drop scans_new if it exists from a previous failed migration
DROP TABLE IF EXISTS scans_new;

-- Create new scans table with updated CHECK constraint
CREATE TABLE scans_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	started_at INTEGER NOT NULL,
	completed_at INTEGER,
	status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'interrupted')),
	files_scanned INTEGER NOT NULL DEFAULT 0,
	errors TEXT,
	scan_type TEXT NOT NULL DEFAULT 'full' CHECK(scan_type IN ('full', 'incremental', 'disk_location')),
	current_phase TEXT,
	last_processed_path TEXT,
	resume_from_scan_id INTEGER,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (resume_from_scan_id) REFERENCES scans(id)
);

-- Copy data from old table (use COALESCE to handle NULL created_at values)
INSERT INTO scans_new (id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, created_at)
SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, COALESCE(created_at, started_at)
FROM scans;

-- Drop old table and indexes
DROP INDEX IF EXISTS idx_scans_status;
DROP INDEX IF EXISTS idx_scans_started_at;
DROP TABLE scans;

-- Rename new table
ALTER TABLE scans_new RENAME TO scans;

-- Recreate indexes
CREATE INDEX idx_scans_status ON scans(status);
CREATE INDEX idx_scans_started_at ON scans(started_at);
`

// Migration to add service_update scan types to scans table CHECK constraint
const migrateAddServiceUpdateToScanType = `
-- Drop scans_new if it exists from a previous failed migration
DROP TABLE IF EXISTS scans_new;

-- Create new scans table with updated CHECK constraint
CREATE TABLE scans_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	started_at INTEGER NOT NULL,
	completed_at INTEGER,
	status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'interrupted', 'completed_with_errors')),
	files_scanned INTEGER NOT NULL DEFAULT 0,
	errors TEXT,
	scan_type TEXT NOT NULL DEFAULT 'full' CHECK(scan_type IN ('full', 'incremental', 'disk_location', 'service_update_all', 'service_update_plex', 'service_update_sonarr', 'service_update_radarr', 'service_update_qbittorrent', 'service_update_stash', 'service_update_calibre')),
	current_phase TEXT,
	last_processed_path TEXT,
	resume_from_scan_id INTEGER,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (resume_from_scan_id) REFERENCES scans(id)
);

-- Copy data from old table (use COALESCE to handle NULL created_at values)
INSERT INTO scans_new (id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, created_at)
SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, COALESCE(created_at, started_at)
FROM scans;

-- Drop old table and indexes
DROP INDEX IF EXISTS idx_scans_status;
DROP INDEX IF EXISTS idx_scans_started_at;
DROP TABLE scans;

-- Rename new table
ALTER TABLE scans_new RENAME TO scans;

-- Recreate indexes
CREATE INDEX idx_scans_status ON scans(status);
CREATE INDEX idx_scans_started_at ON scans(started_at);
`

// Migration to add hash_scan scan type to scans table CHECK constraint
const migrateAddHashScanToScanType = `
-- Drop scans_new if it exists from a previous failed migration
DROP TABLE IF EXISTS scans_new;

-- Create new scans table with updated CHECK constraint
CREATE TABLE scans_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	started_at INTEGER NOT NULL,
	completed_at INTEGER,
	status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'interrupted', 'completed_with_errors')),
	files_scanned INTEGER NOT NULL DEFAULT 0,
	errors TEXT,
	scan_type TEXT NOT NULL DEFAULT 'full' CHECK(scan_type IN ('full', 'incremental', 'disk_location', 'service_update_all', 'service_update_plex', 'service_update_sonarr', 'service_update_radarr', 'service_update_qbittorrent', 'service_update_stash', 'service_update_calibre', 'hash_scan')),
	current_phase TEXT,
	last_processed_path TEXT,
	resume_from_scan_id INTEGER,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (resume_from_scan_id) REFERENCES scans(id)
);

-- Copy data from old table (use COALESCE to handle NULL created_at values)
INSERT INTO scans_new (id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, created_at)
SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, COALESCE(created_at, started_at)
FROM scans;

-- Drop old table and indexes
DROP INDEX IF EXISTS idx_scans_status;
DROP INDEX IF EXISTS idx_scans_started_at;
DROP TABLE scans;

-- Rename new table
ALTER TABLE scans_new RENAME TO scans;

-- Recreate indexes
CREATE INDEX idx_scans_status ON scans(status);
CREATE INDEX idx_scans_started_at ON scans(started_at);
`

// Migration to add 'consolidate' and 'hardlink' to audit_log action CHECK constraint
const migrateAddConsolidateHardlinkToAuditLog = `
-- Drop audit_log_new if it exists from a previous failed migration
DROP TABLE IF EXISTS audit_log_new;

-- Create new audit_log table with updated CHECK constraint
CREATE TABLE audit_log_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	action TEXT NOT NULL CHECK(action IN ('delete', 'mark_rescan', 'config_change', 'consolidate', 'hardlink', 'cleanup')),
	entity_type TEXT NOT NULL,
	entity_id INTEGER,
	details TEXT,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

-- Copy data from old table
INSERT INTO audit_log_new (id, action, entity_type, entity_id, details, created_at)
SELECT id, action, entity_type, entity_id, details, created_at
FROM audit_log;

-- Drop old table and indexes
DROP INDEX IF EXISTS idx_audit_log_created_at;
DROP INDEX IF EXISTS idx_audit_log_action;
DROP TABLE audit_log;

-- Rename new table
ALTER TABLE audit_log_new RENAME TO audit_log;

-- Recreate indexes
CREATE INDEX idx_audit_log_created_at ON audit_log(created_at);
CREATE INDEX idx_audit_log_action ON audit_log(action);
`

// Migration to add deleted_files_count column to scans table
const migrateAddDeletedFilesCount = `
-- Add deleted_files_count column to scans table
ALTER TABLE scans ADD COLUMN deleted_files_count INTEGER DEFAULT 0;
`

// Migration to add service_missing_files table
const migrateAddServiceMissingFilesTable = `
-- Service missing files table for tracking files services report but don't exist in filesystem
-- Cleared at each scan start to only show current scan's missing files
CREATE TABLE IF NOT EXISTS service_missing_files (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	scan_id INTEGER NOT NULL,
	service TEXT NOT NULL CHECK(service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash', 'calibre')),
	service_path TEXT NOT NULL,
	translated_path TEXT NOT NULL,
	size INTEGER,
	service_group TEXT,
	service_group_id TEXT,
	metadata TEXT,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (scan_id) REFERENCES scans(id)
);

CREATE INDEX IF NOT EXISTS idx_missing_files_scan_id ON service_missing_files(scan_id);
CREATE INDEX IF NOT EXISTS idx_missing_files_service ON service_missing_files(service);
CREATE INDEX IF NOT EXISTS idx_missing_files_size ON service_missing_files(size DESC);
CREATE INDEX IF NOT EXISTS idx_missing_files_scan_service ON service_missing_files(scan_id, service);
`

// Migration to add 'cleanup' to audit_log action CHECK constraint
const migrateAddCleanupToAuditLogAction = `
-- Drop audit_log_new if it exists from a previous failed migration
DROP TABLE IF EXISTS audit_log_new;

-- Create new audit_log table with updated CHECK constraint including 'cleanup'
CREATE TABLE audit_log_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	action TEXT NOT NULL CHECK(action IN ('delete', 'mark_rescan', 'config_change', 'consolidate', 'hardlink', 'cleanup')),
	entity_type TEXT NOT NULL,
	entity_id INTEGER,
	details TEXT,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

-- Copy data from old table
INSERT INTO audit_log_new (id, action, entity_type, entity_id, details, created_at)
SELECT id, action, entity_type, entity_id, details, created_at
FROM audit_log;

-- Drop old table and indexes
DROP INDEX IF EXISTS idx_audit_log_created_at;
DROP INDEX IF EXISTS idx_audit_log_action;
DROP TABLE audit_log;

-- Rename new table
ALTER TABLE audit_log_new RENAME TO audit_log;

-- Recreate indexes
CREATE INDEX idx_audit_log_created_at ON audit_log(created_at);
CREATE INDEX idx_audit_log_action ON audit_log(action);
`

// Migration to add 'cleanup' scan type to scans table CHECK constraint
const migrateAddCleanupToScanType = `
-- Drop scans_new if it exists from a previous failed migration
DROP TABLE IF EXISTS scans_new;

-- Create new scans table with updated CHECK constraint
CREATE TABLE scans_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	started_at INTEGER NOT NULL,
	completed_at INTEGER,
	status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'interrupted', 'completed_with_errors')),
	files_scanned INTEGER NOT NULL DEFAULT 0,
	errors TEXT,
	scan_type TEXT NOT NULL DEFAULT 'full' CHECK(scan_type IN ('full', 'incremental', 'disk_location', 'service_update_all', 'service_update_plex', 'service_update_sonarr', 'service_update_radarr', 'service_update_qbittorrent', 'service_update_stash', 'service_update_calibre', 'hash_scan', 'cleanup')),
	current_phase TEXT,
	last_processed_path TEXT,
	resume_from_scan_id INTEGER,
	deleted_files_count INTEGER DEFAULT 0,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (resume_from_scan_id) REFERENCES scans(id)
);

-- Copy data from old table (use COALESCE to handle NULL values)
INSERT INTO scans_new (id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, deleted_files_count, created_at)
SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, COALESCE(deleted_files_count, 0), COALESCE(created_at, started_at)
FROM scans;

-- Drop old table and indexes
DROP INDEX IF EXISTS idx_scans_status;
DROP INDEX IF EXISTS idx_scans_started_at;
DROP TABLE scans;

-- Rename new table
ALTER TABLE scans_new RENAME TO scans;

-- Recreate indexes
CREATE INDEX idx_scans_status ON scans(status);
CREATE INDEX idx_scans_started_at ON scans(started_at);
`

// Migration to add service_update_calibre scan type to scans table CHECK constraint
const migrateAddCalibreServiceUpdateToScanType = `
-- Drop scans_new if it exists from a previous failed migration
DROP TABLE IF EXISTS scans_new;

-- Create new scans table with updated CHECK constraint including service_update_calibre
CREATE TABLE scans_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	started_at INTEGER NOT NULL,
	completed_at INTEGER,
	status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'interrupted', 'completed_with_errors')),
	files_scanned INTEGER NOT NULL DEFAULT 0,
	errors TEXT,
	scan_type TEXT NOT NULL DEFAULT 'full' CHECK(scan_type IN ('full', 'incremental', 'disk_location', 'service_update_all', 'service_update_plex', 'service_update_sonarr', 'service_update_radarr', 'service_update_qbittorrent', 'service_update_stash', 'service_update_calibre', 'hash_scan', 'cleanup', 'file_rescan')),
	current_phase TEXT,
	last_processed_path TEXT,
	resume_from_scan_id INTEGER,
	deleted_files_count INTEGER DEFAULT 0,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (resume_from_scan_id) REFERENCES scans(id)
);

-- Copy data from old table (use COALESCE to handle NULL values)
INSERT INTO scans_new (id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, deleted_files_count, created_at)
SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, COALESCE(deleted_files_count, 0), COALESCE(created_at, started_at)
FROM scans;

-- Drop old table and indexes
DROP INDEX IF EXISTS idx_scans_status;
DROP INDEX IF EXISTS idx_scans_started_at;
DROP TABLE scans;

-- Rename new table
ALTER TABLE scans_new RENAME TO scans;

-- Recreate indexes
CREATE INDEX idx_scans_status ON scans(status);
CREATE INDEX idx_scans_started_at ON scans(started_at);
`

// Migration to add 'calibre' to usage and service_missing_files table CHECK constraints
const migrateAddCalibreToServiceTables = `
-- Update usage table
DROP TABLE IF EXISTS usage_new;
CREATE TABLE usage_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	file_id INTEGER NOT NULL,
	service TEXT NOT NULL CHECK(service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash', 'calibre')),
	reference_path TEXT NOT NULL,
	metadata TEXT,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
	UNIQUE(file_id, service)
);

INSERT INTO usage_new SELECT * FROM usage;

DROP INDEX IF EXISTS idx_usage_file_id;
DROP INDEX IF EXISTS idx_usage_service;
DROP INDEX IF EXISTS idx_usage_reference_path;
DROP TABLE usage;

ALTER TABLE usage_new RENAME TO usage;

CREATE INDEX idx_usage_file_id ON usage(file_id);
CREATE INDEX idx_usage_service ON usage(service);
CREATE INDEX idx_usage_reference_path ON usage(reference_path);

-- Update service_missing_files table
DROP TABLE IF EXISTS service_missing_files_new;
CREATE TABLE service_missing_files_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	scan_id INTEGER NOT NULL,
	service TEXT NOT NULL CHECK(service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash', 'calibre')),
	service_path TEXT NOT NULL,
	translated_path TEXT NOT NULL,
	size INTEGER,
	metadata TEXT,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
);

INSERT INTO service_missing_files_new SELECT * FROM service_missing_files;

DROP INDEX IF EXISTS idx_service_missing_files_scan_id;
DROP INDEX IF EXISTS idx_service_missing_files_service;
DROP TABLE service_missing_files;

ALTER TABLE service_missing_files_new RENAME TO service_missing_files;

CREATE INDEX idx_service_missing_files_scan_id ON service_missing_files(scan_id);
CREATE INDEX idx_service_missing_files_service ON service_missing_files(service);
`

// Migration to add scan_id column to audit_log table
const migrateAddScanIdToAuditLog = `
-- Drop audit_log_new if it exists from a previous failed migration
DROP TABLE IF EXISTS audit_log_new;

-- Create new audit_log table with scan_id column
CREATE TABLE audit_log_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	action TEXT NOT NULL CHECK(action IN ('delete', 'mark_rescan', 'config_change', 'consolidate', 'hardlink', 'cleanup', 'delete_failed')),
	entity_type TEXT NOT NULL,
	entity_id INTEGER,
	scan_id INTEGER,
	details TEXT,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (scan_id) REFERENCES scans(id)
);

-- Copy data from old table
INSERT INTO audit_log_new (id, action, entity_type, entity_id, details, created_at)
SELECT id, action, entity_type, entity_id, details, created_at
FROM audit_log;

-- Drop old table and indexes
DROP INDEX IF EXISTS idx_audit_log_created_at;
DROP INDEX IF EXISTS idx_audit_log_action;
DROP TABLE audit_log;

-- Rename new table
ALTER TABLE audit_log_new RENAME TO audit_log;

-- Recreate indexes
CREATE INDEX idx_audit_log_created_at ON audit_log(created_at);
CREATE INDEX idx_audit_log_action ON audit_log(action);
CREATE INDEX idx_audit_log_scan_id ON audit_log(scan_id);
`

// Migration to add 'file_rescan' scan type to scans table CHECK constraint
const migrateAddFileRescanToScanType = `
-- Drop scans_new if it exists from a previous failed migration
DROP TABLE IF EXISTS scans_new;

-- Create new scans table with updated CHECK constraint
CREATE TABLE scans_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	started_at INTEGER NOT NULL,
	completed_at INTEGER,
	status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'interrupted', 'completed_with_errors')),
	files_scanned INTEGER NOT NULL DEFAULT 0,
	errors TEXT,
	scan_type TEXT NOT NULL DEFAULT 'full' CHECK(scan_type IN ('full', 'incremental', 'disk_location', 'service_update_all', 'service_update_plex', 'service_update_sonarr', 'service_update_radarr', 'service_update_qbittorrent', 'service_update_stash', 'service_update_calibre', 'hash_scan', 'cleanup', 'file_rescan')),
	current_phase TEXT,
	last_processed_path TEXT,
	resume_from_scan_id INTEGER,
	deleted_files_count INTEGER DEFAULT 0,
	created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
	FOREIGN KEY (resume_from_scan_id) REFERENCES scans(id)
);

-- Copy data from old table (use COALESCE to handle NULL values)
INSERT INTO scans_new (id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, deleted_files_count, created_at)
SELECT id, started_at, completed_at, status, files_scanned, errors, scan_type, current_phase, last_processed_path, resume_from_scan_id, COALESCE(deleted_files_count, 0), COALESCE(created_at, started_at)
FROM scans;

-- Drop old table and indexes
DROP INDEX IF EXISTS idx_scans_status;
DROP INDEX IF EXISTS idx_scans_started_at;
DROP TABLE scans;

-- Rename new table
ALTER TABLE scans_new RENAME TO scans;

-- Recreate indexes
CREATE INDEX idx_scans_status ON scans(status);
CREATE INDEX idx_scans_started_at ON scans(started_at);
`
