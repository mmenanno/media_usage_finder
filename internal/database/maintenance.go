package database

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

// Maintenance keys for the config table — used to record when each
// maintenance operation last ran so the UI can surface staleness.
const (
	maintenanceKeyOptimize = "maintenance.last_optimize"
	maintenanceKeyVacuum   = "maintenance.last_incremental_vacuum"
	maintenanceKeyFullVac  = "maintenance.last_full_vacuum"
	maintenanceKeyWALCheck = "maintenance.last_wal_checkpoint"
)

// AutoVacuumMode is the value of `PRAGMA auto_vacuum`.
type AutoVacuumMode int

const (
	AutoVacuumNone        AutoVacuumMode = 0
	AutoVacuumFull        AutoVacuumMode = 1
	AutoVacuumIncremental AutoVacuumMode = 2
)

// FreelistInfo summarizes how much space SQLite could reclaim with VACUUM.
type FreelistInfo struct {
	FreelistCount int64 // pages on the freelist
	PageSize      int64 // bytes per page
	ReclaimableKB int64 // FreelistCount * PageSize / 1024
	DatabaseKB    int64 // total db file size in KB
}

// MaintenanceStatus exposes the wall-clock time of the last run of each
// maintenance operation. Zero values indicate the operation has never run
// (or the record was cleared).
type MaintenanceStatus struct {
	LastOptimize          *time.Time
	LastIncrementalVacuum *time.Time
	LastFullVacuum        *time.Time
	LastWALCheckpoint     *time.Time
	AutoVacuum            AutoVacuumMode
	Freelist              FreelistInfo
}

// OptimizeOnStartup runs the SQLite-recommended maintenance call. It is
// cheap (a no-op for fresh databases) and safe to call on every startup.
// `analysis_limit` bounds runtime; without it ANALYZE on a busy table can
// scan many rows. 400 is the value the SQLite docs recommend.
func (db *DB) OptimizeOnStartup() error {
	if _, err := db.conn.Exec(`PRAGMA analysis_limit = 400`); err != nil {
		return fmt.Errorf("set analysis_limit: %w", err)
	}
	if _, err := db.conn.Exec(`PRAGMA optimize`); err != nil {
		return fmt.Errorf("PRAGMA optimize: %w", err)
	}
	return db.recordMaintenanceRun(maintenanceKeyOptimize)
}

// CheckpointWAL truncates the write-ahead log. Run after large writes (e.g.,
// scan completion) to keep the WAL bounded.
func (db *DB) CheckpointWAL() error {
	if _, err := db.conn.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return fmt.Errorf("PRAGMA wal_checkpoint(TRUNCATE): %w", err)
	}
	return db.recordMaintenanceRun(maintenanceKeyWALCheck)
}

// IncrementalVacuum reclaims free pages without rewriting the entire file.
// Returns the number of bytes reclaimed (best-effort: derived from the
// freelist before/after the call). Requires `auto_vacuum=INCREMENTAL` —
// callers should use GetAutoVacuumMode to check first.
func (db *DB) IncrementalVacuum() (reclaimedBytes int64, err error) {
	before, err := db.GetFreelistInfo()
	if err != nil {
		return 0, fmt.Errorf("read pre-vacuum freelist: %w", err)
	}
	if _, err := db.conn.Exec(`PRAGMA incremental_vacuum`); err != nil {
		return 0, fmt.Errorf("PRAGMA incremental_vacuum: %w", err)
	}
	after, err := db.GetFreelistInfo()
	if err != nil {
		return 0, fmt.Errorf("read post-vacuum freelist: %w", err)
	}
	reclaimedBytes = (before.FreelistCount - after.FreelistCount) * before.PageSize
	if reclaimedBytes < 0 {
		reclaimedBytes = 0
	}
	if err := db.recordMaintenanceRun(maintenanceKeyVacuum); err != nil {
		return reclaimedBytes, err
	}
	return reclaimedBytes, nil
}

// MarkFullVacuum records that a manual full VACUUM ran. Called from
// VacuumDatabase so the Advanced page can report when the last full
// vacuum happened.
func (db *DB) MarkFullVacuum() error {
	return db.recordMaintenanceRun(maintenanceKeyFullVac)
}

// GetAutoVacuumMode returns the current `PRAGMA auto_vacuum` setting.
func (db *DB) GetAutoVacuumMode() (AutoVacuumMode, error) {
	var v int
	if err := db.conn.QueryRow(`PRAGMA auto_vacuum`).Scan(&v); err != nil {
		return AutoVacuumNone, fmt.Errorf("read auto_vacuum: %w", err)
	}
	return AutoVacuumMode(v), nil
}

// GetFreelistInfo returns freelist + page-size info so the UI can show how
// much space a VACUUM would reclaim.
func (db *DB) GetFreelistInfo() (FreelistInfo, error) {
	var info FreelistInfo
	if err := db.conn.QueryRow(`PRAGMA freelist_count`).Scan(&info.FreelistCount); err != nil {
		return info, fmt.Errorf("read freelist_count: %w", err)
	}
	if err := db.conn.QueryRow(`PRAGMA page_size`).Scan(&info.PageSize); err != nil {
		return info, fmt.Errorf("read page_size: %w", err)
	}
	info.ReclaimableKB = (info.FreelistCount * info.PageSize) / 1024
	var pageCount int64
	if err := db.conn.QueryRow(`PRAGMA page_count`).Scan(&pageCount); err != nil {
		return info, fmt.Errorf("read page_count: %w", err)
	}
	info.DatabaseKB = (pageCount * info.PageSize) / 1024
	return info, nil
}

// GetMaintenanceStatus returns the last-run timestamps and current freelist
// info — used to render the maintenance widget on the Advanced page.
func (db *DB) GetMaintenanceStatus() (*MaintenanceStatus, error) {
	status := &MaintenanceStatus{}

	mode, err := db.GetAutoVacuumMode()
	if err != nil {
		return nil, err
	}
	status.AutoVacuum = mode

	freelist, err := db.GetFreelistInfo()
	if err != nil {
		return nil, err
	}
	status.Freelist = freelist

	for key, target := range map[string]**time.Time{
		maintenanceKeyOptimize: &status.LastOptimize,
		maintenanceKeyVacuum:   &status.LastIncrementalVacuum,
		maintenanceKeyFullVac:  &status.LastFullVacuum,
		maintenanceKeyWALCheck: &status.LastWALCheckpoint,
	} {
		t, err := db.getMaintenanceTime(key)
		if err != nil {
			return nil, err
		}
		*target = t
	}

	return status, nil
}

// MaintenanceRunner periodically runs incremental vacuum + optimize. It
// skips runs while a scan is in progress (per `isBusy`) so it never
// contends with active workloads. Cancel the context to stop it.
type MaintenanceRunner struct {
	db       *DB
	interval time.Duration
	isBusy   func() bool
}

// NewMaintenanceRunner returns a runner that ticks every `interval`. The
// `isBusy` callback returns true when work that should not be interrupted
// is in flight (e.g., an active scan or hash run).
func NewMaintenanceRunner(db *DB, interval time.Duration, isBusy func() bool) *MaintenanceRunner {
	return &MaintenanceRunner{db: db, interval: interval, isBusy: isBusy}
}

// Run blocks until ctx is cancelled, ticking at the configured interval.
func (r *MaintenanceRunner) Run(ctx context.Context) {
	if r.interval <= 0 {
		log.Printf("DB maintenance runner: interval <= 0, disabled")
		return
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	log.Printf("DB maintenance runner: ticking every %s", r.interval)
	for {
		select {
		case <-ctx.Done():
			log.Printf("DB maintenance runner: stopping")
			return
		case <-ticker.C:
			r.runOnce()
		}
	}
}

func (r *MaintenanceRunner) runOnce() {
	if r.isBusy != nil && r.isBusy() {
		log.Printf("DB maintenance: skipping run (scan/hash in progress)")
		return
	}

	mode, err := r.db.GetAutoVacuumMode()
	if err != nil {
		log.Printf("DB maintenance: failed to read auto_vacuum mode: %v", err)
		return
	}

	start := time.Now()
	if mode == AutoVacuumIncremental {
		reclaimed, err := r.db.IncrementalVacuum()
		if err != nil {
			log.Printf("DB maintenance: incremental_vacuum failed: %v", err)
		} else {
			log.Printf("DB maintenance: incremental_vacuum reclaimed %d bytes in %s", reclaimed, time.Since(start).Round(time.Millisecond))
		}
	} else {
		log.Printf("DB maintenance: auto_vacuum=%d, skipping incremental_vacuum (run a manual VACUUM once to enable INCREMENTAL)", mode)
	}

	optimizeStart := time.Now()
	if err := r.db.OptimizeOnStartup(); err != nil {
		log.Printf("DB maintenance: PRAGMA optimize failed: %v", err)
	} else {
		log.Printf("DB maintenance: PRAGMA optimize completed in %s", time.Since(optimizeStart).Round(time.Millisecond))
	}
}

// recordMaintenanceRun stores the current time against `key` in the config
// table so it survives restarts.
func (db *DB) recordMaintenanceRun(key string) error {
	value := strconv.FormatInt(time.Now().Unix(), 10)
	if err := db.SetConfig(key, value); err != nil {
		return fmt.Errorf("record %s: %w", key, err)
	}
	return nil
}

// getMaintenanceTime returns nil if the key has never been set.
func (db *DB) getMaintenanceTime(key string) (*time.Time, error) {
	value, err := db.GetConfig(key)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key, err)
	}
	if value == "" {
		return nil, nil
	}
	unix, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, nil
	}
	t := time.Unix(unix, 0)
	return &t, nil
}
