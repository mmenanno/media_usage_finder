package scanner

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
)

// DiskScanner scans individual disks to populate disk-specific file locations
type DiskScanner struct {
	ctx      context.Context
	cfg      *config.Config
	db       *database.DB
	detector *disk.Detector

	// File path cache for fast file_id lookups
	fileCache     map[string]int64 // FUSE path -> file_id
	fileCacheMu   sync.RWMutex
	fileCacheSize int

	// Progress tracking
	progress      *Progress
	filesScanned  int64
	locationsAdded int64
	mu            sync.Mutex
}

// NewDiskScanner creates a new disk scanner
func NewDiskScanner(ctx context.Context, cfg *config.Config, db *database.DB, detector *disk.Detector, progress *Progress) *DiskScanner {
	return &DiskScanner{
		ctx:       ctx,
		cfg:       cfg,
		db:        db,
		detector:  detector,
		fileCache: make(map[string]int64),
		progress:  progress,
	}
}

// ScanDiskLocations scans all configured disks and populates disk location records
func (ds *DiskScanner) ScanDiskLocations() error {
	// Check if disks are configured
	if len(ds.cfg.Disks) == 0 {
		return fmt.Errorf("no disks configured - disk scanning not available")
	}

	// Load file cache for fast lookups
	if err := ds.loadFileCache(); err != nil {
		return fmt.Errorf("failed to load file cache: %w", err)
	}

	// Get worker count from config (default: 5)
	workers := 5
	if ds.cfg.DiskScanWorkers > 0 {
		workers = ds.cfg.DiskScanWorkers
	}

	// Create worker pool
	fileChan := make(chan diskFileInfo, workers*10)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go ds.worker(&wg, fileChan)
	}

	// Scan each disk
	for _, diskCfg := range ds.cfg.Disks {
		if err := ds.scanDisk(diskCfg, fileChan); err != nil {
			fmt.Printf("Warning: Failed to scan disk %s: %v\n", diskCfg.Name, err)
			// Continue with other disks
		}
	}

	// Close channel and wait for workers to finish
	close(fileChan)
	wg.Wait()

	fmt.Printf("Disk scanning complete: %d files scanned, %d disk locations added\n",
		ds.filesScanned, ds.locationsAdded)

	return nil
}

// diskFileInfo contains information about a file found on a disk
type diskFileInfo struct {
	diskName     string
	diskDeviceID int64
	diskPath     string
	fusePath     string
	size         int64
	inode        int64
	modTime      time.Time
}

// scanDisk scans a single disk and sends files to the worker channel
func (ds *DiskScanner) scanDisk(diskCfg config.DiskConfig, fileChan chan<- diskFileInfo) error {
	fmt.Printf("Scanning disk: %s (%s)\n", diskCfg.Name, diskCfg.MountPath)

	// Get disk device ID
	var stat syscall.Stat_t
	if err := syscall.Stat(diskCfg.MountPath, &stat); err != nil {
		return fmt.Errorf("failed to stat disk mount: %w", err)
	}
	diskDeviceID := int64(stat.Dev)

	// Walk the disk mount path
	err := filepath.WalkDir(diskCfg.MountPath, func(path string, d fs.DirEntry, err error) error {
		// Check context cancellation
		select {
		case <-ds.ctx.Done():
			return ds.ctx.Err()
		default:
		}

		if err != nil {
			// Log error but continue
			fmt.Printf("Warning: Error accessing %s: %v\n", path, err)
			return nil
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Get file info
		info, err := d.Info()
		if err != nil {
			fmt.Printf("Warning: Failed to get info for %s: %v\n", path, err)
			return nil
		}

		// Get file stats for inode
		var fileStat syscall.Stat_t
		if err := syscall.Stat(path, &fileStat); err != nil {
			fmt.Printf("Warning: Failed to stat %s: %v\n", path, err)
			return nil
		}

		// Translate disk path to FUSE path
		fusePath := ds.cfg.TranslateDiskPathToFuse(path, diskCfg.MountPath)

		// Send to worker channel
		fileChan <- diskFileInfo{
			diskName:     diskCfg.Name,
			diskDeviceID: diskDeviceID,
			diskPath:     path,
			fusePath:     fusePath,
			size:         info.Size(),
			inode:        int64(fileStat.Ino),
			modTime:      info.ModTime(),
		}

		// Update progress
		ds.mu.Lock()
		ds.filesScanned++
		ds.mu.Unlock()

		return nil
	})

	return err
}

// worker processes files from the channel and inserts disk location records
func (ds *DiskScanner) worker(wg *sync.WaitGroup, fileChan <-chan diskFileInfo) {
	defer wg.Done()

	// Batch for efficient inserts
	batch := make([]*database.FileDiskLocation, 0, 100)

	for fileInfo := range fileChan {
		// Look up file_id from cache
		fileID, err := ds.getFileID(fileInfo.fusePath)
		if err != nil {
			// File not found in database - skip
			// This can happen if the file was added after the main scan
			continue
		}

		// Create disk location record
		loc := &database.FileDiskLocation{
			FileID:       fileID,
			DiskName:     fileInfo.diskName,
			DiskDeviceID: fileInfo.diskDeviceID,
			DiskPath:     fileInfo.diskPath,
			Size:         fileInfo.size,
			Inode:        fileInfo.inode,
			ModifiedTime: fileInfo.modTime,
			LastVerified: time.Now(),
		}

		batch = append(batch, loc)

		// Flush batch when it reaches size
		if len(batch) >= 100 {
			if err := ds.db.BatchUpsertFileDiskLocations(ds.ctx, batch); err != nil {
				fmt.Printf("Warning: Failed to insert batch: %v\n", err)
			} else {
				ds.mu.Lock()
				ds.locationsAdded += int64(len(batch))
				ds.mu.Unlock()
			}
			batch = batch[:0] // Reset batch
		}
	}

	// Flush remaining batch
	if len(batch) > 0 {
		if err := ds.db.BatchUpsertFileDiskLocations(ds.ctx, batch); err != nil {
			fmt.Printf("Warning: Failed to insert final batch: %v\n", err)
		} else {
			ds.mu.Lock()
			ds.locationsAdded += int64(len(batch))
			ds.mu.Unlock()
		}
	}
}

// loadFileCache loads all file paths and IDs into memory for fast lookups
func (ds *DiskScanner) loadFileCache() error {
	fmt.Println("Loading file cache...")

	// Query all file paths and IDs
	query := `SELECT id, path FROM files`
	rows, err := ds.db.Conn().Query(query)
	if err != nil {
		return fmt.Errorf("failed to query files: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int64
		var path string
		if err := rows.Scan(&id, &path); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		ds.fileCacheMu.Lock()
		ds.fileCache[path] = id
		count++
		ds.fileCacheMu.Unlock()
	}

	ds.fileCacheSize = count
	fmt.Printf("Loaded %d files into cache\n", count)

	return rows.Err()
}

// getFileID looks up a file ID by its FUSE path
func (ds *DiskScanner) getFileID(fusePath string) (int64, error) {
	// Check cache first
	ds.fileCacheMu.RLock()
	fileID, ok := ds.fileCache[fusePath]
	ds.fileCacheMu.RUnlock()

	if ok {
		return fileID, nil
	}

	// Not in cache - query database
	file, err := ds.db.GetFileByPath(fusePath)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("file not found: %s", fusePath)
	}
	if err != nil {
		return 0, fmt.Errorf("database error: %w", err)
	}

	// Add to cache
	ds.fileCacheMu.Lock()
	ds.fileCache[fusePath] = file.ID
	ds.fileCacheMu.Unlock()

	return file.ID, nil
}
