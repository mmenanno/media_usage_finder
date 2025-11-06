package scanner

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
)

// HashScanner handles file hashing operations
type HashScanner struct {
	db       *database.DB
	config   *config.DuplicateDetectionConfig
	hasher   *FileHasher
	progress *Progress
	cancel   context.CancelFunc
	scanCtx  context.Context

	// Stats for rate limiting
	mu              sync.Mutex
	bytesProcessed  int64
	lastRateLimited time.Time
}

// NewHashScanner creates a new hash scanner
func NewHashScanner(db *database.DB, cfg *config.DuplicateDetectionConfig) *HashScanner {
	// Parse buffer size from config string (e.g., "4MB")
	bufferSize := 4 * 1024 * 1024 // Default 4MB
	if cfg.HashBufferSize != "" {
		if size, err := disk.ParseSize(cfg.HashBufferSize); err == nil {
			bufferSize = int(size)
		}
	}

	return &HashScanner{
		db:     db,
		config: cfg,
		hasher: NewFileHasher(cfg.HashAlgorithm, bufferSize),
	}
}

// Start begins the hash scanning process
func (hs *HashScanner) Start(ctx context.Context, minSize, maxSize int64) error {
	// Check if hashing is already running
	currentScan, err := hs.db.GetCurrentScan()
	if err != nil {
		return fmt.Errorf("failed to check for running scan: %w", err)
	}
	if currentScan != nil && currentScan.ScanType == "hash_scan" {
		return fmt.Errorf("hash scan already running (ID: %d)", currentScan.ID)
	}

	// Create scan record
	scan, err := hs.db.CreateScan("hash_scan")
	if err != nil {
		return fmt.Errorf("failed to create scan record: %w", err)
	}

	// Initialize progress tracker
	hs.progress = NewProgress(scan.ID, hs.db)
	hs.progress.SetPhase("Initializing")
	hs.progress.Log("Starting hash calculation...")

	// Setup cancellation context
	ctx, cancel := context.WithCancel(ctx)
	hs.cancel = cancel
	hs.scanCtx = ctx

	// Update scan phase
	if err := hs.db.UpdateScanPhase(scan.ID, "Initializing"); err != nil {
		log.Printf("Warning: failed to update scan phase: %v", err)
	}

	// Get files that need hashing (use configured order strategy)
	hashOrder := hs.config.HashOrder
	if hashOrder == "" {
		hashOrder = "smallest_first" // Default
	}
	files, err := hs.db.GetFilesNeedingHash(minSize, maxSize, hashOrder)
	if err != nil {
		hs.progress.AddError(fmt.Sprintf("Failed to get files: %v", err))
		hs.progress.Stop()
		hs.db.CompleteScan(scan.ID, "failed", fmt.Sprintf("Failed to get files: %v", err))
		return fmt.Errorf("failed to get files: %w", err)
	}

	hs.progress.SetTotalFiles(int64(len(files)))
	hs.progress.Log(fmt.Sprintf("Found %d files needing hash", len(files)))

	// Calculate total size
	var totalSize int64
	for _, f := range files {
		totalSize += f.Size
	}
	hs.progress.TotalSize = totalSize

	// Process files (blocks until complete)
	hs.processFiles(ctx, files, scan.ID)

	return nil
}

// Resume continues an interrupted hash scan
func (hs *HashScanner) Resume(ctx context.Context) error {
	// For hash scanning, we just start fresh by checking which files still need hashing
	// The database already tracks which files have been hashed
	// Note: A proper scan will be created in Start(), so we use 0/nil here temporarily
	hs.progress = NewProgress(0, nil)
	hs.progress.SetPhase("Resuming")
	hs.progress.Log("Resuming hash calculation...")

	return hs.Start(ctx, hs.config.MinFileSize, 0)
}

// VerifyDuplicates performs full hashing on files with quick-hash duplicates
func (hs *HashScanner) VerifyDuplicates(ctx context.Context) error {
	// Check if verification is already running
	currentScan, err := hs.db.GetCurrentScan()
	if err != nil {
		return fmt.Errorf("failed to check for running scan: %w", err)
	}
	if currentScan != nil && currentScan.ScanType == "hash_scan" {
		return fmt.Errorf("hash scan already running (ID: %d)", currentScan.ID)
	}

	// Create scan record
	scan, err := hs.db.CreateScan("hash_scan")
	if err != nil {
		return fmt.Errorf("failed to create scan record: %w", err)
	}

	// Initialize progress tracker
	hs.progress = NewProgress(scan.ID, hs.db)
	hs.progress.SetPhase("Finding Duplicates")
	hs.progress.Log("Finding files with quick-hash duplicates to verify...")

	// Setup cancellation context
	ctx, cancel := context.WithCancel(ctx)
	hs.cancel = cancel
	hs.scanCtx = ctx

	// Update scan phase
	if err := hs.db.UpdateScanPhase(scan.ID, "Finding Duplicates"); err != nil {
		log.Printf("Warning: failed to update scan phase: %v", err)
	}

	// Get files with quick hashes that have duplicates
	files, err := hs.db.GetFilesWithQuickHashDuplicates()
	if err != nil {
		hs.progress.AddError(fmt.Sprintf("Failed to get files: %v", err))
		hs.progress.Stop()
		hs.db.CompleteScan(scan.ID, "failed", fmt.Sprintf("Failed to get files: %v", err))
		return fmt.Errorf("failed to get files: %w", err)
	}

	hs.progress.SetTotalFiles(int64(len(files)))
	hs.progress.Log(fmt.Sprintf("Found %d files with quick-hash duplicates to verify", len(files)))

	// Calculate total size
	var totalSize int64
	for _, f := range files {
		totalSize += f.Size
	}
	hs.progress.TotalSize = totalSize

	// Process files for verification (blocks until complete)
	hs.verifyFiles(ctx, files, scan.ID)

	return nil
}

// UpgradeAllQuickHashes performs full hashing on ALL files with quick hashes
func (hs *HashScanner) UpgradeAllQuickHashes(ctx context.Context) error {
	// Check if upgrade is already running
	currentScan, err := hs.db.GetCurrentScan()
	if err != nil {
		return fmt.Errorf("failed to check for running scan: %w", err)
	}
	if currentScan != nil && currentScan.ScanType == "hash_scan" {
		return fmt.Errorf("hash scan already running (ID: %d)", currentScan.ID)
	}

	// Create scan record
	scan, err := hs.db.CreateScan("hash_scan")
	if err != nil {
		return fmt.Errorf("failed to create scan record: %w", err)
	}

	// Initialize progress tracker
	hs.progress = NewProgress(scan.ID, hs.db)
	hs.progress.SetPhase("Finding Quick Hashes")
	hs.progress.Log("Finding all files with quick hashes to upgrade...")

	// Setup cancellation context
	ctx, cancel := context.WithCancel(ctx)
	hs.cancel = cancel
	hs.scanCtx = ctx

	// Update scan phase
	if err := hs.db.UpdateScanPhase(scan.ID, "Finding Quick Hashes"); err != nil {
		log.Printf("Warning: failed to update scan phase: %v", err)
	}

	// Get all files with quick hashes
	files, err := hs.db.GetFilesWithQuickHashes()
	if err != nil {
		hs.progress.AddError(fmt.Sprintf("Failed to get files: %v", err))
		hs.progress.Stop()
		hs.db.CompleteScan(scan.ID, "failed", fmt.Sprintf("Failed to get files: %v", err))
		return fmt.Errorf("failed to get files: %w", err)
	}

	hs.progress.SetTotalFiles(int64(len(files)))
	hs.progress.Log(fmt.Sprintf("Found %d files with quick hashes to upgrade", len(files)))

	// Calculate total size
	var totalSize int64
	for _, f := range files {
		totalSize += f.Size
	}
	hs.progress.TotalSize = totalSize

	// Process files for upgrading (blocks until complete)
	hs.verifyFiles(ctx, files, scan.ID)

	return nil
}

// VerifyDuplicatesProgressive progressively verifies duplicates by upgrading hash levels
// Starts at level 1 and progressively upgrades to higher levels only for files that remain duplicates
func (hs *HashScanner) VerifyDuplicatesProgressive(ctx context.Context) error {
	// Create scan record first
	scan, err := hs.db.CreateScan("hash_scan")
	if err != nil {
		return fmt.Errorf("failed to create scan: %w", err)
	}

	// Initialize progress tracker
	hs.progress = NewProgress(scan.ID, hs.db)
	hs.progress.SetPhase("Preparing Progressive Verification")
	hs.progress.Log("Starting progressive duplicate verification...")

	// Setup cancellation context
	ctx, cancel := context.WithCancel(ctx)
	hs.cancel = cancel
	hs.scanCtx = ctx

	// Process levels 2 through 6
	for level := 2; level <= 6; level++ {
		prevLevel := level - 1

		// Update scan phase
		levelName := GetLevelName(level)
		phaseName := fmt.Sprintf("Upgrading to %s", levelName)
		if err := hs.db.UpdateScanPhase(scan.ID, phaseName); err != nil {
			log.Printf("Warning: failed to update scan phase: %v", err)
		}
		hs.progress.SetPhase(phaseName)

		// Get files with duplicates at previous level
		files, err := hs.db.GetFilesWithHashDuplicatesAtLevel(prevLevel)
		if err != nil {
			hs.progress.AddError(fmt.Sprintf("Failed to get level %d duplicates: %v", prevLevel, err))
			hs.progress.Stop()
			hs.db.CompleteScan(scan.ID, "failed", fmt.Sprintf("Failed to get duplicates: %v", err))
			return fmt.Errorf("failed to get duplicates at level %d: %w", prevLevel, err)
		}

		// If no duplicates at this level, stop (optimization)
		if len(files) == 0 {
			hs.progress.Log(fmt.Sprintf("No duplicates found at level %d, stopping progressive verification", prevLevel))
			break
		}

		hs.progress.SetTotalFiles(int64(len(files)))
		hs.progress.Log(fmt.Sprintf("Found %d files with duplicates at level %d, upgrading to level %d", len(files), prevLevel, level))

		// Calculate total size for this level
		var totalSize int64
		for _, f := range files {
			totalSize += f.Size
		}
		hs.progress.TotalSize = totalSize

		// Process files for this level (blocks until complete)
		hs.progressiveVerifyFiles(ctx, files, scan.ID, level)

		// Check if cancelled
		select {
		case <-ctx.Done():
			return fmt.Errorf("verification cancelled")
		default:
		}

		// Reset counters for next level
		hs.mu.Lock()
		hs.progress.ProcessedFiles = 0
		hs.bytesProcessed = 0
		hs.mu.Unlock()
	}

	// Final completion
	hs.progress.SetPhase("Completed")
	hs.progress.Log("Progressive verification completed")

	var status string
	var errorMsg string
	if len(hs.progress.Errors) > 0 {
		status = "completed_with_errors"
		errorMsg = serializeErrors(hs.progress.Errors)
	} else {
		status = "completed"
	}

	if err := hs.db.CompleteScan(scan.ID, status, errorMsg); err != nil {
		log.Printf("Warning: failed to complete scan: %v", err)
	}

	hs.progress.Stop()
	return nil
}

// progressiveVerifyFiles processes files for progressive hash level upgrade
func (hs *HashScanner) progressiveVerifyFiles(ctx context.Context, files []database.File, _ int64, targetLevel int) {
	// Create work channel
	workChan := make(chan database.File, hs.config.HashWorkers*2)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < hs.config.HashWorkers; i++ {
		wg.Add(1)
		go hs.progressiveWorker(ctx, i+1, workChan, &wg, targetLevel)
	}

	// Send files to workers
	go func() {
		for _, file := range files {
			select {
			case <-ctx.Done():
				close(workChan)
				return
			case workChan <- file:
			}
		}
		close(workChan)
	}()

	// Wait for completion
	wg.Wait()
}

// progressiveWorker processes files for progressive hash upgrade
func (hs *HashScanner) progressiveWorker(ctx context.Context, _ int, workChan <-chan database.File, wg *sync.WaitGroup, targetLevel int) {
	defer wg.Done()

	for file := range workChan {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Calculate hash at target level
		if err := hs.progressiveHashFile(ctx, file, targetLevel); err != nil {
			hs.progress.AddError(fmt.Sprintf("Failed to hash %s: %v", file.Path, err))
			continue
		}

		// Update counters
		hs.mu.Lock()
		hs.progress.ProcessedFiles++
		hs.mu.Unlock()
	}
}

// progressiveHashFile calculates a progressive hash at a specific level
func (hs *HashScanner) progressiveHashFile(ctx context.Context, file database.File, targetLevel int) error {
	// Check context before starting
	select {
	case <-ctx.Done():
		return fmt.Errorf("cancelled")
	default:
	}

	// Get effective level (may be lower if file is small)
	effectiveLevel := GetEffectiveLevel(file.Size, targetLevel)

	var hash string
	var err error

	if effectiveLevel == 6 {
		// Use full hash for level 6
		hs.progress.Log(fmt.Sprintf("Full hashing: %s", filepath.Base(file.Path)))
		var lastProgress int64
		hash, err = hs.hasher.HashWithProgress(file.Path, func(bytesRead int64) {
			// Update processed size incrementally
			delta := bytesRead - lastProgress
			if delta > 0 {
				hs.mu.Lock()
				hs.bytesProcessed += delta
				hs.mu.Unlock()
				lastProgress = bytesRead
			}

			// Check for cancellation periodically
			select {
			case <-ctx.Done():
				return
			default:
			}
		})
	} else {
		// Use partial hash for levels 1-5
		chunkSize := GetChunkSizeForLevel(effectiveLevel)
		hs.progress.Log(fmt.Sprintf("Progressive hashing (Level %d): %s", effectiveLevel, filepath.Base(file.Path)))
		hash, err = hs.hasher.PartialHash(file.Path, file.Size, chunkSize)

		// Track bytes processed (approximate)
		hs.mu.Lock()
		hs.bytesProcessed += chunkSize
		hs.mu.Unlock()
	}

	if err != nil {
		return fmt.Errorf("hash calculation failed: %w", err)
	}

	// Update database with hash level
	if err := hs.db.UpdateFileHashWithLevel(file.ID, hash, hs.hasher.GetAlgorithm(), effectiveLevel); err != nil {
		return fmt.Errorf("failed to update database: %w", err)
	}

	return nil
}

// verifyFiles processes files for full hash verification
func (hs *HashScanner) verifyFiles(ctx context.Context, files []database.File, scanID int64) {
	defer hs.progress.Stop()

	hs.progress.SetPhase("Verifying Duplicates")
	if err := hs.db.UpdateScanPhase(scanID, "Verifying Duplicates"); err != nil {
		log.Printf("Warning: failed to update scan phase: %v", err)
	}

	// Create work channel
	workChan := make(chan database.File, hs.config.HashWorkers*2)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < hs.config.HashWorkers; i++ {
		wg.Add(1)
		go hs.verifyWorker(ctx, i+1, workChan, &wg, scanID)
	}

	// Send files to workers
	go func() {
		for _, file := range files {
			select {
			case <-ctx.Done():
				close(workChan)
				return
			case workChan <- file:
			}
		}
		close(workChan)
	}()

	// Wait for completion
	wg.Wait()

	// Determine final status
	var status string
	var errorMsg string
	select {
	case <-ctx.Done():
		status = "interrupted"
		errorMsg = "Verification was cancelled by user"
		hs.progress.SetPhase("Cancelled")
		hs.progress.Log("Verification was cancelled")
	default:
		if len(hs.progress.Errors) > 0 {
			status = "completed_with_errors"
			// Serialize all accumulated errors to JSON
			errorMsg = serializeErrors(hs.progress.Errors)
		} else {
			status = "completed"
		}
		hs.progress.SetPhase("Completed")
		hs.progress.Log(fmt.Sprintf("Verification completed. Processed %d files", hs.progress.ProcessedFiles))
	}

	// Update scan record
	if err := hs.db.CompleteScan(scanID, status, errorMsg); err != nil {
		log.Printf("Warning: failed to complete scan: %v", err)
	}
}

// verifyWorker processes files for full hash verification
func (hs *HashScanner) verifyWorker(ctx context.Context, _ int, workChan <-chan database.File, wg *sync.WaitGroup, scanID int64) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case file, ok := <-workChan:
			if !ok {
				return // Channel closed
			}

			// Apply rate limiting if configured
			if hs.config.MaxHashRateMB > 0 {
				hs.applyRateLimit(file.Size)
			}

			// Calculate full hash (always full for verification)
			if err := hs.verifyFileHash(ctx, file); err != nil {
				hs.progress.AddError(fmt.Sprintf("Failed to verify %s: %v", file.Path, err))
			} else {
				// Only increment scan file count on success
				if err := hs.db.IncrementScanFiles(scanID, 1); err != nil {
					log.Printf("Warning: failed to increment scan files: %v", err)
				}
			}

			// Update progress
			hs.progress.IncrementFiles(file.Size)
		}
	}
}

// verifyFileHash calculates full hash for a file (used in verification)
func (hs *HashScanner) verifyFileHash(ctx context.Context, file database.File) error {
	// Check context before starting
	select {
	case <-ctx.Done():
		return fmt.Errorf("cancelled")
	default:
	}

	// Log current file
	hs.progress.Log(fmt.Sprintf("Verifying: %s", filepath.Base(file.Path)))

	// Calculate full hash with progress tracking
	var lastProgress int64
	hash, err := hs.hasher.HashWithProgress(file.Path, func(bytesRead int64) {
		// Update processed size incrementally
		delta := bytesRead - lastProgress
		if delta > 0 {
			hs.mu.Lock()
			hs.bytesProcessed += delta
			hs.mu.Unlock()
			lastProgress = bytesRead
		}

		// Check for cancellation periodically
		select {
		case <-ctx.Done():
			return
		default:
		}
	})

	if err != nil {
		return fmt.Errorf("hash calculation failed: %w", err)
	}

	// Update database with full hash (replacing quick hash)
	if err := hs.db.UpdateFileHash(file.ID, hash, hs.hasher.GetAlgorithm(), "full"); err != nil {
		return fmt.Errorf("failed to update database: %w", err)
	}

	return nil
}

// Cancel gracefully stops the hash scan
func (hs *HashScanner) Cancel() bool {
	if hs.cancel != nil {
		log.Println("Cancelling hash scan...")
		if hs.progress != nil {
			hs.progress.SetPhase("Cancelling")
			hs.progress.Log("Hash scan cancelled by user")
		}
		hs.cancel()
		return true
	}
	return false
}

// GetProgress returns the current progress tracker
func (hs *HashScanner) GetProgress() *Progress {
	return hs.progress
}

// processFiles processes files using a worker pool
func (hs *HashScanner) processFiles(ctx context.Context, files []database.File, scanID int64) {
	defer hs.progress.Stop()

	hs.progress.SetPhase("Hashing Files")
	if err := hs.db.UpdateScanPhase(scanID, "Hashing Files"); err != nil {
		log.Printf("Warning: failed to update scan phase: %v", err)
	}

	// Create work channel
	workChan := make(chan database.File, hs.config.HashWorkers*2)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < hs.config.HashWorkers; i++ {
		wg.Add(1)
		go hs.worker(ctx, i+1, workChan, &wg, scanID)
	}

	// Send files to workers
	go func() {
		for _, file := range files {
			select {
			case <-ctx.Done():
				close(workChan)
				return
			case workChan <- file:
			}
		}
		close(workChan)
	}()

	// Wait for completion
	wg.Wait()

	// Determine final status
	var status string
	var errorMsg string
	select {
	case <-ctx.Done():
		status = "interrupted"
		errorMsg = "Hash scan was cancelled by user"
		hs.progress.SetPhase("Cancelled")
		hs.progress.Log("Hash scan was cancelled")
	default:
		if len(hs.progress.Errors) > 0 {
			status = "completed_with_errors"
			// Serialize all accumulated errors to JSON
			errorMsg = serializeErrors(hs.progress.Errors)
		} else {
			status = "completed"
		}
		hs.progress.SetPhase("Completed")
		hs.progress.Log(fmt.Sprintf("Hash scan completed. Processed %d files", hs.progress.ProcessedFiles))
	}

	// Update scan record
	if err := hs.db.CompleteScan(scanID, status, errorMsg); err != nil {
		log.Printf("Warning: failed to complete scan: %v", err)
	}
}

// worker processes files from the work channel
func (hs *HashScanner) worker(ctx context.Context, _ int, workChan <-chan database.File, wg *sync.WaitGroup, scanID int64) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case file, ok := <-workChan:
			if !ok {
				return // Channel closed
			}

			// Apply rate limiting if configured
			if hs.config.MaxHashRateMB > 0 {
				hs.applyRateLimit(file.Size)
			}

			// Calculate hash
			if err := hs.hashFile(ctx, file); err != nil {
				hs.progress.AddError(fmt.Sprintf("Failed to hash %s: %v", file.Path, err))
			} else {
				// Only increment scan file count on success
				if err := hs.db.IncrementScanFiles(scanID, 1); err != nil {
					log.Printf("Warning: failed to increment scan files: %v", err)
				}
			}

			// Update progress
			hs.progress.IncrementFiles(file.Size)
		}
	}
}

// hashFile calculates the hash for a single file
func (hs *HashScanner) hashFile(ctx context.Context, file database.File) error {
	// Check context before starting
	select {
	case <-ctx.Done():
		return fmt.Errorf("cancelled")
	default:
	}

	// Determine hash type based on config mode
	var hash string
	var hashLevel int
	var err error

	switch hs.config.HashMode {
	case "quick_manual", "quick_auto":
		// Use quick hash (first + last 1MB)
		hs.progress.Log(fmt.Sprintf("Quick hashing: %s", filepath.Base(file.Path)))
		hash, err = hs.hasher.QuickHash(file.Path, file.Size)
		hashLevel = 1 // Quick hash is level 1
	case "progressive":
		// Use progressive hash starting at level 1 (1MB)
		level := GetEffectiveLevel(file.Size, 1)
		chunkSize := GetChunkSizeForLevel(level)
		hs.progress.Log(fmt.Sprintf("Progressive hashing (Level %d): %s", level, filepath.Base(file.Path)))
		hash, err = hs.hasher.PartialHash(file.Path, file.Size, chunkSize)
		hashLevel = level
	default: // "full" or unspecified
		// Use full hash with progress tracking
		hs.progress.Log(fmt.Sprintf("Full hashing: %s", filepath.Base(file.Path)))
		var lastProgress int64
		hash, err = hs.hasher.HashWithProgress(file.Path, func(bytesRead int64) {
			// Update processed size incrementally
			delta := bytesRead - lastProgress
			if delta > 0 {
				hs.mu.Lock()
				hs.bytesProcessed += delta
				hs.mu.Unlock()
				lastProgress = bytesRead
			}

			// Check for cancellation periodically
			select {
			case <-ctx.Done():
				return
			default:
			}
		})
		hashLevel = 6 // Full hash is level 6
	}

	if err != nil {
		return fmt.Errorf("hash calculation failed: %w", err)
	}

	// Update database with hash level
	if err := hs.db.UpdateFileHashWithLevel(file.ID, hash, hs.hasher.GetAlgorithm(), hashLevel); err != nil {
		return fmt.Errorf("failed to update database: %w", err)
	}

	return nil
}

// applyRateLimit applies rate limiting to avoid saturating disk I/O
func (hs *HashScanner) applyRateLimit(_ int64) {
	if hs.config.MaxHashRateMB <= 0 {
		return // Rate limiting disabled
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(hs.lastRateLimited)

	if elapsed < time.Second {
		// Calculate allowed bytes per second
		maxBytesPerSec := int64(hs.config.MaxHashRateMB) * 1024 * 1024

		// If we're over the limit, sleep
		if hs.bytesProcessed > maxBytesPerSec {
			sleepTime := time.Second - elapsed
			hs.mu.Unlock() // Unlock while sleeping
			time.Sleep(sleepTime)
			hs.mu.Lock()

			// Reset counters
			hs.bytesProcessed = 0
			hs.lastRateLimited = time.Now()
		}
	} else {
		// Reset counters after a second
		hs.bytesProcessed = 0
		hs.lastRateLimited = now
	}
}
