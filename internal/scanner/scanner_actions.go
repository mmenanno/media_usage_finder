package scanner

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
)

func (s *Scanner) RescanFiles(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return fmt.Errorf("no paths provided for rescan")
	}

	// Create scan record
	scan, err := s.db.CreateScan("file_rescan")
	if err != nil {
		return fmt.Errorf("failed to create scan record: %w", err)
	}

	// Create progress tracker
	s.progress = NewProgress(scan.ID, s.db)
	s.progress.SetPhase("Validating Files")
	s.progress.SetTotalFiles(int64(len(paths)))
	s.progress.Log(fmt.Sprintf("Rescanning %d file(s)...", len(paths)))

	// Track context for cancellation
	s.scanCtx = ctx
	defer func() {
		s.scanCtx = nil
		s.progress = nil
	}()

	// Store errors
	var errors []string

	// Validate files and update metadata
	validPaths := make(map[string]bool)
	var filesToUpdate []*database.File
	deletedCount := 0

	for i, path := range paths {
		s.progress.IncrementFiles(0) // Size will be added when we get file info

		// Check for context cancellation
		select {
		case <-ctx.Done():
			s.progress.Log("Rescan cancelled")
			s.db.UpdateScanStatus(scan.ID, "interrupted", "")
			return ctx.Err()
		default:
		}

		// Check if file exists and get metadata
		fileInfo, err := GetFileInfo(path)
		if err != nil {
			// File doesn't exist - delete from database
			s.progress.Log(fmt.Sprintf("File not found, removing from database: %s", path))
			if delErr := s.db.DeleteFileByPath(path, "File no longer exists", false); delErr != nil {
				errMsg := fmt.Sprintf("Failed to delete missing file %s: %v", path, delErr)
				errors = append(errors, errMsg)
				s.progress.Log(errMsg)
			} else {
				deletedCount++
			}
			continue
		}

		// File exists - mark for update
		validPaths[path] = true

		file := &database.File{
			Path:         path,
			Size:         fileInfo.Size,
			Inode:        fileInfo.Inode,
			DeviceID:     fileInfo.DeviceID,
			ModifiedTime: time.Unix(fileInfo.ModifiedTime, 0),
			ScanID:       scan.ID,
			Extension:    database.ExtractExtension(path),
		}
		filesToUpdate = append(filesToUpdate, file)

		if i < 3 || (i+1)%100 == 0 {
			s.progress.Log(fmt.Sprintf("Validated %d/%d files", i+1, len(paths)))
		}
	}

	s.progress.Log(fmt.Sprintf("Validated files: %d exist, %d removed", len(filesToUpdate), deletedCount))

	// Update files in database
	if len(filesToUpdate) > 0 {
		s.progress.SetPhase("Updating File Metadata")
		if err := s.db.BatchUpsertFiles(ctx, filesToUpdate); err != nil {
			errMsg := fmt.Sprintf("Failed to update file metadata: %v", err)
			errors = append(errors, errMsg)
			s.progress.Log(errMsg)
		} else {
			s.progress.Log(fmt.Sprintf("Updated metadata for %d files", len(filesToUpdate)))
		}
	}

	// Query all configured services
	if len(validPaths) > 0 {
		if err := s.updateAllServicesForPaths(ctx, scan.ID, validPaths); err != nil {
			errMsg := fmt.Sprintf("Failed to update service usage: %v", err)
			errors = append(errors, errMsg)
			s.progress.Log(errMsg)
		}
	}

	// Recalculate orphaned status
	s.progress.SetPhase("Recalculating Orphaned Status")
	s.progress.Log("Recalculating orphaned file status...")
	if err := s.db.UpdateOrphanedStatus(ctx); err != nil {
		errMsg := fmt.Sprintf("Failed to update orphaned status: %v", err)
		errors = append(errors, errMsg)
		s.progress.Log(errMsg)
	} else {
		s.progress.Log("Orphaned status recalculated")
	}

	// Update scan status
	status := "completed"
	if len(errors) > 0 {
		status = "completed_with_errors"
	}

	var errorStr string
	if len(errors) > 0 {
		errorStr = serializeErrors(errors)
	}

	if err := s.db.UpdateScanStatus(scan.ID, status, errorStr); err != nil {
		log.Printf("Warning: Failed to update scan status: %v", err)
	}

	if deletedCount > 0 {
		if err := s.db.UpdateScanDeletedCount(scan.ID, int64(deletedCount)); err != nil {
			log.Printf("Warning: Failed to update deleted files count: %v", err)
		}
	}

	s.progress.SetPhase("Completed")
	s.progress.Log(fmt.Sprintf("Rescan complete: %d files processed, %d deleted", len(paths), deletedCount))

	// Call completion callback if set
	if s.onScanComplete != nil {
		s.onScanComplete()
	}

	return nil
}

// Scan performs a full or incremental scan
func (s *Scanner) RecalculateOrphanedStatus() error {
	// Create temporary progress for logging (no persistent logs for this lightweight operation)
	tempProgress := NewProgress(0, nil)
	tempProgress.SetPhase("Recalculating orphaned status")
	originalProgress := s.progress
	s.progress = tempProgress
	defer func() {
		s.progress = originalProgress
		tempProgress.Stop()
	}()

	s.progress.Log("Manually recalculating orphaned status...")

	if err := s.db.UpdateOrphanedStatus(context.Background()); err != nil {
		return fmt.Errorf("failed to update orphaned status: %w", err)
	}

	s.progress.Log("Orphaned status recalculated successfully!")
	return nil
}

// ScanDiskLocations scans configured disks and populates disk-specific file locations
// This enables cross-disk duplicate detection while maintaining FUSE paths as canonical
func (s *Scanner) ScanDiskLocations(detector *disk.Detector) error {
	// Check if disks are configured
	if len(s.config.Disks) == 0 {
		return fmt.Errorf("no disks configured - disk scanning not available")
	}

	// Check if there's already a running scan
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		return fmt.Errorf("failed to check for running scan: %w", err)
	}

	if currentScan != nil {
		return fmt.Errorf("cannot start disk scan while another scan is running (ID: %d)", currentScan.ID)
	}

	// Create scan record
	scan, err := s.db.CreateScan("disk_location")
	if err != nil {
		return fmt.Errorf("failed to create disk scan record: %w", err)
	}

	// Initialize progress tracker
	s.diskScanProgress = NewProgress(scan.ID, s.db)
	s.diskScanProgress.SetPhase("Initializing")
	s.diskScanProgress.Log("Starting disk location scan...")

	// Create disk scanner
	diskScanner := NewDiskScanner(context.Background(), s.config, s.db, detector, s.diskScanProgress)

	// Update phase to loading cache
	s.diskScanProgress.SetPhase("Loading File Cache")
	if err := s.db.UpdateScanPhase(scan.ID, "Loading File Cache"); err != nil {
		log.Printf("Warning: failed to update scan phase: %v", err)
	}

	// Run disk scan
	err = diskScanner.ScanDiskLocations()

	// Determine final status
	status := "completed"
	if err != nil {
		status = "failed"
		s.diskScanProgress.SetPhase("Failed")
		s.diskScanProgress.Log(fmt.Sprintf("Disk scan failed: %v", err))
	} else {
		s.diskScanProgress.SetPhase("Completed")
		s.diskScanProgress.Log("Disk location scanning completed successfully")
	}

	// Update scan record with final status
	var errorMsg *string
	if err != nil {
		msg := err.Error()
		errorMsg = &msg
	}
	if updateErr := s.db.UpdateScan(scan.ID, status, diskScanner.filesScanned, errorMsg); updateErr != nil {
		log.Printf("Warning: failed to update scan record: %v", updateErr)
	}

	// Stop progress
	s.diskScanProgress.Stop()

	// Call onScanComplete callback to invalidate stats cache
	if s.onScanComplete != nil {
		s.onScanComplete()
	}

	// Clear progress reference
	s.diskScanProgress = nil

	if err != nil {
		return fmt.Errorf("disk scanning failed: %w", err)
	}

	return nil
}

// RunCleanupScan walks the filesystem and removes database entries for files that no longer exist
// This is a manual cleanup operation that can be run independently of full scans
func (s *Scanner) RunCleanupScan() error {
	// Check if there's already a running scan
	currentScan, err := s.db.GetCurrentScan()
	if err != nil {
		return fmt.Errorf("failed to check for running scan: %w", err)
	}

	if currentScan != nil {
		return fmt.Errorf("cannot start cleanup scan while another scan is running (ID: %d)", currentScan.ID)
	}

	// Create scan record with type 'cleanup'
	scan, err := s.db.CreateScan("cleanup")
	if err != nil {
		return fmt.Errorf("failed to create cleanup scan record: %w", err)
	}

	// Create progress tracker with persistent logging
	if s.progress != nil {
		return fmt.Errorf("cleanup scan already in progress")
	}
	s.progress = NewProgress(scan.ID, s.db)

	// Ensure progress is stopped and scan is finalized
	defer func() {
		if s.progress != nil {
			s.progress.Stop()
			s.progress = nil
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Store scan context
	s.scanCtx = ctx

	// Run the cleanup operation
	var cleanupErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				cleanupErr = fmt.Errorf("cleanup scan panic: %v", r)
				s.progress.Log(fmt.Sprintf("PANIC: %v", r))
			}
		}()

		s.progress.SetPhase("Initializing")
		s.progress.Log("Starting cleanup scan - walking filesystem to find existing files...")

		// Update scan phase
		if err := s.db.UpdateScanPhase(scan.ID, "Walking filesystem"); err != nil {
			cleanupErr = fmt.Errorf("failed to update scan phase: %w", err)
			return
		}

		// Walk filesystem and collect existing file paths using WalkFiles
		s.progress.SetPhase("Walking filesystem")

		// Create channel to receive file info
		fileInfoChan := make(chan FileInfo, s.config.ScanBufferSize)
		existingPaths := make(map[string]bool)

		// Start goroutine to collect paths from channel
		collectDone := make(chan struct{})
		go func() {
			defer close(collectDone)
			for fileInfo := range fileInfoChan {
				existingPaths[fileInfo.Path] = true
				s.progress.IncrementFiles(fileInfo.Size)
			}
		}()

		// Walk filesystem
		s.progress.Log("Walking filesystem to find existing files...")
		err := WalkFiles(ctx, s.config.ScanPaths, fileInfoChan, s.progress)
		close(fileInfoChan) // Signal collection goroutine to finish
		<-collectDone       // Wait for collection to complete

		if err != nil {
			if ctx.Err() != nil {
				cleanupErr = fmt.Errorf("cleanup scan cancelled")
				return
			}
			cleanupErr = fmt.Errorf("failed to walk filesystem: %w", err)
			return
		}

		s.progress.Log(fmt.Sprintf("Found %d files on disk", len(existingPaths)))

		// Delete files not in the existing set
		s.progress.SetPhase("Removing missing files")
		if err := s.db.UpdateScanPhase(scan.ID, "Removing missing files"); err != nil {
			cleanupErr = fmt.Errorf("failed to update scan phase: %w", err)
			return
		}

		deletedCount, err := s.db.DeleteFilesNotInSet(ctx, existingPaths, scan.ID)
		if err != nil {
			cleanupErr = fmt.Errorf("failed to delete missing files: %w", err)
			return
		}

		if deletedCount > 0 {
			s.progress.Log(fmt.Sprintf("Removed %d files that no longer exist on disk", deletedCount))
			// Update scan record with deleted count
			if err := s.db.UpdateScanDeletedCount(scan.ID, deletedCount); err != nil {
				s.progress.Log(fmt.Sprintf("Warning: Failed to update deleted files count: %v", err))
			}
		} else {
			s.progress.Log("No missing files found to cleanup")
		}

		s.progress.SetPhase("Completed")
		s.progress.Log("Cleanup scan completed successfully!")
	}()

	// Update scan status
	status := "completed"
	var errorStr *string
	if cleanupErr != nil {
		status = "failed"
		errMsg := cleanupErr.Error()
		errorStr = &errMsg
	}

	processedFiles := s.progress.ProcessedFiles
	if err := s.db.UpdateScan(scan.ID, status, processedFiles, errorStr); err != nil {
		return fmt.Errorf("failed to update scan status: %w", err)
	}

	// Call onScanComplete callback to invalidate stats cache
	if s.onScanComplete != nil {
		s.onScanComplete()
	}

	if cleanupErr != nil {
		return cleanupErr
	}

	return nil
}
