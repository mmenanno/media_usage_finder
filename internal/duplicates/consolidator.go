package duplicates

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"

	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/scanner"
)

// Consolidator manages safe file consolidation and hardlink creation
type Consolidator struct {
	db     *database.DB
	config *config.DuplicateConsolidationConfig
	hasher *scanner.FileHasher
}

// NewConsolidator creates a new consolidator
func NewConsolidator(db *database.DB, cfg *config.DuplicateConsolidationConfig, hasher *scanner.FileHasher) *Consolidator {
	return &Consolidator{
		db:     db,
		config: cfg,
		hasher: hasher,
	}
}

// ConsolidationResult contains the results of a consolidation operation
type ConsolidationResult struct {
	GroupsProcessed int
	FilesDeleted    int
	SpaceFreed      int64
	Errors          []ConsolidationError
	DryRun          bool
}

// ConsolidationError represents an error during consolidation
type ConsolidationError struct {
	GroupHash string
	FilePath  string
	Error     string
}

// ConsolidateCrossDisk executes cross-disk consolidation plan
func (c *Consolidator) ConsolidateCrossDisk(plans []*ConsolidationPlan, dryRun bool) (*ConsolidationResult, error) {
	result := &ConsolidationResult{
		DryRun: dryRun,
	}

	for _, plan := range plans {
		if err := c.processGroup(plan, dryRun); err != nil {
			result.Errors = append(result.Errors, ConsolidationError{
				GroupHash: plan.Group.FileHash,
				FilePath:  plan.KeepFile.Path,
				Error:     err.Error(),
			})
			continue
		}

		result.GroupsProcessed++
		result.FilesDeleted += len(plan.DeleteFiles)
		result.SpaceFreed += plan.SpaceSavings
	}

	return result, nil
}

// processGroup handles one duplicate group
func (c *Consolidator) processGroup(plan *ConsolidationPlan, dryRun bool) error {
	// Step 1: Verify kept file exists and is readable
	if err := c.verifyFileSafety(plan.KeepFile); err != nil {
		return fmt.Errorf("keep file verification failed: %w", err)
	}

	// Step 2: If VerifyBeforeDelete is enabled, re-hash the kept file
	if c.config.VerifyBeforeDelete && !dryRun {
		if err := c.verifyFileHash(plan.KeepFile.Path, plan.Group.FileHash); err != nil {
			return fmt.Errorf("keep file hash verification failed: %w", err)
		}
	}

	// Step 3: Process each file to delete
	for _, deleteFile := range plan.DeleteFiles {
		if dryRun {
			log.Printf("[DRY-RUN] Would delete: %s (save %d bytes)", deleteFile.Path, deleteFile.Size)
			continue
		}

		// Verify file before deletion
		if err := c.verifyFileSafety(deleteFile); err != nil {
			log.Printf("WARNING: Skipping %s: %v", deleteFile.Path, err)
			continue
		}

		// Optionally verify hash matches
		if c.config.VerifyBeforeDelete {
			if err := c.verifyFileHash(deleteFile.Path, plan.Group.FileHash); err != nil {
				log.Printf("WARNING: Hash mismatch for %s, skipping: %v", deleteFile.Path, err)
				continue
			}
		}

		// Delete the file atomically
		if err := os.Remove(deleteFile.Path); err != nil {
			return fmt.Errorf("failed to delete %s: %w", deleteFile.Path, err)
		}

		// Update database - remove file from files table
		details := fmt.Sprintf("Cross-disk consolidation: kept %s on %s, deleted duplicate on %s",
			plan.KeepFile.Path, plan.KeepFile.DiskName, deleteFile.DiskName)
		if err := c.db.DeleteFileByPath(deleteFile.Path, details); err != nil {
			log.Printf("WARNING: Failed to update database after deleting %s: %v", deleteFile.Path, err)
		}

		// Log consolidation to audit log
		if err := c.db.LogConsolidation(plan.KeepFile, deleteFile, plan.ReasonToKeep); err != nil {
			log.Printf("WARNING: Failed to log consolidation for %s: %v", deleteFile.Path, err)
		}

		log.Printf("Deleted: %s (freed %d bytes)", deleteFile.Path, deleteFile.Size)
	}

	return nil
}

// CreateHardlinks creates hardlinks for same-disk duplicates
func (c *Consolidator) CreateHardlinks(plans []*ConsolidationPlan, dryRun bool) (*ConsolidationResult, error) {
	log.Printf("CreateHardlinks: Starting with %d plans, dryRun=%v", len(plans), dryRun)
	result := &ConsolidationResult{
		DryRun: dryRun,
	}

	for i, plan := range plans {
		log.Printf("CreateHardlinks: Processing plan %d/%d (group: %s)", i+1, len(plans), plan.Group.FileHash[:16])
		if err := c.processHardlinkGroup(plan, dryRun); err != nil {
			result.Errors = append(result.Errors, ConsolidationError{
				GroupHash: plan.Group.FileHash,
				FilePath:  plan.KeepFile.Path,
				Error:     err.Error(),
			})
			continue
		}

		result.GroupsProcessed++
		result.FilesDeleted += len(plan.DeleteFiles) // Actually hardlinked, not deleted
		result.SpaceFreed += plan.SpaceSavings
	}

	return result, nil
}

// processHardlinkGroup creates hardlinks for one duplicate group
func (c *Consolidator) processHardlinkGroup(plan *ConsolidationPlan, dryRun bool) error {
	// Verify primary file exists
	if err := c.verifyFileSafety(plan.KeepFile); err != nil {
		return fmt.Errorf("primary file verification failed: %w", err)
	}

	// Get inode of primary file
	var primaryStat syscall.Stat_t
	if err := syscall.Stat(plan.KeepFile.Path, &primaryStat); err != nil {
		return fmt.Errorf("failed to stat primary file: %w", err)
	}
	primaryInode := primaryStat.Ino

	// Log already-linked clusters if present
	if len(plan.AlreadyLinked) > 0 {
		for _, cluster := range plan.AlreadyLinked {
			if uint64(cluster.Inode) != primaryInode {
				log.Printf("INFO: Cluster with inode %d is already internally linked (%d files) but separate from primary inode %d",
					cluster.Inode, cluster.LinkCount, primaryInode)
			}
		}
	}

	for _, dupFile := range plan.DeleteFiles {
		// Check if file already has the same inode as primary (already hardlinked)
		var dupStat syscall.Stat_t
		if err := syscall.Stat(dupFile.Path, &dupStat); err != nil {
			log.Printf("WARNING: Failed to stat %s, skipping: %v", dupFile.Path, err)
			continue
		}

		if dupStat.Ino == primaryInode {
			// File is already hardlinked to primary - skip it
			if dryRun {
				log.Printf("[DRY-RUN] Skipping %s - already hardlinked to primary (inode %d, 0 bytes savings)",
					dupFile.Path, int64(primaryInode))
			} else {
				log.Printf("INFO: Skipping %s - already hardlinked to primary (inode %d)", dupFile.Path, int64(primaryInode))
			}
			continue
		}

		if dryRun {
			log.Printf("[DRY-RUN] Would hardlink: %s -> %s (save %d bytes, inode %d -> %d)",
				dupFile.Path, plan.KeepFile.Path, dupFile.Size, int64(dupStat.Ino), int64(primaryInode))
			continue
		}

		log.Printf("About to hardlink: %s -> %s", dupFile.Path, plan.KeepFile.Path)

		// Verify duplicate file
		if err := c.verifyFileSafety(dupFile); err != nil {
			log.Printf("WARNING: Skipping %s: %v", dupFile.Path, err)
			continue
		}

		// Verify hash if configured
		if c.config.VerifyBeforeDelete {
			log.Printf("Verifying hash for %s (this may take a while for large files)...", dupFile.Path)
			if err := c.verifyFileHash(dupFile.Path, plan.Group.FileHash); err != nil {
				log.Printf("WARNING: Hash mismatch for %s, skipping: %v", dupFile.Path, err)
				continue
			}
			log.Printf("Hash verification passed for %s", dupFile.Path)
		}

		// Create hardlink atomically
		if err := c.createHardlinkAtomic(dupFile.Path, plan.KeepFile.Path); err != nil {
			log.Printf("WARNING: Failed to create hardlink for %s: %v", dupFile.Path, err)
			continue
		}

		// Verify inode matches after hardlink
		var newStat syscall.Stat_t
		if err := syscall.Stat(dupFile.Path, &newStat); err != nil {
			log.Printf("WARNING: Failed to verify hardlink for %s: %v", dupFile.Path, err)
			continue
		}

		if newStat.Ino != primaryInode {
			log.Printf("ERROR: Hardlink verification failed for %s (inode mismatch: expected %d, got %d)",
				dupFile.Path, primaryInode, newStat.Ino)
			continue
		}

		// Log hardlink creation
		if err := c.db.LogHardlinkCreation(plan.KeepFile, dupFile, plan.ReasonToKeep); err != nil {
			log.Printf("WARNING: Failed to log hardlink creation for %s: %v", dupFile.Path, err)
		}

		log.Printf("Hardlinked: %s -> %s (saved %d bytes, inode %d -> %d)",
			dupFile.Path, plan.KeepFile.Path, dupFile.Size, int64(dupStat.Ino), int64(primaryInode))
	}

	return nil
}

// createHardlinkAtomic creates a hardlink atomically using temp file + rename
func (c *Consolidator) createHardlinkAtomic(oldPath, newPath string) error {
	// Create temp file in same directory as target
	dir := filepath.Dir(oldPath)
	tempPath := filepath.Join(dir, fmt.Sprintf(".hardlink-temp-%d", os.Getpid()))

	// Remove temp file if it exists
	os.Remove(tempPath)

	// Create hardlink to temp path
	if err := os.Link(newPath, tempPath); err != nil {
		return fmt.Errorf("failed to create temp hardlink: %w", err)
	}

	// Atomic rename temp file to target path
	if err := os.Rename(tempPath, oldPath); err != nil {
		os.Remove(tempPath) // Cleanup temp file
		return fmt.Errorf("failed to rename hardlink: %w", err)
	}

	return nil
}

// verifyFileSafety performs safety checks on a file before operations
func (c *Consolidator) verifyFileSafety(file *database.DuplicateFile) error {
	// Check file exists and is readable
	info, err := os.Stat(file.Path)
	if err != nil {
		return fmt.Errorf("file not accessible: %w", err)
	}

	// Check it's a regular file (not a directory or device)
	if !info.Mode().IsRegular() {
		return fmt.Errorf("not a regular file")
	}

	// Check parent directory is writable
	dir := filepath.Dir(file.Path)
	if !isDirWritable(dir) {
		return fmt.Errorf("directory not writable: %s", dir)
	}

	return nil
}

// isDirWritable checks if a directory is writable
func isDirWritable(dir string) bool {
	// Try to create a temp file in the directory
	tempFile := filepath.Join(dir, fmt.Sprintf(".write-test-%d", os.Getpid()))
	f, err := os.Create(tempFile)
	if err != nil {
		return false
	}
	f.Close()
	os.Remove(tempFile)
	return true
}

// verifyFileHash verifies that a file's hash matches the expected hash
func (c *Consolidator) verifyFileHash(path, expectedHash string) error {
	actualHash, err := c.hasher.FullHash(path)
	if err != nil {
		return fmt.Errorf("failed to calculate hash: %w", err)
	}

	if actualHash != expectedHash {
		return fmt.Errorf("hash mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	return nil
}

// PreviewConsolidation generates a preview of what would be consolidated
func (c *Consolidator) PreviewConsolidation(plans []*ConsolidationPlan) *ConsolidationPreview {
	preview := &ConsolidationPreview{
		TotalGroups: len(plans),
		DiskImpacts: make(map[string]*DiskImpact),
	}

	for _, plan := range plans {
		preview.TotalFilesToDelete += len(plan.DeleteFiles)
		preview.TotalSpaceSaved += plan.SpaceSavings

		// Track disk impacts
		for _, delFile := range plan.DeleteFiles {
			diskName := delFile.DiskName
			if diskName == "" {
				diskName = fmt.Sprintf("Device %d", delFile.DeviceID)
			}

			impact, exists := preview.DiskImpacts[diskName]
			if !exists {
				impact = &DiskImpact{
					DiskName: diskName,
				}
				preview.DiskImpacts[diskName] = impact
			}

			impact.SpaceFreed += delFile.Size
			impact.FilesDeleted++
		}
	}

	return preview
}

// ConsolidationPreview represents what will happen during consolidation
type ConsolidationPreview struct {
	TotalGroups        int
	TotalFilesToDelete int
	TotalSpaceSaved    int64
	DiskImpacts        map[string]*DiskImpact
}

// DiskImpact represents the impact on a specific disk
type DiskImpact struct {
	DiskName     string
	SpaceFreed   int64
	FilesDeleted int
}
