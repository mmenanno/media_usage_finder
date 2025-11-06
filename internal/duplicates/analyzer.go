package duplicates

import (
	"fmt"
	"sort"

	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
)

// Analyzer analyzes duplicate files and recommends consolidation strategies
type Analyzer struct {
	db           *database.DB
	diskDetector *disk.Detector
	config       *config.DuplicateConsolidationConfig
}

// NewAnalyzer creates a new duplicate analyzer
func NewAnalyzer(db *database.DB, diskDetector *disk.Detector, cfg *config.DuplicateConsolidationConfig) *Analyzer {
	return &Analyzer{
		db:           db,
		diskDetector: diskDetector,
		config:       cfg,
	}
}

// ConsolidationPlan represents a plan to consolidate duplicate files
type ConsolidationPlan struct {
	Group          *database.DuplicateGroup
	KeepFile       *database.DuplicateFile   // File to keep (primary from KeepCluster)
	DeleteFiles    []*database.DuplicateFile // Files to delete/link (DEPRECATED for hardlinks, use clusters)
	KeepCluster    *database.HardlinkCluster // Primary cluster to keep (hardlinks only)
	LinkClusters   []*database.HardlinkCluster // Clusters needing linking (hardlinks only)
	AlreadyLinked  []*database.HardlinkCluster // Clusters already linked together (hardlinks only)
	ActualCopies   int                          // Number of unique inodes (hardlinks only)
	TotalFiles     int                          // Total file count (hardlinks only)
	SpaceSavings   int64
	ReasonToKeep   string // Explanation of why this file was chosen
	KeepDisk       *disk.DiskInfo
	DeleteDisks    []*disk.DiskInfo
}

// AnalyzeCrossDiskDuplicates creates consolidation plans for cross-disk duplicates
// limit parameter controls how many groups to analyze (0 = all groups)
func (a *Analyzer) AnalyzeCrossDiskDuplicates(limit int) ([]*ConsolidationPlan, error) {
	// Get cross-disk duplicate groups with optional limit
	groups, err := a.db.GetCrossDiskDuplicates(limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get cross-disk duplicates: %w", err)
	}

	var plans []*ConsolidationPlan
	errorCount := 0
	for i, group := range groups {
		plan, err := a.createConsolidationPlan(group)
		if err != nil {
			// Log error but continue with other groups
			errorCount++
			fmt.Printf("ERROR: Failed to create consolidation plan for group %d/%d (hash %s): %v\n", i+1, len(groups), group.FileHash, err)
			continue
		}

		if plan != nil {
			plans = append(plans, plan)
		}
	}

	// Sort plans by space savings (descending)
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].SpaceSavings > plans[j].SpaceSavings
	})

	return plans, nil
}

// AnalyzeSameDiskDuplicates creates plans for same-disk duplicates (hardlink candidates)
// limit parameter controls how many groups to analyze (0 = all groups)
func (a *Analyzer) AnalyzeSameDiskDuplicates(limit int) ([]*ConsolidationPlan, error) {
	// Get same-disk duplicate groups with optional limit
	groups, err := a.db.GetSameDiskDuplicates(limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get same-disk duplicates: %w", err)
	}

	var plans []*ConsolidationPlan
	errorCount := 0
	for i, group := range groups {
		plan, err := a.createHardlinkPlan(group)
		if err != nil {
			// Log error but continue with other groups
			errorCount++
			fmt.Printf("ERROR: Failed to create hardlink plan for group %d/%d (hash %s): %v\n", i+1, len(groups), group.FileHash, err)
			continue
		}

		if plan != nil {
			plans = append(plans, plan)
		}
	}

	// Sort plans by space savings (descending)
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].SpaceSavings > plans[j].SpaceSavings
	})

	return plans, nil
}

// createConsolidationPlan creates a consolidation plan for a cross-disk duplicate group
func (a *Analyzer) createConsolidationPlan(group *database.DuplicateGroup) (*ConsolidationPlan, error) {
	if len(group.Files) < 2 {
		return nil, nil // Not actually a duplicate group
	}

	// Enrich files with disk information
	if err := a.enrichFilesWithDiskInfo(group.Files); err != nil {
		return nil, fmt.Errorf("failed to enrich files with disk info: %w", err)
	}

	// Recommend which file to keep
	keepFile, reason := a.RecommendKeepFile(group)
	if keepFile == nil {
		return nil, fmt.Errorf("no suitable file to keep")
	}

	// Build list of files to delete
	var deleteFiles []*database.DuplicateFile
	var deleteDisks []*disk.DiskInfo
	spaceSavings := int64(0)

	for i := range group.Files {
		if group.Files[i].ID != keepFile.ID {
			deleteFiles = append(deleteFiles, &group.Files[i])
			spaceSavings += group.Files[i].Size

			// Get disk info for delete files if detector available
			if a.diskDetector != nil {
				diskInfo, err := a.diskDetector.GetDiskForFile(group.Files[i].DeviceID)
				if err == nil {
					deleteDisks = append(deleteDisks, diskInfo)
				}
			}
		}
	}

	// Get disk info for the keep file - try file_disk_locations first
	var keepDisk *disk.DiskInfo
	keepLocations, err := a.db.GetDiskLocationsForFile(keepFile.ID)
	if err == nil && len(keepLocations) > 0 {
		// Try to get full disk info from detector if available
		if a.diskDetector != nil {
			diskInfo, err := a.diskDetector.GetDiskForFile(keepFile.DeviceID)
			if err == nil {
				keepDisk = diskInfo
			} else {
				// Create minimal disk info from location data
				keepDisk = &disk.DiskInfo{
					Name:        keepLocations[0].DiskName,
					DeviceID:    keepLocations[0].DiskDeviceID,
					UsedPercent: 50.0, // Unknown
				}
			}
		} else {
			// No disk detector - use location data only
			keepDisk = &disk.DiskInfo{
				Name:        keepLocations[0].DiskName,
				DeviceID:    keepLocations[0].DiskDeviceID,
				UsedPercent: 50.0, // Unknown
			}
		}
	} else {
		// Fallback to disk detector only if available
		if a.diskDetector != nil {
			keepDisk, err = a.diskDetector.GetDiskForFile(keepFile.DeviceID)
			if err != nil {
				return nil, fmt.Errorf("failed to get disk info for keep file: %w", err)
			}
		} else {
			// No disk detector and no location data - create minimal info
			keepDisk = &disk.DiskInfo{
				Name:        fmt.Sprintf("Device %d", keepFile.DeviceID),
				DeviceID:    keepFile.DeviceID,
				UsedPercent: 50.0, // Unknown
			}
		}
	}

	return &ConsolidationPlan{
		Group:        group,
		KeepFile:     keepFile,
		DeleteFiles:  deleteFiles,
		SpaceSavings: spaceSavings,
		ReasonToKeep: reason,
		KeepDisk:     keepDisk,
		DeleteDisks:  deleteDisks,
	}, nil
}

// createHardlinkPlan creates a plan for hardlinking same-disk duplicates
func (a *Analyzer) createHardlinkPlan(group *database.DuplicateGroup) (*ConsolidationPlan, error) {
	if len(group.Files) < 2 {
		return nil, nil // Not actually a duplicate group
	}

	// Enrich files with disk information
	if err := a.enrichFilesWithDiskInfo(group.Files); err != nil {
		return nil, fmt.Errorf("failed to enrich files with disk info: %w", err)
	}

	// If no clusters yet (backwards compat), create them now
	if len(group.HardlinkClusters) == 0 {
		group.HardlinkClusters = database.GroupFilesByInode(group.Files, group.TotalSize)
	}

	// If only one cluster, all files are already hardlinked - nothing to do
	if len(group.HardlinkClusters) <= 1 {
		return nil, nil
	}

	// Select the best cluster to keep as primary
	keepCluster := a.selectPrimaryCluster(group.HardlinkClusters)
	if keepCluster == nil {
		return nil, fmt.Errorf("no suitable cluster to keep")
	}

	// Build lists of clusters
	var linkClusters []*database.HardlinkCluster
	var alreadyLinkedClusters []*database.HardlinkCluster

	// The keep cluster itself is "already linked" if it has multiple files
	if keepCluster.IsLinked {
		alreadyLinkedClusters = append(alreadyLinkedClusters, keepCluster)
	}

	// Categorize other clusters
	for i := range group.HardlinkClusters {
		cluster := &group.HardlinkClusters[i]
		if cluster.Inode == keepCluster.Inode {
			continue // Skip the primary cluster
		}

		if cluster.IsLinked {
			// Already linked internally, but needs linking to primary
			alreadyLinkedClusters = append(alreadyLinkedClusters, cluster)
		}

		// All non-primary clusters need linking to primary
		linkClusters = append(linkClusters, cluster)
	}

	// Calculate space savings using ActualSavings from group
	spaceSavings := group.ActualSavings

	// Build reason string
	keepFile := &keepCluster.Files[0]
	var reason string
	if len(keepFile.ServiceUsage) > 0 {
		reason = fmt.Sprintf("Used by %d service(s): %v", len(keepFile.ServiceUsage), keepFile.ServiceUsage)
	} else {
		reason = "Oldest file in primary cluster (least likely to be temporary)"
	}

	// For backwards compatibility, build DeleteFiles list from all non-primary files
	var deleteFiles []*database.DuplicateFile
	for _, cluster := range linkClusters {
		for i := range cluster.Files {
			deleteFiles = append(deleteFiles, &cluster.Files[i])
		}
	}

	// Get disk info for the keep file
	var keepDisk *disk.DiskInfo
	keepLocations, err := a.db.GetDiskLocationsForFile(keepFile.ID)
	if err == nil && len(keepLocations) > 0 {
		if a.diskDetector != nil {
			diskInfo, err := a.diskDetector.GetDiskForFile(keepFile.DeviceID)
			if err == nil {
				keepDisk = diskInfo
			} else {
				keepDisk = &disk.DiskInfo{
					Name:        keepLocations[0].DiskName,
					DeviceID:    keepLocations[0].DiskDeviceID,
					UsedPercent: 50.0,
				}
			}
		} else {
			keepDisk = &disk.DiskInfo{
				Name:        keepLocations[0].DiskName,
				DeviceID:    keepLocations[0].DiskDeviceID,
				UsedPercent: 50.0,
			}
		}
	} else {
		if a.diskDetector != nil {
			keepDisk, err = a.diskDetector.GetDiskForFile(keepFile.DeviceID)
			if err != nil {
				return nil, fmt.Errorf("failed to get disk info for keep file: %w", err)
			}
		} else {
			keepDisk = &disk.DiskInfo{
				Name:        fmt.Sprintf("Device %d", keepFile.DeviceID),
				DeviceID:    keepFile.DeviceID,
				UsedPercent: 50.0,
			}
		}
	}

	return &ConsolidationPlan{
		Group:         group,
		KeepFile:      keepFile,
		DeleteFiles:   deleteFiles,
		KeepCluster:   keepCluster,
		LinkClusters:  linkClusters,
		AlreadyLinked: alreadyLinkedClusters,
		ActualCopies:  len(group.HardlinkClusters),
		TotalFiles:    len(group.Files),
		SpaceSavings:  spaceSavings,
		ReasonToKeep:  reason,
		KeepDisk:      keepDisk,
		DeleteDisks:   []*disk.DiskInfo{keepDisk}, // Same disk for all
	}, nil
}

// selectPrimaryCluster chooses the best cluster to use as the primary for hardlinking
func (a *Analyzer) selectPrimaryCluster(clusters []database.HardlinkCluster) *database.HardlinkCluster {
	if len(clusters) == 0 {
		return nil
	}

	var bestCluster *database.HardlinkCluster
	bestScore := -1

	for i := range clusters {
		cluster := &clusters[i]
		score := 0

		// Priority 1: Cluster with files used by most services
		totalServices := 0
		for _, file := range cluster.Files {
			totalServices += len(file.ServiceUsage)
		}
		score += totalServices * 1000 // High weight for service usage

		// Priority 2: Cluster that's already linked (prefer existing hardlink groups)
		if cluster.IsLinked {
			score += 100
		}

		// Priority 3: Oldest files (check first file in cluster)
		if len(cluster.Files) > 0 {
			// Earlier timestamps get higher scores (inverted)
			// This is a simplification - we'd need to know the oldest timestamp in advance
			// For now, just give a small bonus
			if !cluster.Files[0].ModifiedTime.IsZero() {
				score += 10
			}
		}

		if bestCluster == nil || score > bestScore {
			bestCluster = cluster
			bestScore = score
		}
	}

	return bestCluster
}

// RecommendKeepFile determines which file to keep in a duplicate group
func (a *Analyzer) RecommendKeepFile(group *database.DuplicateGroup) (*database.DuplicateFile, string) {
	if len(group.Files) == 0 {
		return nil, "No files in group"
	}

	// Enrich files with disk information if not already done
	a.enrichFilesWithDiskInfo(group.Files)

	// Strategy: least_full_disk (default)
	if a.config == nil || a.config.Strategy == "" || a.config.Strategy == "least_full_disk" {
		return a.recommendByLeastFullDisk(group.Files)
	}

	// Future: Support other strategies like "preferred_disk"
	return a.recommendByLeastFullDisk(group.Files)
}

// recommendByLeastFullDisk recommends keeping the file on the least full disk
func (a *Analyzer) recommendByLeastFullDisk(files []database.DuplicateFile) (*database.DuplicateFile, string) {
	var bestFile *database.DuplicateFile
	lowestUsedPercent := 100.0

	for i := range files {
		if files[i].DiskUsedPercent < lowestUsedPercent {
			lowestUsedPercent = files[i].DiskUsedPercent
			bestFile = &files[i]
		}
	}

	if bestFile != nil {
		reason := fmt.Sprintf("Least full disk (%.1f%% used)", bestFile.DiskUsedPercent)
		if bestFile.DiskName != "" {
			reason = fmt.Sprintf("Least full disk: %s (%.1f%% used)", bestFile.DiskName, bestFile.DiskUsedPercent)
		}
		return bestFile, reason
	}

	// Fallback to first file if all have same usage
	return &files[0], "First file in list"
}

// enrichFilesWithDiskInfo adds disk information to files
// Uses batch loading to avoid N+1 query problem
func (a *Analyzer) enrichFilesWithDiskInfo(files []database.DuplicateFile) error {
	if len(files) == 0 {
		return nil
	}

	// Collect all file IDs for batch loading
	fileIDs := make([]int64, len(files))
	for i := range files {
		fileIDs[i] = files[i].ID
	}

	// Batch load all disk locations in a single query
	locationsByFileID, err := a.db.GetDiskLocationsByFileIDs(fileIDs)
	if err != nil {
		// If batch loading fails, fall back to individual queries
		fmt.Printf("Warning: batch loading disk locations failed, falling back to individual queries: %v\n", err)
		return a.enrichFilesWithDiskInfoFallback(files)
	}

	// Enrich each file with its disk information
	for i := range files {
		locations := locationsByFileID[files[i].ID]

		if len(locations) > 0 {
			// Use the first location's disk name
			files[i].DiskName = locations[0].DiskName

			// Try to get disk usage from disk detector if available
			if a.diskDetector != nil {
				diskInfo, err := a.diskDetector.GetDiskForFile(locations[0].DiskDeviceID)
				if err == nil {
					files[i].DiskUsedPercent = diskInfo.UsedPercent
				} else {
					// Can't get usage, but we have the disk name - use 50% as default
					files[i].DiskUsedPercent = 50.0
				}
			} else {
				// No disk detector available - use default
				files[i].DiskUsedPercent = 50.0
			}
		} else {
			// No disk location found - use defaults
			// Don't attempt to look up FUSE device IDs (e.g., mergerfs device 143)
			// as they don't correspond to physical disks in the detector
			files[i].DiskName = fmt.Sprintf("Device %d", files[i].DeviceID)
			files[i].DiskUsedPercent = 50.0
		}
	}

	return nil
}

// enrichFilesWithDiskInfoFallback is the original N+1 implementation used as fallback
func (a *Analyzer) enrichFilesWithDiskInfoFallback(files []database.DuplicateFile) error {
	for i := range files {
		// First, try to get disk info from file_disk_locations table
		locations, err := a.db.GetDiskLocationsForFile(files[i].ID)
		if err == nil && len(locations) > 0 {
			// Use the first location's disk name
			files[i].DiskName = locations[0].DiskName

			// Try to get disk usage from disk detector if available
			if a.diskDetector != nil {
				diskInfo, err := a.diskDetector.GetDiskForFile(locations[0].DiskDeviceID)
				if err == nil {
					files[i].DiskUsedPercent = diskInfo.UsedPercent
				} else {
					// Can't get usage, but we have the disk name - use 50% as default
					files[i].DiskUsedPercent = 50.0
				}
			} else {
				// No disk detector available - use default
				files[i].DiskUsedPercent = 50.0
			}
		} else {
			// No disk location found - use defaults
			// Don't attempt to look up FUSE device IDs (e.g., mergerfs device 143)
			// as they don't correspond to physical disks in the detector
			files[i].DiskName = fmt.Sprintf("Device %d", files[i].DeviceID)
			files[i].DiskUsedPercent = 50.0
		}
	}

	return nil
}

// GetConsolidationPlanByHash gets a consolidation plan for a specific hash
func (a *Analyzer) GetConsolidationPlanByHash(hash string) (*ConsolidationPlan, error) {
	group, err := a.db.GetDuplicateGroupByHash(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get duplicate group: %w", err)
	}

	// Determine if this is cross-disk or same-disk
	if group.UniqueDiskCount > 1 {
		return a.createConsolidationPlan(group)
	}

	return a.createHardlinkPlan(group)
}

// CalculateTotalSavings calculates total potential savings from all plans
func CalculateTotalSavings(plans []*ConsolidationPlan) int64 {
	total := int64(0)
	for _, plan := range plans {
		total += plan.SpaceSavings
	}
	return total
}

// FilterPlansByHashType filters plans by hash type (quick or full)
func FilterPlansByHashType(plans []*ConsolidationPlan, hashType string) []*ConsolidationPlan {
	if hashType == "" || hashType == "all" {
		return plans
	}

	filtered := make([]*ConsolidationPlan, 0)
	for _, plan := range plans {
		if plan.Group.HashType == hashType {
			filtered = append(filtered, plan)
		}
	}
	return filtered
}

// FilterPlansByMinSavings filters plans by minimum space savings
func FilterPlansByMinSavings(plans []*ConsolidationPlan, minBytes int64) []*ConsolidationPlan {
	filtered := make([]*ConsolidationPlan, 0)
	for _, plan := range plans {
		if plan.SpaceSavings >= minBytes {
			filtered = append(filtered, plan)
		}
	}
	return filtered
}
