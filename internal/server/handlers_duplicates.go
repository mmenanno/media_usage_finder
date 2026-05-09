package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"syscall"

	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
	"github.com/mmenanno/media-usage-finder/internal/duplicates"
	"github.com/mmenanno/media-usage-finder/internal/scanner"
)

func (s *Server) HandleDuplicates(w http.ResponseWriter, r *http.Request) {
	// Get active tab from query parameter (default to same-disk)
	activeTab := r.URL.Query().Get("tab")
	if activeTab == "" {
		activeTab = "same-disk"
	}

	// Check if hash scanning is enabled
	if !s.config.DuplicateDetection.Enabled {
		// Show page with message that hash scanning is disabled
		data := &DuplicatesData{
			Title:               "Duplicate Files",
			Version:             s.version,
			ActiveTab:           activeTab,
			CrossDiskGroups:     []*duplicates.ConsolidationPlan{},
			SameDiskGroups:      []*duplicates.ConsolidationPlan{},
			CrossDiskCount:      0,
			SameDiskCount:       0,
			TotalSavings:        0,
			CrossDiskSavings:    0,
			SameDiskSavings:     0,
			HashScanningEnabled: false,
		}

		s.renderTemplate(w, "duplicates.html", data)
		return
	}

	// Parse filters from URL parameters
	filters := database.DuplicateFilters{
		SearchText: r.URL.Query().Get("search"),
		HashType:   r.URL.Query().Get("hash_type"),
		SortBy:     r.URL.Query().Get("sort"),
		Limit:      25, // Default limit
	}

	// Map "progressive" to "partial" for database query compatibility
	// Progressive hashes at levels 2-5 are stored as hash_type='partial' in the database
	if filters.HashType == "progressive" {
		filters.HashType = "partial"
	}

	// Parse minimum size filter
	if minSizeStr := r.URL.Query().Get("min_size"); minSizeStr != "" {
		switch minSizeStr {
		case "1gb":
			filters.MinSize = 1024 * 1024 * 1024
		case "10gb":
			filters.MinSize = 10 * 1024 * 1024 * 1024
		case "100gb":
			filters.MinSize = 100 * 1024 * 1024 * 1024
		case "1tb":
			filters.MinSize = 1024 * 1024 * 1024 * 1024
		}
	}

	// Parse hash level filter (specific progression level)
	if hashLevelStr := r.URL.Query().Get("hash_level"); hashLevelStr != "" {
		if level, err := strconv.Atoi(hashLevelStr); err == nil && level >= 0 && level <= 6 {
			filters.HashLevel = level
		}
	}

	// Parse pagination
	page := 1
	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}

	// Parse limit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			filters.Limit = l
		}
	}

	// Calculate offset for pagination
	filters.Offset = (page - 1) * filters.Limit

	// Get actual totals from database (not limited)
	duplicateStats, err := s.db.GetDuplicateStats()
	if err != nil {
		log.Printf("ERROR: Failed to get duplicate stats: %v", err)
		duplicateStats = &database.DuplicateStats{}
	}

	// Count files missing disk location data
	filesMissingDiskLocations, err := s.db.CountFilesMissingDiskLocations()
	if err != nil {
		log.Printf("ERROR: Failed to count files missing disk locations: %v", err)
		filesMissingDiskLocations = 0
	}

	// Create analyzer
	analyzer := duplicates.NewAnalyzer(s.db, s.diskDetector, &s.config.DuplicateConsolidation)

	// Get cross-disk duplicates with filters and pagination
	crossDiskPlans, err := analyzer.AnalyzeCrossDiskDuplicates(filters)
	if err != nil {
		log.Printf("ERROR: Failed to analyze cross-disk duplicates: %v", err)
		crossDiskPlans = []*duplicates.ConsolidationPlan{}
	}

	// Get same-disk duplicates with filters and pagination
	sameDiskPlans, err := analyzer.AnalyzeSameDiskDuplicates(filters)
	if err != nil {
		log.Printf("ERROR: Failed to analyze same-disk duplicates: %v", err)
		sameDiskPlans = []*duplicates.ConsolidationPlan{}
	}

	// Calculate skipped same-disk groups (total groups - displayed groups)
	sameDiskGroupsSkipped := int(duplicateStats.SameDiskGroups) - len(sameDiskPlans)
	if sameDiskGroupsSkipped < 0 {
		sameDiskGroupsSkipped = 0
	}

	// Get total count for pagination (both tabs now support it)
	var total int64
	var totalPages int
	if activeTab == "same-disk" {
		total, err = s.db.GetSameDiskDuplicateCount(filters)
		if err != nil {
			log.Printf("ERROR: Failed to get same-disk duplicate count: %v", err)
			total = int64(len(sameDiskPlans))
		}
		totalPages = int((total + int64(filters.Limit) - 1) / int64(filters.Limit))
	} else {
		total, err = s.db.GetCrossDiskDuplicateCount(filters)
		if err != nil {
			log.Printf("ERROR: Failed to get cross-disk duplicate count: %v", err)
			total = int64(len(crossDiskPlans))
		}
		totalPages = int((total + int64(filters.Limit) - 1) / int64(filters.Limit))
	}

	// Count files in displayed plans
	crossDiskFilesToDelete := 0
	for _, plan := range crossDiskPlans {
		if plan != nil && plan.DeleteFiles != nil {
			crossDiskFilesToDelete += len(plan.DeleteFiles)
		}
	}

	sameDiskFilesToLink := 0
	for _, plan := range sameDiskPlans {
		if plan != nil && plan.DeleteFiles != nil {
			sameDiskFilesToLink += len(plan.DeleteFiles)
		}
	}

	// Prepare template data
	data := &DuplicatesData{
		Title:                     "Duplicate Files",
		Version:                   s.version,
		ActiveTab:                 activeTab,
		CrossDiskGroups:           crossDiskPlans,
		SameDiskGroups:            sameDiskPlans,
		CrossDiskCount:            duplicateStats.CrossDiskGroups,
		SameDiskCount:             duplicateStats.SameDiskGroups,
		TotalSavings:              duplicateStats.TotalPotentialSavings,
		CrossDiskSavings:          duplicateStats.CrossDiskPotentialSavings,
		SameDiskSavings:           duplicateStats.SameDiskPotentialSavings,
		CrossDiskFilesToDelete:    crossDiskFilesToDelete,
		SameDiskFilesToLink:       sameDiskFilesToLink,
		HashScanningEnabled:       true,
		DisplayLimit:              filters.Limit,
		ShowingCrossDisk:          len(crossDiskPlans),
		ShowingSameDisk:           len(sameDiskPlans),
		FilesMissingDiskLocations: filesMissingDiskLocations,
		SameDiskGroupsSkipped:     sameDiskGroupsSkipped,
		// Pagination fields
		Page:       page,
		TotalPages: totalPages,
		Total:      total,
		Limit:      filters.Limit,
		Filters:    filters,
	}

	// Check if this is an HTMX request (for pagination/filtering)
	isHTMX := r.Header.Get("HX-Request") == "true"

	if isHTMX {
		// Return just the table fragment for HTMX updates
		tmplSet, ok := s.templates["duplicates_table.html"]
		if !ok {
			log.Printf("ERROR: duplicates_table template not found")
			http.Error(w, "Template not found", http.StatusInternalServerError)
			return
		}

		if err := tmplSet.ExecuteTemplate(w, "duplicates_table.html", data); err != nil {
			log.Printf("ERROR: Failed to render duplicates_table template: %v", err)
			http.Error(w, "Failed to render template", http.StatusInternalServerError)
			return
		}
	} else {
		// Return full page
		s.renderTemplate(w, "duplicates.html", data)
	}
}

// HandleDuplicateGroupCount returns the count of duplicate groups matching the provided filters
func (s *Server) HandleDuplicateGroupCount(w http.ResponseWriter, r *http.Request) {
	// Parse type parameter (same-disk or cross-disk)
	duplicateType := r.URL.Query().Get("type")
	if duplicateType == "" {
		duplicateType = "same-disk" // Default to same-disk
	}

	// Parse filters from URL parameters (same logic as HandleDuplicates)
	filters := database.DuplicateFilters{
		SearchText: r.URL.Query().Get("search"),
		HashType:   r.URL.Query().Get("hash_type"),
		SortBy:     r.URL.Query().Get("sort"),
		Limit:      0, // No limit for count query
	}

	// Map "progressive" to "partial" for database query compatibility
	// Progressive hashes at levels 2-5 are stored as hash_type='partial' in the database
	if filters.HashType == "progressive" {
		filters.HashType = "partial"
	}

	// Parse minimum size filter
	if minSizeStr := r.URL.Query().Get("min_size"); minSizeStr != "" {
		switch minSizeStr {
		case "1gb":
			filters.MinSize = 1024 * 1024 * 1024
		case "10gb":
			filters.MinSize = 10 * 1024 * 1024 * 1024
		case "100gb":
			filters.MinSize = 100 * 1024 * 1024 * 1024
		case "1tb":
			filters.MinSize = 1024 * 1024 * 1024 * 1024
		}
	}

	// Parse hash level filter (specific progression level)
	if hashLevelStr := r.URL.Query().Get("hash_level"); hashLevelStr != "" {
		if level, err := strconv.Atoi(hashLevelStr); err == nil && level >= 0 && level <= 6 {
			filters.HashLevel = level
		}
	}

	// Get count based on type
	var count int64
	var err error

	if duplicateType == "cross-disk" {
		count, err = s.db.GetCrossDiskDuplicateCount(filters)
	} else {
		count, err = s.db.GetSameDiskDuplicateCount(filters)
	}

	if err != nil {
		log.Printf("ERROR: Failed to get duplicate group count: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to count duplicate groups", "count_failed")
		return
	}

	// Return count as JSON
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"type":  duplicateType,
		"count": count,
	})
}

// HandleConsolidateDuplicates executes cross-disk consolidation
func (s *Server) HandleConsolidateDuplicates(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Parse request body
	var req struct {
		DryRun      bool     `json:"dry_run"`
		GroupHashes []string `json:"group_hashes"` // Optional: specific groups only
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body", "invalid_request")
		return
	}

	// Create analyzer and consolidator
	analyzer := duplicates.NewAnalyzer(s.db, s.diskDetector, &s.config.DuplicateConsolidation)

	// Parse buffer size from config
	bufferSize := 4 * 1024 * 1024 // Default 4MB
	if s.config.DuplicateDetection.HashBufferSize != "" {
		if size, err := disk.ParseSize(s.config.DuplicateDetection.HashBufferSize); err == nil {
			bufferSize = int(size)
		}
	}

	// Create hasher for verification
	hasher := scanner.NewFileHasher(s.config.DuplicateDetection.HashAlgorithm, bufferSize)
	consolidator := duplicates.NewConsolidator(s.db, &s.config.DuplicateConsolidation, hasher)

	// Get all cross-disk duplicates (large limit, needed for consolidation)
	filters := database.DuplicateFilters{
		Limit: 10000, // Large limit to get all groups
	}
	plans, err := analyzer.AnalyzeCrossDiskDuplicates(filters)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to analyze duplicates", "analysis_failed")
		return
	}

	// Filter to specific groups if requested
	if len(req.GroupHashes) > 0 {
		hashSet := make(map[string]bool)
		for _, h := range req.GroupHashes {
			hashSet[h] = true
		}

		filteredPlans := make([]*duplicates.ConsolidationPlan, 0)
		for _, plan := range plans {
			if hashSet[plan.Group.FileHash] {
				filteredPlans = append(filteredPlans, plan)
			}
		}
		plans = filteredPlans
	}

	// Execute consolidation
	result, err := consolidator.ConsolidateCrossDisk(plans, req.DryRun)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Consolidation failed", "consolidation_failed")
		return
	}

	// Invalidate stats cache
	s.statsCache.Invalidate()

	// Return result
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":           "success",
		"dry_run":          result.DryRun,
		"groups_processed": result.GroupsProcessed,
		"files_deleted":    result.FilesDeleted,
		"space_freed":      result.SpaceFreed,
		"errors":           result.Errors,
	})
}

// HandleCreateHardlinks creates hardlinks for same-disk duplicates
func (s *Server) HandleCreateHardlinks(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Parse request body
	var req struct {
		DryRun      bool     `json:"dry_run"`
		GroupHashes []string `json:"group_hashes"` // Optional: specific groups only
		// Filter parameters (optional)
		SearchText string `json:"search_text"`
		HashType   string `json:"hash_type"`
		MinSize    int64  `json:"min_size"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body", "invalid_request")
		return
	}

	// Log the request details for debugging
	log.Printf("HandleCreateHardlinks called: dry_run=%v, group_hashes=%v (count: %d), filters=(search:%s, hash_type:%s, min_size:%d)",
		req.DryRun, req.GroupHashes, len(req.GroupHashes), req.SearchText, req.HashType, req.MinSize)

	// Create analyzer and consolidator
	analyzer := duplicates.NewAnalyzer(s.db, s.diskDetector, &s.config.DuplicateConsolidation)

	// Parse buffer size from config
	bufferSize := 4 * 1024 * 1024 // Default 4MB
	if s.config.DuplicateDetection.HashBufferSize != "" {
		if size, err := disk.ParseSize(s.config.DuplicateDetection.HashBufferSize); err == nil {
			bufferSize = int(size)
		}
	}

	// Create hasher for verification
	hasher := scanner.NewFileHasher(s.config.DuplicateDetection.HashAlgorithm, bufferSize)
	consolidator := duplicates.NewConsolidator(s.db, &s.config.DuplicateConsolidation, hasher)

	// Build filters from request
	filters := database.DuplicateFilters{
		SearchText: req.SearchText,
		HashType:   req.HashType,
		MinSize:    req.MinSize,
		Limit:      10000, // Large limit to get all matching groups
	}

	// Map "progressive" to "partial" for database query compatibility
	// Progressive hashes at levels 2-5 are stored as hash_type='partial' in the database
	if filters.HashType == "progressive" {
		filters.HashType = "partial"
	}

	// Get same-disk duplicates with filters applied
	log.Printf("Analyzing same-disk duplicates with filters...")
	plans, err := analyzer.AnalyzeSameDiskDuplicates(filters)
	if err != nil {
		log.Printf("ERROR: Failed to analyze duplicates: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to analyze duplicates", "analysis_failed")
		return
	}
	log.Printf("Found %d plans matching filters", len(plans))

	// Filter to specific groups if requested
	if len(req.GroupHashes) > 0 {
		log.Printf("Filtering to specific groups: %v", req.GroupHashes)
		hashSet := make(map[string]bool)
		for _, h := range req.GroupHashes {
			hashSet[h] = true
		}

		filteredPlans := make([]*duplicates.ConsolidationPlan, 0)
		for _, plan := range plans {
			if hashSet[plan.Group.FileHash] {
				filteredPlans = append(filteredPlans, plan)
			}
		}
		plans = filteredPlans
		log.Printf("Filtered to %d plans", len(plans))
	}

	// Calculate detailed statistics before execution (for dry run summary)
	var totalFilesToLink, totalFilesAlreadyLinked, totalClustersNeedingLink, totalClustersAlreadyLinked int
	hashBreakdown := map[string]int{
		"full_hash":        0,
		"progressive_hash": 0,
		"quick_hash":       0,
	}
	var warnings []string

	// Build top groups list (for preview)
	type TopGroup struct {
		Hash          string `json:"hash"`
		FileSize      int64  `json:"file_size"`
		TotalFiles    int    `json:"total_files"`
		Savings       int64  `json:"savings"`
		HashLevel     int    `json:"hash_level"`
		HashLevelName string `json:"hash_level_name"`
	}
	topGroups := make([]TopGroup, 0)

	// Build list of files to be linked (for preview)
	type FileToLink struct {
		Path      string `json:"path"`
		Size      int64  `json:"size"`
		GroupHash string `json:"group_hash"`
	}
	filesToLink := make([]FileToLink, 0)

	log.Printf("Building statistics for %d plans...", len(plans))
	for _, plan := range plans {
		// Count files needing action vs already linked
		for _, cluster := range plan.LinkClusters {
			isAlreadyLinked := false
			for _, linkedCluster := range plan.AlreadyLinked {
				if cluster.Inode == linkedCluster.Inode {
					isAlreadyLinked = true
					break
				}
			}

			if isAlreadyLinked {
				totalFilesAlreadyLinked += len(cluster.Files)
			} else {
				totalFilesToLink += len(cluster.Files)
				// Add files to the list (excluding the keeper file which is first in cluster)
				for i, file := range cluster.Files {
					if i > 0 { // Skip first file (keeper)
						filesToLink = append(filesToLink, FileToLink{
							Path:      file.Path,
							Size:      file.Size,
							GroupHash: plan.Group.FileHash,
						})
					}
				}
			}
		}

		// Count clusters
		totalClustersAlreadyLinked += len(plan.AlreadyLinked)
		for _, cluster := range plan.LinkClusters {
			needsAction := true
			for _, linkedCluster := range plan.AlreadyLinked {
				if cluster.Inode == linkedCluster.Inode {
					needsAction = false
					break
				}
			}
			if needsAction {
				totalClustersNeedingLink++
			}
		}

		// Hash breakdown
		hashLevel := plan.Group.HashLevel
		if hashLevel == 6 {
			hashBreakdown["full_hash"]++
		} else if hashLevel == 1 {
			hashBreakdown["quick_hash"]++
		} else if hashLevel > 1 && hashLevel < 6 {
			hashBreakdown["progressive_hash"]++
		}

		// Add to top groups (we'll sort and limit later)
		hashLevelName := "Unknown"
		if hashLevel == 6 {
			hashLevelName = "Full Hash ✓"
		} else if hashLevel == 1 {
			hashLevelName = "Quick Hash ⚠️"
		} else if hashLevel > 1 && hashLevel < 6 {
			hashLevelName = fmt.Sprintf("Progressive %d 🔄", hashLevel)
		}

		topGroups = append(topGroups, TopGroup{
			Hash:          plan.Group.FileHash,
			FileSize:      plan.Group.TotalSize,
			TotalFiles:    plan.TotalFiles,
			Savings:       plan.SpaceSavings,
			HashLevel:     hashLevel,
			HashLevelName: hashLevelName,
		})
	}

	// Sort top groups by savings (descending) and limit to top 10
	for i := 0; i < len(topGroups); i++ {
		for j := i + 1; j < len(topGroups); j++ {
			if topGroups[j].Savings > topGroups[i].Savings {
				topGroups[i], topGroups[j] = topGroups[j], topGroups[i]
			}
		}
	}
	if len(topGroups) > 10 {
		topGroups = topGroups[:10]
	}

	// Generate warnings
	if hashBreakdown["quick_hash"] > 0 {
		warnings = append(warnings, fmt.Sprintf("%d group(s) use quick hash - verification recommended before linking", hashBreakdown["quick_hash"]))
	}

	log.Printf("Statistics complete. About to execute hardlinks for %d plans (dry_run=%v)...", len(plans), req.DryRun)

	// Execute hardlink creation
	result, err := consolidator.CreateHardlinks(plans, req.DryRun)
	if err != nil {
		log.Printf("ERROR: Hardlink creation failed: %v", err)
		respondError(w, http.StatusInternalServerError, "Hardlink creation failed", "hardlink_failed")
		return
	}

	// Log the result
	log.Printf("Hardlink operation completed: dry_run=%v, groups_processed=%d, files_linked=%d, space_saved=%d, errors=%d",
		result.DryRun, result.GroupsProcessed, result.FilesDeleted, result.SpaceFreed, len(result.Errors))

	// Invalidate stats cache
	s.statsCache.Invalidate()

	// Build active filters description for modal display
	activeFilters := make(map[string]interface{})
	if req.SearchText != "" {
		activeFilters["search"] = req.SearchText
	}
	if req.HashType != "" && req.HashType != "all" {
		activeFilters["hash_type"] = req.HashType
	}
	if req.MinSize > 0 {
		activeFilters["min_size"] = req.MinSize
	}

	// Return enhanced result
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":                  "success",
		"dry_run":                 result.DryRun,
		"groups_processed":        result.GroupsProcessed,
		"files_linked":            result.FilesDeleted, // "Deleted" is used for linked in same-disk
		"files_to_link":           totalFilesToLink,
		"files_already_linked":    totalFilesAlreadyLinked,
		"clusters_needing_link":   totalClustersNeedingLink,
		"clusters_already_linked": totalClustersAlreadyLinked,
		"space_saved":             result.SpaceFreed,
		"hash_breakdown":          hashBreakdown,
		"top_groups":              topGroups,
		"warnings":                warnings,
		"errors":                  result.Errors,
		"files_list":              filesToLink,
		"active_filters":          activeFilters,
	})
}

// HandlePreviewConsolidation generates a preview of consolidation impact
func (s *Server) HandlePreviewConsolidation(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	// Get type parameter (cross-disk or same-disk)
	consolidationType := r.URL.Query().Get("type")
	if consolidationType == "" {
		consolidationType = "cross-disk"
	}

	// Parse filter parameters from query string
	filters := database.DuplicateFilters{
		SearchText: r.URL.Query().Get("search"),
		HashType:   r.URL.Query().Get("hash_type"),
		Limit:      10000, // Large limit to get all matching groups for preview
	}

	// Map "progressive" to "partial" for database query compatibility
	// Progressive hashes at levels 2-5 are stored as hash_type='partial' in the database
	if filters.HashType == "progressive" {
		filters.HashType = "partial"
	}

	// Parse minimum size filter
	if minSizeStr := r.URL.Query().Get("min_size"); minSizeStr != "" {
		switch minSizeStr {
		case "1gb":
			filters.MinSize = 1024 * 1024 * 1024
		case "10gb":
			filters.MinSize = 10 * 1024 * 1024 * 1024
		case "100gb":
			filters.MinSize = 100 * 1024 * 1024 * 1024
		case "1tb":
			filters.MinSize = 1024 * 1024 * 1024 * 1024
		}
	}

	// Parse hash level filter (specific progression level)
	if hashLevelStr := r.URL.Query().Get("hash_level"); hashLevelStr != "" {
		if level, err := strconv.Atoi(hashLevelStr); err == nil && level >= 0 && level <= 6 {
			filters.HashLevel = level
		}
	}

	// Create analyzer
	analyzer := duplicates.NewAnalyzer(s.db, s.diskDetector, &s.config.DuplicateConsolidation)

	// Parse buffer size from config
	bufferSize := 4 * 1024 * 1024 // Default 4MB
	if s.config.DuplicateDetection.HashBufferSize != "" {
		if size, err := disk.ParseSize(s.config.DuplicateDetection.HashBufferSize); err == nil {
			bufferSize = int(size)
		}
	}

	// Create hasher and consolidator
	hasher := scanner.NewFileHasher(s.config.DuplicateDetection.HashAlgorithm, bufferSize)
	consolidator := duplicates.NewConsolidator(s.db, &s.config.DuplicateConsolidation, hasher)

	var plans []*duplicates.ConsolidationPlan
	var err error

	// Get duplicates with filters applied
	if consolidationType == "cross-disk" {
		plans, err = analyzer.AnalyzeCrossDiskDuplicates(filters)
	} else {
		plans, err = analyzer.AnalyzeSameDiskDuplicates(filters)
	}

	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to analyze duplicates", "analysis_failed")
		return
	}

	// Generate preview
	preview := consolidator.PreviewConsolidation(plans)

	// Build active filters description for response
	activeFilters := make(map[string]interface{})
	if filters.SearchText != "" {
		activeFilters["search"] = filters.SearchText
	}
	if filters.HashType != "" && filters.HashType != "all" {
		activeFilters["hash_type"] = filters.HashType
	}
	if filters.MinSize > 0 {
		activeFilters["min_size"] = filters.MinSize
	}

	// Return preview data
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"type":                   consolidationType,
		"total_groups":           preview.TotalGroups,
		"total_files_to_process": preview.TotalFilesToDelete,
		"total_space_saved":      preview.TotalSpaceSaved,
		"disk_impacts":           preview.DiskImpacts,
		"active_filters":         activeFilters,
	})
}

// HandleRefreshGroupInodes refreshes inodes from the filesystem for all files in a duplicate group
func (s *Server) HandleRefreshGroupInodes(w http.ResponseWriter, r *http.Request) {
	// Support both GET and POST
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "Method not allowed", "method_not_allowed")
		return
	}

	// Get group hash parameter
	groupHash := r.URL.Query().Get("group_hash")
	if groupHash == "" {
		respondError(w, http.StatusBadRequest, "Missing group_hash parameter", "missing_parameter")
		return
	}

	// Get all files in the duplicate group
	files, err := s.db.GetDuplicateFilesByHash(groupHash)
	if err != nil {
		log.Printf("ERROR: Failed to get files for group %s: %v", groupHash, err)
		respondError(w, http.StatusInternalServerError, "Failed to get duplicate group", "query_failed")
		return
	}

	if len(files) == 0 {
		respondError(w, http.StatusNotFound, "Duplicate group not found", "not_found")
		return
	}

	// Refresh inode for each file
	updated := 0
	errors := []string{}

	for _, file := range files {
		// Stat the file to get current inode/device
		var stat syscall.Stat_t
		if err := syscall.Stat(file.Path, &stat); err != nil {
			errMsg := fmt.Sprintf("Failed to stat %s: %v", file.Path, err)
			log.Printf("WARNING: %s", errMsg)
			errors = append(errors, errMsg)
			continue
		}

		// Update database with current inode
		if err := s.db.UpdateFileInode(file.Path, uint64(stat.Dev), uint64(stat.Ino)); err != nil {
			errMsg := fmt.Sprintf("Failed to update database for %s: %v", file.Path, err)
			log.Printf("WARNING: %s", errMsg)
			errors = append(errors, errMsg)
			continue
		}

		updated++
	}

	log.Printf("Refreshed inodes for group %s: %d updated, %d errors", groupHash, updated, len(errors))

	// Return result
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"updated": updated,
		"total":   len(files),
		"errors":  errors,
	})
}

// HandleGetMissingFiles returns missing files from the latest scan as JSON
