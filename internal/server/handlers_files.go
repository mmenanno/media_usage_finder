package server

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/constants"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
)

func (s *Server) HandleFiles(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	page = ValidatePage(page)

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	limit = ValidateLimit(limit)

	offset := (page - 1) * limit

	orphanedOnly := r.URL.Query().Get("orphaned") == "true"
	hardlinksOnly := r.URL.Query().Get("hardlink") == "true"
	search := r.URL.Query().Get("search")
	orderBy := r.URL.Query().Get("order")
	direction := r.URL.Query().Get("direction")

	// Parse services filter (can be comma-separated or multiple params)
	var services []string
	if servicesParam := r.URL.Query().Get("services"); servicesParam != "" {
		for _, svc := range strings.Split(servicesParam, ",") {
			svc = strings.TrimSpace(strings.ToLower(svc))
			if svc != "" {
				services = append(services, svc)
			}
		}
	}
	// Support legacy single service parameter for backward compatibility
	if len(services) == 0 {
		if service := r.URL.Query().Get("service"); service != "" {
			services = []string{service}
		}
	}

	// Parse service filter mode: "any", "all", or "exact"
	serviceFilterMode := r.URL.Query().Get("service_filter_mode")
	if serviceFilterMode == "" {
		serviceFilterMode = "any" // default
	}

	// Parse extensions filter (can be comma-separated or multiple params)
	var extensions []string
	if extParam := r.URL.Query().Get("extensions"); extParam != "" {
		for _, ext := range strings.Split(extParam, ",") {
			ext = strings.TrimSpace(strings.ToLower(ext))
			if ext != "" {
				extensions = append(extensions, ext)
			}
		}
	}

	// Parse device names filter and convert to device IDs
	var deviceIDs []int64
	var deviceNames []string
	if devicesParam := r.URL.Query().Get("devices"); devicesParam != "" && s.diskDetector != nil {
		for _, devName := range strings.Split(devicesParam, ",") {
			devName = strings.TrimSpace(devName)
			if devName == "" {
				continue
			}
			deviceNames = append(deviceNames, devName)
			// Find the disk by name and get its device ID
			for _, disk := range s.diskDetector.GetAllDisks() {
				if disk.Name == devName {
					deviceIDs = append(deviceIDs, disk.DeviceID)
					break
				}
			}
		}
	}

	var files []*database.File
	var total int
	var err error

	if search != "" {
		files, total, err = s.db.SearchFiles(search, orphanedOnly, services, serviceFilterMode, hardlinksOnly, extensions, deviceIDs, limit, offset, orderBy, direction)
	} else {
		files, total, err = s.db.ListFiles(orphanedOnly, services, serviceFilterMode, hardlinksOnly, extensions, deviceIDs, limit, offset, orderBy, direction)
	}

	if err != nil {
		log.Printf("ERROR: Failed to list files: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to list files. The database may be locked or experiencing issues", "database_error")
		return
	}

	// Get total unfiltered count for comparison
	totalUnfiltered, err := s.db.GetCurrentFileCount()
	if err != nil {
		log.Printf("WARNING: Failed to get total unfiltered count: %v", err)
		totalUnfiltered = int64(total) // Fallback to filtered count if we can't get total
	}

	// Batch load usage for all files (fixes N+1 query problem)
	fileIDs := make([]int64, len(files))
	for i, file := range files {
		fileIDs[i] = file.ID
	}

	usageMap, err := s.db.GetUsageByFileIDs(fileIDs)
	if err != nil {
		// Log error but continue with empty usage
		usageMap = make(map[int64][]*database.Usage)
	}

	// Batch load disk locations for all files (if disk scanning is configured)
	diskLocationsMap := make(map[int64][]*database.FileDiskLocation)
	if len(s.config.Disks) > 0 {
		diskLocationsMap, err = s.db.GetDiskLocationsByFileIDs(fileIDs)
		if err != nil {
			// Log error but continue with empty disk locations
			diskLocationsMap = make(map[int64][]*database.FileDiskLocation)
		}
	}

	filesWithUsage := make([]map[string]interface{}, 0, len(files))
	for _, file := range files {
		filesWithUsage = append(filesWithUsage, map[string]interface{}{
			"File":          file,
			"Usage":         usageMap[file.ID],
			"DiskLocations": diskLocationsMap[file.ID],
		})
	}

	// For backward compatibility, set Service if there's exactly one service
	legacyService := ""
	if len(services) == 1 {
		legacyService = services[0]
	}

	// Get available disks for filter dropdown
	var availableDisks []*disk.DiskInfo
	if s.diskDetector != nil {
		availableDisks = s.diskDetector.GetAllDisks()
	}

	data := FilesData{
		Files:                     filesWithUsage,
		Total:                     int64(total),
		TotalUnfiltered:           totalUnfiltered,
		Page:                      int64(page),
		Limit:                     limit,
		TotalPages:                CalculateTotalPages(total, limit),
		Title:                     "Files",
		Orphaned:                  orphanedOnly,
		Hardlinks:                 hardlinksOnly,
		Service:                   legacyService,
		Services:                  services,
		ServiceFilterMode:         serviceFilterMode,
		Search:                    search,
		OrderBy:                   orderBy,
		Direction:                 direction,
		Extensions:                extensions,
		Devices:                   deviceNames,
		AvailableDisks:            availableDisks,
		DiskResolver:              s.diskResolver,
		HasDiskLocations:          len(s.config.Disks) > 0,
		Version:                   s.version,
		DeleteFilesFromFilesystem: s.config.DeleteFilesFromFilesystem,
	}

	s.renderTemplate(w, "files.html", data)
}

// HandleGetFileExtensions returns a JSON list of distinct file extensions
func (s *Server) HandleGetFileExtensions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse filter parameters
	orphanedOnly := r.URL.Query().Get("orphaned") == "true"
	service := r.URL.Query().Get("service")

	extensions, err := s.db.GetFileExtensions(orphanedOnly, service)
	if err != nil {
		log.Printf("ERROR: Failed to get file extensions: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to get file extensions", "database_error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"extensions": extensions,
	})
}

// HandleConfig serves the configuration page
func (s *Server) HandleExport(w http.ResponseWriter, r *http.Request) {
	format := r.URL.Query().Get("format")
	orphanedOnly := r.URL.Query().Get("orphaned") == "true"

	// Validate format before writing response
	if format != "json" && format != "csv" {
		http.Error(w, "Invalid format. Supported formats: json, csv", http.StatusBadRequest)
		return
	}

	// Stream files in batches to avoid loading everything into memory
	batchSize := constants.ExportBatchSize
	offset := 0

	switch format {
	case "json":
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=files.json")

		// Write opening bracket
		w.Write([]byte("[\n"))

		first := true
		for {
			files, _, err := s.db.ListFiles(orphanedOnly, nil, "any", false, nil, nil, batchSize, offset, "path", "asc")
			if err != nil {
				if offset == 0 {
					http.Error(w, "Failed to list files", http.StatusInternalServerError)
				}
				return
			}

			if len(files) == 0 {
				break
			}

			for _, file := range files {
				if !first {
					w.Write([]byte(",\n"))
				}
				first = false

				// Stream each file entry - marshal manually to avoid newline issues
				data, err := json.Marshal(file)
				if err != nil {
					log.Printf("Failed to marshal file: %v", err)
					continue
				}
				w.Write(data)
			}

			offset += batchSize

			// Flush to client
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}

		// Write closing bracket
		w.Write([]byte("\n]"))

	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=files.csv")

		// Create CSV writer for proper escaping
		csvWriter := csv.NewWriter(w)
		defer csvWriter.Flush()

		// Write CSV header
		if err := csvWriter.Write([]string{"path", "size", "is_orphaned"}); err != nil {
			http.Error(w, "Failed to write CSV header", http.StatusInternalServerError)
			return
		}

		for {
			files, _, err := s.db.ListFiles(orphanedOnly, nil, "any", false, nil, nil, batchSize, offset, "path", "asc")
			if err != nil {
				if offset == 0 {
					http.Error(w, "Failed to list files", http.StatusInternalServerError)
				}
				return
			}

			if len(files) == 0 {
				break
			}

			// Stream CSV rows in batches
			for _, file := range files {
				record := []string{
					file.Path,
					fmt.Sprintf("%d", file.Size),
					fmt.Sprintf("%v", file.IsOrphaned),
				}
				if err := csvWriter.Write(record); err != nil {
					log.Printf("Failed to write CSV record: %v", err)
					continue
				}
			}

			csvWriter.Flush()
			offset += batchSize

			// Flush to client
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}

	default:
		// This should never be reached due to validation at start
		http.Error(w, "Invalid format", http.StatusBadRequest)
	}
}

// HandleDeleteFile deletes a file or files
func (s *Server) HandleDeleteFile(w http.ResponseWriter, r *http.Request) {
	if !requireAnyMethod(w, r, http.MethodPost, http.MethodDelete) {
		return
	}

	fileID := r.URL.Query().Get("id")
	orphaned := r.URL.Query().Get("orphaned") == "true"

	// Get config setting for filesystem deletion
	deleteFromFilesystem := s.config.DeleteFilesFromFilesystem

	// Single file deletion
	if fileID != "" {
		id, err := strconv.ParseInt(fileID, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, "Invalid file ID", "invalid_file_id")
			return
		}

		if err := s.db.DeleteFile(id, "UI deletion", deleteFromFilesystem); err != nil {
			// Log the error for debugging
			log.Printf("ERROR: Failed to delete file ID %d: %v", id, err)

			// Provide specific error message for filesystem failures
			if deleteFromFilesystem && strings.Contains(err.Error(), "failed to delete file from filesystem") {
				respondError(w, http.StatusInternalServerError, err.Error(), "filesystem_delete_failed")
			} else {
				respondError(w, http.StatusInternalServerError, "Failed to delete file", "delete_failed")
			}
			return
		}

		// Update success message based on deletion mode
		var successMsg string
		if deleteFromFilesystem {
			successMsg = "File deleted from filesystem and database"
		} else {
			successMsg = "File removed from database"
		}

		w.Header().Set("X-Toast-Message", successMsg)
		w.Header().Set("X-Toast-Type", "success")
		respondSuccess(w, successMsg, nil)
		return
	}

	// Bulk orphaned files deletion
	if orphaned {
		// Process files in batches to avoid loading everything into memory
		batchSize := 1000
		offset := 0
		totalDeleted := 0
		totalErrors := 0

		for {
			// Fetch batch of orphaned files
			files, _, err := s.db.ListFiles(true, nil, "any", false, nil, nil, batchSize, offset, "path", "asc")
			if err != nil {
				respondError(w, http.StatusInternalServerError, "Failed to list orphaned files", "list_failed")
				return
			}

			// No more files to process
			if len(files) == 0 {
				break
			}

			// Delete current batch
			for _, file := range files {
				if err := s.db.DeleteFile(file.ID, "Bulk orphaned cleanup", deleteFromFilesystem); err != nil {
					totalErrors++
					// Log the error for debugging
					log.Printf("ERROR: Failed to delete file ID %d (%s): %v", file.ID, file.Path, err)
					continue
				}
				totalDeleted++
			}

			// Move to next batch
			offset += batchSize
		}

		// Check if any files were found
		if totalDeleted == 0 && totalErrors == 0 {
			respondSuccess(w, "No orphaned files to delete", nil)
			return
		}

		// Build success message
		var msg string
		if deleteFromFilesystem {
			msg = fmt.Sprintf("Deleted %d files from filesystem", totalDeleted)
		} else {
			msg = fmt.Sprintf("Removed %d files from database", totalDeleted)
		}

		if totalErrors > 0 {
			msg = fmt.Sprintf("%s (%d errors)", msg, totalErrors)
		}

		w.Header().Set("X-Toast-Message", msg)
		if totalErrors > 0 {
			w.Header().Set("X-Toast-Type", "warning")
		} else {
			w.Header().Set("X-Toast-Type", "success")
		}

		response := BulkDeleteResponse{
			Status:  "success",
			Message: msg,
			Deleted: totalDeleted,
			Errors:  totalErrors,
		}
		respondJSON(w, http.StatusOK, response)
		return
	}

	respondError(w, http.StatusBadRequest, "Must specify file ID or orphaned flag", "missing_parameter")
}

// HandleBatchDeleteFiles deletes multiple files in a single request
func (s *Server) HandleBatchDeleteFiles(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	// Parse request body
	var req BatchDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("ERROR: HandleBatchDeleteFiles - failed to decode JSON body: %v", err)
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err), "invalid_request")
		return
	}

	log.Printf("INFO: HandleBatchDeleteFiles - received %d file IDs for batch deletion", len(req.FileIDs))

	if len(req.FileIDs) == 0 {
		respondError(w, http.StatusBadRequest, "No file IDs provided", "empty_request")
		return
	}

	// Limit batch size to prevent abuse
	if len(req.FileIDs) > 1000 {
		respondError(w, http.StatusBadRequest, "Batch size too large (max 1000)", "batch_too_large")
		return
	}

	// Get config setting for filesystem deletion
	deleteFromFilesystem := s.config.DeleteFilesFromFilesystem

	// Process deletions
	deleted := 0
	failed := 0
	results := make([]BatchDeleteFileResult, 0, len(req.FileIDs))

	for _, fileID := range req.FileIDs {
		if err := s.db.DeleteFile(fileID, "Batch deletion", deleteFromFilesystem); err != nil {
			failed++
			results = append(results, BatchDeleteFileResult{
				FileID:  fileID,
				Success: false,
				Error:   err.Error(),
			})
		} else {
			deleted++
			results = append(results, BatchDeleteFileResult{
				FileID:  fileID,
				Success: true,
			})
		}
	}

	// Build response
	var msg string
	if deleteFromFilesystem {
		msg = fmt.Sprintf("Deleted %d files from filesystem, %d failed", deleted, failed)
	} else {
		msg = fmt.Sprintf("Removed %d files from database, %d failed", deleted, failed)
	}

	response := BatchDeleteResponse{
		Status:  "success",
		Message: msg,
		Deleted: deleted,
		Failed:  failed,
		Results: results,
	}

	w.Header().Set("X-Toast-Message", msg)
	if failed > 0 {
		w.Header().Set("X-Toast-Type", "warning")
	} else {
		w.Header().Set("X-Toast-Type", "success")
	}

	respondJSON(w, http.StatusOK, response)
}

// HandleFileDetails returns detailed information about a specific file
func (s *Server) HandleFileDetails(w http.ResponseWriter, r *http.Request) {
	fileIDStr := r.URL.Query().Get("id")
	if fileIDStr == "" {
		respondError(w, http.StatusBadRequest, "File ID is required", "missing_file_id")
		return
	}

	fileID, err := strconv.ParseInt(fileIDStr, 10, 64)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid file ID", "invalid_file_id")
		return
	}

	// Get file
	file, err := s.db.GetFileByID(fileID)
	if err != nil {
		respondError(w, http.StatusNotFound, "File not found", "file_not_found")
		return
	}

	// Get usage
	usage, err := s.db.GetUsageByFileID(fileID)
	if err != nil {
		usage = []*database.Usage{} // Empty array on error
	}

	// Get hardlinks if applicable (optimized query)
	var hardlinks []string
	if file.Inode != 0 && file.DeviceID != 0 {
		group, err := s.db.GetHardlinksByInodeDevice(file.Inode, file.DeviceID)
		if err == nil && len(group) > 1 {
			hardlinks = make([]string, 0, len(group))
			for _, f := range group {
				hardlinks = append(hardlinks, f.Path)
			}
		}
	}

	// Get disk locations if available
	var diskLocations []*database.FileDiskLocation
	if len(s.config.Disks) > 0 {
		diskLocations, _ = s.db.GetDiskLocationsForFile(fileID)
	}

	response := FileDetailsResponse{
		ID:            file.ID,
		Path:          file.Path,
		Size:          file.Size,
		Inode:         file.Inode,
		DeviceID:      file.DeviceID,
		ModifiedTime:  file.ModifiedTime.Unix(),
		LastVerified:  file.LastVerified.Unix(),
		IsOrphaned:    file.IsOrphaned,
		CreatedAt:     file.CreatedAt.Unix(),
		Usage:         usage,
		Hardlinks:     hardlinks,
		DiskLocations: diskLocations,
	}

	// Resolve device name and color
	// Prefer disk location info if available (more accurate for mergerfs setups)
	if len(diskLocations) > 0 {
		// Use the first disk location (files typically exist on one disk)
		loc := diskLocations[0]
		if s.diskResolver != nil {
			response.DeviceName = s.diskResolver.ResolveDisplayName(loc.DiskDeviceID)
			response.DeviceColor = s.diskResolver.ResolveColor(loc.DiskDeviceID)
		} else {
			response.DeviceName = fmt.Sprintf("%s (%d)", loc.DiskName, loc.DiskDeviceID)
			response.DeviceColor = "blue"
		}
	} else if s.diskResolver != nil {
		// Fallback to file's device_id (for files not yet scanned with disk locations)
		response.DeviceName = s.diskResolver.ResolveDisplayName(file.DeviceID)
		response.DeviceColor = s.diskResolver.ResolveColor(file.DeviceID)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleRescanFiles rescans specific files immediately
func (s *Server) HandleRescanFiles(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	fileIDStr := r.URL.Query().Get("id")
	fileIDsStr := r.URL.Query().Get("ids")
	orphaned := r.URL.Query().Get("orphaned") == "true"

	var paths []string
	var count int64

	// Single file by ID
	if fileIDStr != "" {
		fileID, err := strconv.ParseInt(fileIDStr, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, "Invalid file ID", "invalid_file_id")
			return
		}

		// Get file path from database
		file, err := s.db.GetFileByID(fileID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "Failed to get file", "file_not_found")
			return
		}

		paths = []string{file.Path}
		count = 1
	} else if fileIDsStr != "" {
		// Multiple files by IDs (comma-separated)
		idStrings := strings.Split(fileIDsStr, ",")
		for _, idStr := range idStrings {
			idStr = strings.TrimSpace(idStr)
			if idStr == "" {
				continue
			}

			fileID, err := strconv.ParseInt(idStr, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid file ID: %s", idStr), "invalid_file_id")
				return
			}

			file, err := s.db.GetFileByID(fileID)
			if err != nil {
				log.Printf("Warning: Failed to get file ID %d: %v", fileID, err)
				continue
			}

			paths = append(paths, file.Path)
		}

		count = int64(len(paths))

		if count == 0 {
			respondError(w, http.StatusBadRequest, "No valid file IDs provided", "no_files")
			return
		}
	} else if orphaned {
		// Query orphaned file paths directly
		rows, err := s.db.Conn().Query("SELECT path FROM files WHERE is_orphaned = 1")
		if err != nil {
			respondError(w, http.StatusInternalServerError, "Failed to get orphaned files", "query_failed")
			return
		}
		defer rows.Close()

		for rows.Next() {
			var path string
			if err := rows.Scan(&path); err != nil {
				log.Printf("Warning: Failed to scan orphaned file path: %v", err)
				continue
			}
			paths = append(paths, path)
		}

		if err := rows.Err(); err != nil {
			respondError(w, http.StatusInternalServerError, "Failed to read orphaned files", "query_failed")
			return
		}

		count = int64(len(paths))

		if count == 0 {
			w.Header().Set("X-Toast-Message", "No orphaned files to rescan")
			w.Header().Set("X-Toast-Type", "info")
			response := BulkRescanResponse{
				Status:  "success",
				Message: "No orphaned files to rescan",
				Count:   0,
			}
			respondJSON(w, http.StatusOK, response)
			return
		}
	} else {
		respondError(w, http.StatusBadRequest, "Must specify file ID(s) or orphaned flag", "missing_parameter")
		return
	}

	// Launch rescan in background goroutine
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
		defer cancel()

		if err := s.scanner.RescanFiles(ctx, paths); err != nil {
			log.Printf("ERROR: Rescan failed: %v", err)
		} else {
			log.Printf("INFO: Rescan completed successfully for %d file(s)", len(paths))
		}
	}()

	// Return immediately
	w.Header().Set("X-Toast-Message", fmt.Sprintf("Rescanning %d file(s)...", count))
	w.Header().Set("X-Toast-Type", "info")

	response := BulkRescanResponse{
		Status:  "success",
		Message: fmt.Sprintf("Rescan started for %d file(s)", count),
		Count:   count,
	}
	respondJSON(w, http.StatusOK, response)
}

// Admin/Advanced page handlers

// HandleAdvanced renders the advanced admin page
func (s *Server) HandleGetMissingFiles(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	ctx := r.Context()
	missingFiles, err := s.db.GetLatestMissingFiles(ctx)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to retrieve missing files", "query_failed")
		return
	}

	// Group by service for easier rendering
	type ServiceMissingFiles struct {
		Service string                  `json:"service"`
		Count   int                     `json:"count"`
		Files   []*database.MissingFile `json:"files"`
	}

	groupedByService := make(map[string]*ServiceMissingFiles)
	for _, mf := range missingFiles {
		if _, exists := groupedByService[mf.Service]; !exists {
			groupedByService[mf.Service] = &ServiceMissingFiles{
				Service: mf.Service,
				Files:   []*database.MissingFile{},
			}
		}
		groupedByService[mf.Service].Files = append(groupedByService[mf.Service].Files, mf)
		groupedByService[mf.Service].Count++
	}

	// Convert map to slice for consistent ordering
	result := make([]*ServiceMissingFiles, 0, len(groupedByService))
	for _, group := range groupedByService {
		result = append(result, group)
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"total":    len(missingFiles),
		"services": result,
	})
}

// HandleExportMissingFiles exports missing files as CSV
func (s *Server) HandleExportMissingFiles(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	ctx := r.Context()
	missingFiles, err := s.db.GetLatestMissingFiles(ctx)
	if err != nil {
		http.Error(w, "Failed to retrieve missing files", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=missing_files.csv")

	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	// Write CSV header
	header := []string{
		"Service",
		"Service Path",
		"Translated Path",
		"Size (Bytes)",
		"Size (Human)",
		"Grouping Name",
		"Grouping ID",
	}
	if err := csvWriter.Write(header); err != nil {
		http.Error(w, "Failed to write CSV header", http.StatusInternalServerError)
		return
	}

	// Write data rows
	for _, mf := range missingFiles {
		// Format size in human-readable format
		sizeHuman := disk.FormatBytes(mf.Size)

		record := []string{
			mf.Service,
			mf.ServicePath,
			mf.TranslatedPath,
			fmt.Sprintf("%d", mf.Size),
			sizeHuman,
			mf.ServiceGroup,
			mf.ServiceGroupID,
		}
		if err := csvWriter.Write(record); err != nil {
			log.Printf("Failed to write CSV record: %v", err)
			continue
		}
	}

	csvWriter.Flush()
}
