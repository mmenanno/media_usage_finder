package server

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/mmenanno/media-usage-finder/internal/database"
)

func (s *Server) HandleAdminClearFiles(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	orphanedOnly := r.URL.Query().Get("orphaned") == "true"

	var count int64
	var err error
	var message string

	if orphanedOnly {
		count, err = s.db.ClearOrphanedFiles()
		message = fmt.Sprintf("Cleared %d orphaned files", count)
	} else {
		count, err = s.db.ClearAllFiles()
		message = fmt.Sprintf("Cleared %d files", count)
	}

	if err != nil {
		log.Printf("Failed to clear files: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to clear files", "clear_failed")
		return
	}

	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
	})
}

// HandleAdminClearScans handles clearing scan history
func (s *Server) HandleAdminClearScans(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	count, err := s.db.ClearScans()
	if err != nil {
		log.Printf("Failed to clear scans: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to clear scan history", "clear_failed")
		return
	}

	message := fmt.Sprintf("Cleared %d scan records", count)
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
	})
}

// HandleAdminClearUsage handles clearing service usage records
func (s *Server) HandleAdminClearUsage(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	service := r.URL.Query().Get("service")

	var count int64
	var err error
	var message string

	if service != "" {
		err = s.db.DeleteUsageByService(r.Context(), service)
		if err != nil {
			log.Printf("Failed to clear usage for service %s: %v", service, err)
			respondError(w, http.StatusInternalServerError, "Failed to clear usage", "clear_failed")
			return
		}
		// We don't get a count from DeleteUsageByService, so we'll just report success
		message = fmt.Sprintf("Cleared usage records for %s", service)
		count = 0 // Unknown count
	} else {
		count, err = s.db.ClearAllUsage()
		if err != nil {
			log.Printf("Failed to clear all usage: %v", err)
			respondError(w, http.StatusInternalServerError, "Failed to clear usage", "clear_failed")
			return
		}
		message = fmt.Sprintf("Cleared %d usage records", count)
	}

	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
	})
}

// HandleAdminVacuum handles database vacuum and analyze
func (s *Server) HandleAdminVacuum(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if err := s.db.VacuumDatabase(); err != nil {
		log.Printf("Failed to vacuum database: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to optimize database", "vacuum_failed")
		return
	}

	message := "Database optimized successfully"
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
	})
}

// HandleAdminRebuildFTS handles rebuilding the full-text search index
func (s *Server) HandleAdminRebuildFTS(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if err := s.db.RebuildFTSIndex(); err != nil {
		log.Printf("Failed to rebuild FTS index: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to rebuild search index", "rebuild_failed")
		return
	}

	message := "Search index rebuilt successfully"
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
	})
}

// HandleAdminCleanStaleScans handles cleaning up stale running scans
func (s *Server) HandleAdminCleanStaleScans(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	count, err := s.db.CleanStaleScans()
	if err != nil {
		log.Printf("Failed to clean stale scans: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to clean stale scans", "clean_failed")
		return
	}

	message := fmt.Sprintf("Cleaned %d stale scans", count)
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
	})
}

// HandleAdminRecalculateOrphaned handles recalculating orphaned file status
func (s *Server) HandleAdminRecalculateOrphaned(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	if err := s.db.UpdateOrphanedStatus(r.Context()); err != nil {
		log.Printf("Failed to recalculate orphaned status: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to recalculate orphaned status", "recalc_failed")
		return
	}

	message := "Orphaned status recalculated successfully"
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
	})
}

// HandleAdminDatabaseStats returns database statistics as JSON
func (s *Server) HandleAdminDatabaseStats(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	stats, err := s.db.GetDatabaseStats()
	if err != nil {
		log.Printf("Failed to get database stats: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to get database statistics", "stats_failed")
		return
	}

	respondJSON(w, http.StatusOK, stats)
}

// HandleAdminAuditLog returns paginated audit log entries
func (s *Server) HandleAdminAuditLog(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	limit := 50
	offset := 0

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if parsed, err := strconv.Atoi(offsetStr); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	// Create filters for audit log query
	filters := database.AuditLogFilters{
		Limit:  limit,
		Offset: offset,
	}

	entries, total, err := s.db.GetAuditLog(filters)
	if err != nil {
		log.Printf("Failed to get audit log: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to get audit log", "audit_failed")
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"entries": entries,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

// HandleAdminClearConfig handles clearing all configuration
func (s *Server) HandleAdminClearConfig(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	count, err := s.db.ClearConfig()
	if err != nil {
		log.Printf("Failed to clear config: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to clear configuration", "clear_failed")
		return
	}

	message := fmt.Sprintf("Cleared %d configuration values", count)
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
	})
}

// HandleAdminClearAuditLog handles clearing old audit log entries
func (s *Server) HandleAdminClearAuditLog(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	days := 90 // Default to 90 days
	if daysStr := r.URL.Query().Get("days"); daysStr != "" {
		if parsed, err := strconv.Atoi(daysStr); err == nil && parsed > 0 {
			days = parsed
		}
	}

	count, err := s.db.ClearAuditLog(days)
	if err != nil {
		log.Printf("Failed to clear audit log: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to clear audit log", "clear_failed")
		return
	}

	message := fmt.Sprintf("Cleared %d audit log entries older than %d days", count, days)
	w.Header().Set("X-Toast-Message", message)
	w.Header().Set("X-Toast-Type", "success")

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": message,
		"count":   count,
		"days":    days,
	})
}

// HandleAdminVacuumInfo returns freelist + database-size info so the UI can
// preview how much space a VACUUM would reclaim before the user confirms.
func (s *Server) HandleAdminVacuumInfo(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	info, err := s.db.GetFreelistInfo()
	if err != nil {
		log.Printf("Failed to get vacuum info: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to read database info", "vacuum_info_failed")
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"freelist_count":  info.FreelistCount,
		"page_size":       info.PageSize,
		"reclaimable_kb":  info.ReclaimableKB,
		"database_kb":     info.DatabaseKB,
		"scan_in_flight":  s.scanner != nil && s.scanner.IsBusy(),
	})
}

// HandleAdminMaintenanceStatus returns last-run timestamps for each
// maintenance operation plus current freelist info — feeds the
// "Maintenance" widget on the Advanced page.
func (s *Server) HandleAdminMaintenanceStatus(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	status, err := s.db.GetMaintenanceStatus()
	if err != nil {
		log.Printf("Failed to get maintenance status: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to read maintenance status", "maintenance_status_failed")
		return
	}

	toUnix := func(t *database.MaintenanceStatus) map[string]interface{} {
		out := map[string]interface{}{}
		if t.LastOptimize != nil {
			out["last_optimize"] = t.LastOptimize.Unix()
		}
		if t.LastIncrementalVacuum != nil {
			out["last_incremental_vacuum"] = t.LastIncrementalVacuum.Unix()
		}
		if t.LastFullVacuum != nil {
			out["last_full_vacuum"] = t.LastFullVacuum.Unix()
		}
		if t.LastWALCheckpoint != nil {
			out["last_wal_checkpoint"] = t.LastWALCheckpoint.Unix()
		}
		return out
	}

	resp := toUnix(status)
	resp["auto_vacuum"] = int(status.AutoVacuum)
	resp["freelist_count"] = status.Freelist.FreelistCount
	resp["page_size"] = status.Freelist.PageSize
	resp["reclaimable_kb"] = status.Freelist.ReclaimableKB
	resp["database_kb"] = status.Freelist.DatabaseKB
	resp["interval_seconds"] = int(s.config.DBMaintenanceInterval.Seconds())

	respondJSON(w, http.StatusOK, resp)
}
