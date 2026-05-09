package server

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/database"
)

func (s *Server) HandleGetScanLogs(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for filtering
	filters := database.LogFilters{
		Level:      r.URL.Query().Get("level"),
		Phase:      r.URL.Query().Get("phase"),
		SearchText: r.URL.Query().Get("search"),
		Limit:      100, // Default limit
		Offset:     0,
	}

	// Parse scan_id filter
	if scanIDStr := r.URL.Query().Get("scan_id"); scanIDStr != "" && scanIDStr != "all" {
		scanID, err := strconv.ParseInt(scanIDStr, 10, 64)
		if err == nil {
			filters.ScanID = &scanID
		}
	}

	// Parse pagination
	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		page, _ := strconv.Atoi(pageStr)
		if page > 0 {
			filters.Offset = (page - 1) * filters.Limit
		}
	}

	// Parse limit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		limit, _ := strconv.Atoi(limitStr)
		if limit > 0 && limit <= 1000 {
			filters.Limit = limit
		}
	}

	// Parse date range filters
	if startTimeStr := r.URL.Query().Get("start_time"); startTimeStr != "" {
		if startTime, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
			filters.StartTime = &startTime
		}
	}

	if endTimeStr := r.URL.Query().Get("end_time"); endTimeStr != "" {
		if endTime, err := time.Parse(time.RFC3339, endTimeStr); err == nil {
			filters.EndTime = &endTime
		}
	}

	// Get logs with filters
	logs, err := s.db.GetScanLogs(filters)
	if err != nil {
		log.Printf("ERROR: Failed to get scan logs: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to retrieve logs", "database_error")
		return
	}

	// Get total count for pagination
	total, err := s.db.GetScanLogCount(filters)
	if err != nil {
		log.Printf("ERROR: Failed to get log count: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to count logs", "database_error")
		return
	}

	// Calculate pagination info
	currentPage := (filters.Offset / filters.Limit) + 1
	totalPages := (total + filters.Limit - 1) / filters.Limit

	// Check if this is an HTMX request
	isHTMX := r.Header.Get("HX-Request") == "true"

	data := map[string]interface{}{
		"Logs":       logs,
		"Total":      total,
		"Page":       currentPage,
		"TotalPages": totalPages,
		"Filters":    filters,
		"IsHTMX":     isHTMX,
	}

	if isHTMX {
		// Return just the logs table fragment for HTMX updates
		tmplSet, ok := s.templates["logs_table.html"]
		if !ok {
			http.Error(w, "logs_table template not found", http.StatusInternalServerError)
			return
		}

		if err := tmplSet.ExecuteTemplate(w, "logs_table.html", data); err != nil {
			log.Printf("ERROR: Failed to execute template: %v", err)
			http.Error(w, "Failed to render logs", http.StatusInternalServerError)
			return
		}
	} else {
		// Return JSON for API requests
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	}
}

// HandleGetAuditLogs handles requests for filtered audit log entries
func (s *Server) HandleGetAuditLogs(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for filtering
	action := r.URL.Query().Get("action")
	if action == "all" {
		action = ""
	}

	entityType := r.URL.Query().Get("entity_type")
	if entityType == "all" {
		entityType = ""
	}

	filters := database.AuditLogFilters{
		Action:     action,
		EntityType: entityType,
		SearchText: r.URL.Query().Get("search"),
		Limit:      100, // Default limit
		Offset:     0,
	}

	// Parse scan_id filter
	if scanIDStr := r.URL.Query().Get("scan_id"); scanIDStr != "" {
		scanID, err := strconv.ParseInt(scanIDStr, 10, 64)
		if err == nil {
			filters.ScanID = &scanID
		}
	}

	// Parse pagination
	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		page, _ := strconv.Atoi(pageStr)
		if page > 0 {
			filters.Offset = (page - 1) * filters.Limit
		}
	}

	// Parse limit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		limit, _ := strconv.Atoi(limitStr)
		if limit > 0 && limit <= 1000 {
			filters.Limit = limit
		}
	}

	// Get audit logs with filters
	entries, total, err := s.db.GetAuditLog(filters)
	if err != nil {
		log.Printf("ERROR: Failed to get audit logs: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to retrieve audit logs", "database_error")
		return
	}

	// Calculate pagination info
	currentPage := (filters.Offset / filters.Limit) + 1
	totalPages := (total + filters.Limit - 1) / filters.Limit

	// Check if this is an HTMX request
	isHTMX := r.Header.Get("HX-Request") == "true"

	data := map[string]interface{}{
		"Entries":    entries,
		"Total":      total,
		"Page":       currentPage,
		"TotalPages": totalPages,
		"Filters":    filters,
		"IsHTMX":     isHTMX,
	}

	if isHTMX {
		// Return just the audit logs table fragment for HTMX updates
		tmplSet, ok := s.templates["audit_logs_table.html"]
		if !ok {
			http.Error(w, "audit_logs_table template not found", http.StatusInternalServerError)
			return
		}

		if err := tmplSet.ExecuteTemplate(w, "audit_logs_table.html", data); err != nil {
			log.Printf("ERROR: Failed to execute template: %v", err)
			http.Error(w, "Failed to render audit logs", http.StatusInternalServerError)
			return
		}
	} else {
		// Return JSON for API requests
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	}
}

// HandleHealth serves the health check endpoint with detailed status
