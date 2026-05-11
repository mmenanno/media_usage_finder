package server

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/mmenanno/media-usage-finder/internal/database"
)

// HandleListViewsOrCreate — GET /api/views (list) or
// POST /api/views (create). One handler so /api/views (no trailing
// slash) covers both verbs without a sub-mux.
func (s *Server) HandleListViewsOrCreate(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		views, err := s.db.ListSavedViews()
		if err != nil {
			log.Printf("Failed to list saved views: %v", err)
			respondError(w, http.StatusInternalServerError, "Failed to load saved views", "list_views_failed")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"views": viewsToJSON(views),
		})
	case http.MethodPost:
		s.HandleCreateView(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// HandleCreateView — POST /api/views — body:
//
//	{ "name": "My view", "icon": "...", "description": "...",
//	  "filters": { "services": "plex", "extensions": "mkv", ... },
//	  "sort_order": 0 }
//
// Returns 201 Created with the new view's id.
func (s *Server) HandleCreateView(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		Name        string            `json:"name"`
		Icon        string            `json:"icon"`
		Description string            `json:"description"`
		Filters     map[string]string `json:"filters"`
		SortOrder   int               `json:"sort_order"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid JSON body", "bad_json")
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		respondError(w, http.StatusBadRequest, "View name is required", "missing_name")
		return
	}
	if req.Filters == nil {
		req.Filters = map[string]string{}
	}
	id, err := s.db.CreateSavedView(&database.SavedView{
		Name:        strings.TrimSpace(req.Name),
		Icon:        req.Icon,
		Description: req.Description,
		Filters:     req.Filters,
		SortOrder:   req.SortOrder,
	})
	if err != nil {
		log.Printf("Failed to create view: %v", err)
		respondError(w, http.StatusBadRequest, "Failed to save view (name may be duplicate)", "create_view_failed")
		return
	}
	respondJSON(w, http.StatusCreated, map[string]interface{}{"id": id})
}

// HandleViewByID — PATCH/DELETE /api/views/{id}.
//
// PATCH body is the same shape as CreateView. DELETE has no body and
// 404s if the view is system-owned.
func (s *Server) HandleViewByID(w http.ResponseWriter, r *http.Request) {
	id, ok := parseViewID(r.URL.Path)
	if !ok {
		respondError(w, http.StatusBadRequest, "Invalid view id", "bad_id")
		return
	}

	switch r.Method {
	case http.MethodPatch:
		s.updateView(w, r, id)
	case http.MethodDelete:
		s.deleteView(w, id)
	case http.MethodPost:
		// Touch endpoint: POST /api/views/{id}/touch updates last_used.
		// Used by the Files page when a chip is clicked.
		if strings.HasSuffix(r.URL.Path, "/touch") {
			if err := s.db.TouchSavedView(id); err != nil {
				respondError(w, http.StatusInternalServerError, "Failed to touch view", "touch_failed")
				return
			}
			respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		http.Error(w, "Not found", http.StatusNotFound)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) updateView(w http.ResponseWriter, r *http.Request, id int64) {
	existing, err := s.db.GetSavedView(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "View not found", "not_found")
		return
	}

	var req struct {
		Name        *string           `json:"name"`
		Icon        *string           `json:"icon"`
		Description *string           `json:"description"`
		Filters     map[string]string `json:"filters"`
		SortOrder   *int              `json:"sort_order"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid JSON body", "bad_json")
		return
	}

	// Apply only fields the client sent — pointer-typed so we can tell
	// "not provided" apart from "empty string".
	if req.Name != nil {
		existing.Name = strings.TrimSpace(*req.Name)
	}
	if req.Icon != nil {
		existing.Icon = *req.Icon
	}
	if req.Description != nil {
		existing.Description = *req.Description
	}
	if req.Filters != nil {
		existing.Filters = req.Filters
	}
	if req.SortOrder != nil {
		existing.SortOrder = *req.SortOrder
	}

	if err := s.db.UpdateSavedView(existing); err != nil {
		log.Printf("Failed to update view %d: %v", id, err)
		respondError(w, http.StatusInternalServerError, "Failed to update view", "update_failed")
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) deleteView(w http.ResponseWriter, id int64) {
	if err := s.db.DeleteSavedView(id); err != nil {
		// DeleteSavedView returns an error for system-owned views and
		// for unknown IDs. Treat both as 404 — clearer than a 500.
		respondError(w, http.StatusNotFound, "View cannot be deleted (not found or system-owned)", "delete_blocked")
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// parseViewID extracts the numeric id from /api/views/{id}[/touch].
func parseViewID(path string) (int64, bool) {
	const prefix = "/api/views/"
	if !strings.HasPrefix(path, prefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(path, prefix)
	rest = strings.TrimSuffix(rest, "/touch")
	id, err := strconv.ParseInt(rest, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

// viewsToJSON shapes saved views for JSON output. Pointer time fields
// become unix seconds (omitted when nil).
func viewsToJSON(views []*database.SavedView) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(views))
	for _, v := range views {
		row := map[string]interface{}{
			"id":          v.ID,
			"name":        v.Name,
			"icon":        v.Icon,
			"description": v.Description,
			"filters":     v.Filters,
			"sort_order":  v.SortOrder,
			"is_system":   v.IsSystem,
			"created_at":  v.CreatedAt.Unix(),
			"updated_at":  v.UpdatedAt.Unix(),
		}
		if v.LastUsed != nil {
			row["last_used"] = v.LastUsed.Unix()
		}
		out = append(out, row)
	}
	return out
}
