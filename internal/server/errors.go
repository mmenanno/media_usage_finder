package server

import (
	"encoding/json"
	"net/http"
)

// ErrorResponse represents a structured error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    string `json:"code,omitempty"`
	Details string `json:"details,omitempty"`
}

// respondJSON sends a JSON response
func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// respondError sends a structured error response
func respondError(w http.ResponseWriter, status int, message, code string) {
	respondJSON(w, status, ErrorResponse{
		Error: message,
		Code:  code,
	})
}

// respondSuccess sends a success response with optional data
func respondSuccess(w http.ResponseWriter, message string, data map[string]interface{}) {
	response := map[string]interface{}{
		"status":  "success",
		"message": message,
	}
	for k, v := range data {
		response[k] = v
	}
	respondJSON(w, http.StatusOK, response)
}
