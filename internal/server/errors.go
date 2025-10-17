package server

import (
	"encoding/json"
	"net/http"
)

// ErrorResponse represents a structured error response
type ErrorResponse struct {
	Error      string `json:"error"`
	Code       string `json:"code,omitempty"`
	Details    string `json:"details,omitempty"`
	Suggestion string `json:"suggestion,omitempty"` // User-friendly suggestion
}

// respondJSON sends a JSON response
func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// respondError sends a structured error response
func respondError(w http.ResponseWriter, status int, message, code string) {
	suggestion := getErrorSuggestion(code)
	respondJSON(w, status, ErrorResponse{
		Error:      message,
		Code:       code,
		Suggestion: suggestion,
	})
}

// getErrorSuggestion returns a user-friendly suggestion based on error code
func getErrorSuggestion(code string) string {
	suggestions := map[string]string{
		"connection_failed":    "Check that the service is running and the URL is correct. Verify network connectivity.",
		"validation_failed":    "Please review the highlighted fields and ensure all required information is provided.",
		"missing_file_id":      "The file ID was not provided in the request.",
		"invalid_file_id":      "The provided file ID is not valid. It should be a number.",
		"file_not_found":       "The file may have been deleted or moved. Try refreshing the page.",
		"list_failed":          "There was an error retrieving the list. Try refreshing the page.",
		"delete_failed":        "The file could not be deleted. Check file permissions and try again.",
		"save_failed":          "Configuration could not be saved. Check file permissions on the config directory.",
		"scan_already_running": "A scan is already in progress. Wait for it to complete or cancel it first.",
		"method_not_allowed":   "This action requires a different request method.",
		"unknown_service":      "The requested service is not recognized. Valid services: plex, sonarr, radarr, qbittorrent.",
		"parse_error":          "The submitted data could not be parsed. Check the form data and try again.",
	}

	if suggestion, ok := suggestions[code]; ok {
		return suggestion
	}

	return "If the problem persists, check the application logs for more details."
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
