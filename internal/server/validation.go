package server

import (
	"fmt"
	"regexp"
	"strings"
)

// ValidateFilter validates a SQL WHERE clause filter to prevent injection
// Only allows safe patterns for file filtering
func ValidateFilter(filter string) error {
	if filter == "" {
		return fmt.Errorf("filter cannot be empty")
	}

	// Allowed patterns for filters
	allowedPatterns := []string{
		`^is_orphaned\s*=\s*[01]$`,       // is_orphaned = 0/1
		`^id\s*=\s*\d+$`,                 // id = 123
		`^size\s*[<>=]+\s*\d+$`,          // size > 100000
		`^path\s+LIKE\s+'[^']*'$`,        // path LIKE '%something%'
		`^modified_time\s*[<>=]+\s*\d+$`, // modified_time > 12345
	}

	// Check if filter matches any allowed pattern
	for _, pattern := range allowedPatterns {
		matched, err := regexp.MatchString(pattern, strings.TrimSpace(filter))
		if err != nil {
			return fmt.Errorf("invalid filter pattern: %w", err)
		}
		if matched {
			return nil
		}
	}

	return fmt.Errorf("filter does not match any allowed pattern")
}

// ValidatePathSegment validates a path segment to prevent path traversal
func ValidatePathSegment(path string) error {
	if path == "" {
		return fmt.Errorf("path cannot be empty")
	}

	// Check for path traversal attempts
	if strings.Contains(path, "..") {
		return fmt.Errorf("path traversal detected")
	}

	// Check for null bytes
	if strings.Contains(path, "\x00") {
		return fmt.Errorf("invalid characters in path")
	}

	return nil
}

// ValidateLimit validates pagination limit
func ValidateLimit(limit int) int {
	if limit < 1 {
		return 50 // default
	}
	if limit > 1000 {
		return 1000 // max
	}
	return limit
}

// ValidatePage validates pagination page number
func ValidatePage(page int) int {
	if page < 1 {
		return 1
	}
	return page
}
