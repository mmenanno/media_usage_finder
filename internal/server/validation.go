package server

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// ValidateFilter validates a SQL WHERE clause filter to prevent injection
// NOTE: This function is currently unused as all filters use predefined allowlists
// in the database package (see MarkFilesForRescan). Kept for potential future use
// but all current implementations should use the safer allowlist approach.
//
// DEPRECATED: Use database.MarkFilesForRescan with predefined filter types instead
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

// ValidateURL validates a URL for service configuration
func ValidateURL(urlStr string) error {
	if urlStr == "" {
		return nil // Empty is okay - service might not be configured
	}

	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("invalid URL format: %w", err)
	}

	// Must have a scheme
	if parsedURL.Scheme == "" {
		return fmt.Errorf("URL must include a scheme (http:// or https://)")
	}

	// Only allow http and https
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("URL scheme must be http or https")
	}

	// Must have a host
	if parsedURL.Host == "" {
		return fmt.Errorf("URL must include a host")
	}

	return nil
}

// ValidateAPIKey validates an API key format (basic validation)
func ValidateAPIKey(apiKey string) error {
	if apiKey == "" {
		return nil // Empty is okay - service might not be configured
	}

	// API keys should be alphanumeric (possibly with some special chars)
	// This is a basic check - adjust based on actual API key formats
	matched, err := regexp.MatchString(`^[a-zA-Z0-9\-_+=/.]+$`, apiKey)
	if err != nil {
		return err
	}

	if !matched {
		return fmt.Errorf("API key contains invalid characters")
	}

	// Reasonable length check (most API keys are 20-64 chars)
	if len(apiKey) < 10 {
		return fmt.Errorf("API key is too short (minimum 10 characters)")
	}

	if len(apiKey) > 256 {
		return fmt.Errorf("API key is too long (maximum 256 characters)")
	}

	return nil
}
