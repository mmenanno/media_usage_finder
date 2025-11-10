package api

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// CalibreClient handles communication with Calibre via direct SQLite access
type CalibreClient struct {
	libraryPath string
	dbPath      string
	timeout     time.Duration
}

// CalibreFile represents a file tracked by Calibre
type CalibreFile struct {
	Path       string
	Size       int64
	BookID     int64
	Title      string
	Author     string
	Series     string
	SeriesIndex float64
	Format     string
}

// NewCalibreClient creates a new Calibre client
func NewCalibreClient(libraryPath, dbPath string, timeout time.Duration) *CalibreClient {
	return &CalibreClient{
		libraryPath: libraryPath,
		dbPath:      dbPath,
		timeout:     timeout,
	}
}

// Test tests the connection to Calibre database
func (c *CalibreClient) Test() error {
	// Try to open the database in read-only mode
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro", c.dbPath))
	if err != nil {
		return fmt.Errorf("failed to open Calibre database at %s: %w. Check the database path is correct and accessible", c.dbPath, err)
	}
	defer db.Close()

	// Set connection timeout
	db.SetConnMaxLifetime(c.timeout)

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to connect to Calibre database at %s: %w. Check the database file exists and is readable", c.dbPath, err)
	}

	// Verify it's a Calibre database by checking for required tables
	var tableName string
	err = db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='books'").Scan(&tableName)
	if err != nil {
		return fmt.Errorf("calibre database verification failed: %w. The database at %s does not appear to be a valid Calibre library", err, c.dbPath)
	}

	// Get count of books
	var bookCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM books").Scan(&bookCount)
	if err != nil {
		return fmt.Errorf("failed to query Calibre database: %w", err)
	}

	log.Printf("Connected to Calibre database with %d books", bookCount)
	return nil
}

// GetAllFiles retrieves all files tracked by Calibre
func (c *CalibreClient) GetAllFiles(ctx context.Context) ([]CalibreFile, error) {
	// Open database in read-only mode
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro", c.dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open Calibre database: %w", err)
	}
	defer db.Close()

	// Set connection timeout
	db.SetConnMaxLifetime(c.timeout)

	// Query to get all books and their files
	query := `
		SELECT
			b.id,
			b.path,
			b.title,
			COALESCE(b.author_sort, ''),
			COALESCE((SELECT name FROM series WHERE id = b.series), ''),
			COALESCE(b.series_index, 0),
			d.format,
			d.name,
			COALESCE(d.uncompressed_size, 0)
		FROM books b
		JOIN data d ON b.id = d.book
		ORDER BY b.id
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query Calibre database: %w", err)
	}
	defer rows.Close()

	var files []CalibreFile
	for rows.Next() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var (
			bookID      int64
			bookPath    string
			title       string
			author      string
			series      string
			seriesIndex float64
			format      string
			name        string
			size        int64
		)

		if err := rows.Scan(&bookID, &bookPath, &title, &author, &series, &seriesIndex, &format, &name, &size); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Construct full file path: {library_path}/{book.path}/{name}.{format}
		fullPath := filepath.Join(c.libraryPath, bookPath, fmt.Sprintf("%s.%s", name, strings.ToLower(format)))

		files = append(files, CalibreFile{
			Path:        fullPath,
			Size:        size,
			BookID:      bookID,
			Title:       title,
			Author:      author,
			Series:      series,
			SeriesIndex: seriesIndex,
			Format:      format,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	log.Printf("Total Calibre files found: %d", len(files))
	return files, nil
}

// GetSampleFile retrieves a single sample file from Calibre that matches the path prefix
// This is optimized for path mapping validation - it stops as soon as it finds one matching file
func (c *CalibreClient) GetSampleFile(pathPrefix string) (string, error) {
	// Use background context (not cancellable)
	ctx := context.Background()

	// Open database in read-only mode
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro", c.dbPath))
	if err != nil {
		return "", fmt.Errorf("failed to open Calibre database: %w", err)
	}
	defer db.Close()

	// Set connection timeout
	db.SetConnMaxLifetime(c.timeout)

	// Query to get first file
	query := `
		SELECT
			b.path,
			d.format,
			d.name
		FROM books b
		JOIN data d ON b.id = d.book
		LIMIT 1
	`

	var (
		bookPath string
		format   string
		name     string
	)

	err = db.QueryRowContext(ctx, query).Scan(&bookPath, &format, &name)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil // No files found
		}
		return "", fmt.Errorf("failed to query sample file: %w", err)
	}

	// Construct full file path
	fullPath := filepath.Join(c.libraryPath, bookPath, fmt.Sprintf("%s.%s", name, strings.ToLower(format)))

	// Check if it matches the prefix (if specified)
	if pathPrefix != "" && !strings.HasPrefix(fullPath, pathPrefix) {
		// Try to find one that matches
		query = `
			SELECT
				b.path,
				d.format,
				d.name
			FROM books b
			JOIN data d ON b.id = d.book
			LIMIT 10
		`

		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return "", fmt.Errorf("failed to query sample files: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			if err := rows.Scan(&bookPath, &format, &name); err != nil {
				continue
			}

			testPath := filepath.Join(c.libraryPath, bookPath, fmt.Sprintf("%s.%s", name, strings.ToLower(format)))
			if strings.HasPrefix(testPath, pathPrefix) {
				return testPath, nil
			}
		}
	}

	return fullPath, nil
}
