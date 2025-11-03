package scanner

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// FileInfo represents information about a file from the filesystem
type FileInfo struct {
	Path         string
	Size         int64
	ModifiedTime int64
	Inode        int64
	DeviceID     int64
}

// WalkFiles walks the filesystem and sends file info to the channel
// Walks multiple paths in parallel for improved performance
func WalkFiles(ctx context.Context, paths []string, out chan<- FileInfo, progress *Progress) error {
	// Use WaitGroup to track all walking goroutines
	var wg sync.WaitGroup
	errChan := make(chan error, len(paths))

	// Walk each path in parallel
	for _, path := range paths {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()

			err := filepath.WalkDir(p, func(filePath string, d fs.DirEntry, err error) error {
				// Check for context cancellation
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				if err != nil {
					progress.AddError(fmt.Sprintf("Error accessing %s: %v", filePath, err))
					return nil // Continue walking
				}

				// Skip directories
				if d.IsDir() {
					return nil
				}

				// Skip symlinks (we track the actual files they point to)
				if d.Type()&fs.ModeSymlink != 0 {
					return nil
				}

				// Get file info
				info, err := d.Info()
				if err != nil {
					progress.AddError(fmt.Sprintf("Error getting info for %s: %v", filePath, err))
					return nil
				}

				// Get inode and device ID for hardlink detection
				stat, ok := info.Sys().(*syscall.Stat_t)
				if !ok {
					progress.AddError(fmt.Sprintf("Unable to get system stats for %s", filePath))
					return nil
				}

				fileInfo := FileInfo{
					Path:         filePath,
					Size:         info.Size(),
					ModifiedTime: info.ModTime().Unix(),
					Inode:        int64(stat.Ino),
					DeviceID:     int64(stat.Dev),
				}

				// Send to workers - block until space is available to ensure no files are dropped
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- fileInfo:
				}

				return nil
			})

			if err != nil {
				errChan <- fmt.Errorf("failed to walk %s: %w", p, err)
			}
		}(path)
	}

	// Wait for all walkers to complete, then close the error channel
	// Note: We don't close 'out' here because it's owned by the caller (WorkerPool)
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check for any errors from walkers
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// CountFiles counts the total number of files in the given paths
func CountFiles(ctx context.Context, paths []string) (int64, error) {
	var count int64

	for _, path := range paths {
		err := filepath.WalkDir(path, func(filePath string, d fs.DirEntry, err error) error {
			// Check for context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if err != nil {
				return nil // Continue counting
			}

			// Count only regular files (skip directories and symlinks)
			if !d.IsDir() && d.Type()&fs.ModeSymlink == 0 {
				count++
			}

			return nil
		})

		if err != nil {
			return 0, fmt.Errorf("failed to count files in %s: %w", path, err)
		}
	}

	return count, nil
}

// GetFileInfo gets detailed information about a specific file
func GetFileInfo(path string) (*FileInfo, error) {
	// Validate path to prevent directory traversal
	if err := validatePath(path); err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("unable to get system stats")
	}

	return &FileInfo{
		Path:         path,
		Size:         info.Size(),
		ModifiedTime: info.ModTime().Unix(),
		Inode:        int64(stat.Ino),
		DeviceID:     int64(stat.Dev),
	}, nil
}

// validatePath validates a file path to prevent directory traversal and other attacks
func validatePath(path string) error {
	if path == "" {
		return fmt.Errorf("path cannot be empty")
	}

	// Check for null bytes
	for i := 0; i < len(path); i++ {
		if path[i] == 0 {
			return fmt.Errorf("path contains null byte")
		}
	}

	// Clean the path to resolve any ./ or ../ components
	cleanPath := filepath.Clean(path)

	// Ensure the path is absolute for security
	if !filepath.IsAbs(cleanPath) {
		return fmt.Errorf("path must be absolute")
	}

	return nil
}
