package scanner

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
func WalkFiles(ctx context.Context, paths []string, out chan<- FileInfo, progress *Progress) error {
	defer close(out)

	for _, path := range paths {
		err := filepath.WalkDir(path, func(filePath string, d fs.DirEntry, err error) error {
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

			// Send to workers
			select {
			case out <- fileInfo:
			default:
				// Channel full, this shouldn't happen with proper buffering
				progress.AddError(fmt.Sprintf("Channel blocked for %s", filePath))
			}

			return nil
		})

		if err != nil {
			return fmt.Errorf("failed to walk %s: %w", path, err)
		}
	}

	return nil
}

// CountFiles counts the total number of files in the given paths
func CountFiles(paths []string) (int64, error) {
	var count int64

	for _, path := range paths {
		err := filepath.WalkDir(path, func(filePath string, d fs.DirEntry, err error) error {
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
