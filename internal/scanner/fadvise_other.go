//go:build !linux

package scanner

import "os"

// applySequentialHint is a no-op on non-Linux systems
func applySequentialHint(f *os.File) {
	// No-op: fadvise is Linux-specific
}

// releaseCacheForLargeFile is a no-op on non-Linux systems
func releaseCacheForLargeFile(f *os.File, size int64) {
	// No-op: fadvise is Linux-specific
}
