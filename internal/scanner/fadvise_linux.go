//go:build linux

package scanner

import (
	"os"

	"golang.org/x/sys/unix"
)

// applySequentialHint tells the kernel we'll read the file sequentially
// This doubles the read-ahead window for better performance
func applySequentialHint(f *os.File) {
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL)
}

// releaseCacheForLargeFile frees page cache for large files to prevent pollution
func releaseCacheForLargeFile(f *os.File, size int64) {
	if size > 1073741824 { // 1GB
		_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
	}
}
