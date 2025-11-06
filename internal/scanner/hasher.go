package scanner

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/zeebo/blake3"
)

// FileHasher handles file hashing operations with support for multiple algorithms
type FileHasher struct {
	algorithm     string // "sha256" or "blake3"
	quickHashSize int64  // Size of quick hash chunks (default: 1MB)
	bufferSize    int    // Buffer size for full hash reads (configurable)
}

// NewFileHasher creates a new FileHasher with the specified algorithm and buffer size
func NewFileHasher(algorithm string, bufferSize int) *FileHasher {
	// Default to 4MB if buffer size not specified or invalid
	if bufferSize <= 0 {
		bufferSize = 4 * 1024 * 1024 // 4MB default
	}
	return &FileHasher{
		algorithm:     algorithm,
		quickHashSize: 1024 * 1024, // 1MB
		bufferSize:    bufferSize,
	}
}

// QuickHash calculates a fast hash using first + last chunks of the file
// This is useful for quick duplicate screening before doing full hash
// Format: hash(size || first_1mb || last_1mb)
func (h *FileHasher) QuickHash(path string, size int64) (string, error) {
	if size == 0 {
		return "", fmt.Errorf("cannot hash empty file")
	}

	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Create hasher
	hasher := h.createHasher()

	// Write file size as part of hash (prevents size-different files from matching)
	sizeBytes := []byte(fmt.Sprintf("%d", size))
	if _, err := hasher.Write(sizeBytes); err != nil {
		return "", fmt.Errorf("failed to write size to hasher: %w", err)
	}

	// Read first chunk
	chunkSize := h.quickHashSize
	if size < chunkSize {
		chunkSize = size
	}

	firstChunk := make([]byte, chunkSize)
	n, err := f.Read(firstChunk)
	if err != nil && err != io.EOF {
		return "", fmt.Errorf("failed to read first chunk: %w", err)
	}
	if _, err := hasher.Write(firstChunk[:n]); err != nil {
		return "", fmt.Errorf("failed to write first chunk to hasher: %w", err)
	}

	// If file is larger than one chunk, read last chunk
	if size > h.quickHashSize {
		lastChunk := make([]byte, h.quickHashSize)
		_, err := f.Seek(-h.quickHashSize, io.SeekEnd)
		if err != nil {
			return "", fmt.Errorf("failed to seek to last chunk: %w", err)
		}

		n, err := f.Read(lastChunk)
		if err != nil && err != io.EOF {
			return "", fmt.Errorf("failed to read last chunk: %w", err)
		}
		if _, err := hasher.Write(lastChunk[:n]); err != nil {
			return "", fmt.Errorf("failed to write last chunk to hasher: %w", err)
		}
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// FullHash calculates the hash of the entire file
func (h *FileHasher) FullHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Hint to kernel that we'll read sequentially (doubles read-ahead)
	// Gracefully degrades on non-Linux systems
	applySequentialHint(f)

	hasher := h.createHasher()

	// Use configurable buffer size for better performance
	buf := make([]byte, h.bufferSize)
	if _, err := io.CopyBuffer(hasher, f, buf); err != nil {
		return "", fmt.Errorf("failed to hash file: %w", err)
	}

	// Get file size to determine if we should free cache
	stat, err := f.Stat()
	if err == nil {
		// Free page cache for large files to prevent cache pollution
		releaseCacheForLargeFile(f, stat.Size())
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// HashWithProgress calculates full hash with progress callback
// The progressFunc is called periodically with bytes read so far
func (h *FileHasher) HashWithProgress(path string, progressFunc func(bytesRead int64)) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Hint to kernel that we'll read sequentially (doubles read-ahead)
	applySequentialHint(f)

	hasher := h.createHasher()

	// Use configurable buffer size for reading
	buf := make([]byte, h.bufferSize)
	var totalRead int64

	for {
		n, err := f.Read(buf)
		if n > 0 {
			totalRead += int64(n)
			if _, writeErr := hasher.Write(buf[:n]); writeErr != nil {
				return "", fmt.Errorf("failed to write to hasher: %w", writeErr)
			}

			// Call progress callback
			if progressFunc != nil {
				progressFunc(totalRead)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("failed to read file: %w", err)
		}
	}

	// Free page cache for large files to prevent cache pollution
	releaseCacheForLargeFile(f, totalRead)

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// createHasher creates a hash.Hash instance based on the configured algorithm
func (h *FileHasher) createHasher() hash.Hash {
	switch h.algorithm {
	case "blake3":
		return blake3.New()
	case "sha256":
		fallthrough
	default:
		return sha256.New()
	}
}

// GetAlgorithm returns the configured hash algorithm
func (h *FileHasher) GetAlgorithm() string {
	return h.algorithm
}

// VerifyHash re-calculates the hash and compares with expected
func (h *FileHasher) VerifyHash(path, expectedHash string) (bool, error) {
	actualHash, err := h.FullHash(path)
	if err != nil {
		return false, err
	}
	return actualHash == expectedHash, nil
}

// PartialHash calculates a hash by reading the first N bytes of a file
// Used for progressive hash verification (levels 1-5)
// Format: hash(fileSize || first_maxBytes)
func (h *FileHasher) PartialHash(path string, fileSize, maxBytes int64) (string, error) {
	if fileSize == 0 {
		return "", fmt.Errorf("cannot hash empty file")
	}

	// If maxBytes >= fileSize, use full hash instead
	if maxBytes >= fileSize {
		return h.FullHash(path)
	}

	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Apply sequential read hint for better performance
	applySequentialHint(f)

	hasher := h.createHasher()

	// Include file size in hash (prevents different-size files from matching)
	sizeBytes := []byte(fmt.Sprintf("%d", fileSize))
	if _, err := hasher.Write(sizeBytes); err != nil {
		return "", fmt.Errorf("failed to write size to hasher: %w", err)
	}

	// Read and hash first maxBytes contiguously
	bytesToRead := maxBytes
	buf := make([]byte, h.bufferSize) // Use configured buffer size
	var totalRead int64

	for totalRead < bytesToRead {
		// Calculate how much to read in this iteration
		remaining := bytesToRead - totalRead
		readSize := int64(len(buf))
		if remaining < readSize {
			readSize = remaining
		}

		n, err := f.Read(buf[:readSize])
		if n > 0 {
			totalRead += int64(n)
			if _, writeErr := hasher.Write(buf[:n]); writeErr != nil {
				return "", fmt.Errorf("failed to write to hasher: %w", writeErr)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("failed to read file: %w", err)
		}
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// GetChunkSizeForLevel returns the number of bytes to hash for a given level
// Levels: 0=none, 1=1MB, 2=10MB, 3=100MB, 4=1GB, 5=10GB, 6=full
func GetChunkSizeForLevel(level int) int64 {
	const (
		MB = 1024 * 1024
		GB = MB * 1024
	)

	switch level {
	case 0:
		return 0
	case 1:
		return 1 * MB
	case 2:
		return 10 * MB
	case 3:
		return 100 * MB
	case 4:
		return 1 * GB
	case 5:
		return 10 * GB
	case 6:
		return 0 // Full hash (handled specially)
	default:
		return 1 * MB // Default to quick hash
	}
}

// GetEffectiveLevel returns the appropriate hash level for a file
// Don't try to hash 10GB of a 5GB file - cap at appropriate level
func GetEffectiveLevel(fileSize int64, requestedLevel int) int {
	// For level 6 (full hash), always use full hash
	if requestedLevel == 6 {
		return 6
	}

	chunkSize := GetChunkSizeForLevel(requestedLevel)

	// If file is smaller than the chunk size, find the right level
	if fileSize <= chunkSize {
		// Find the highest level that fits the file size
		for level := requestedLevel - 1; level >= 1; level-- {
			if fileSize > GetChunkSizeForLevel(level) {
				return level
			}
		}
		// If file is very small, use level 1
		return 1
	}

	return requestedLevel
}

// GetLevelName returns a human-readable name for a hash level
// Used for UI display (e.g., "Progressive (100MB)")
func GetLevelName(level int) string {
	switch level {
	case 0:
		return "No Hash"
	case 1:
		return "Progressive (1MB)"
	case 2:
		return "Progressive (10MB)"
	case 3:
		return "Progressive (100MB)"
	case 4:
		return "Progressive (1GB)"
	case 5:
		return "Progressive (10GB)"
	case 6:
		return "Full"
	default:
		return fmt.Sprintf("Progressive (Level %d)", level)
	}
}
