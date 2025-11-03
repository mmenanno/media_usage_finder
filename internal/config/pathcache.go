package config

import (
	"sync"

	"github.com/mmenanno/media-usage-finder/internal/constants"
)

// PathCache provides thread-safe caching for path translations
type PathCache struct {
	cache     map[string]string
	mu        sync.RWMutex
	hits      uint64
	total     uint64
	evictions uint64
}

// NewPathCache creates a new path cache
func NewPathCache() *PathCache {
	return &PathCache{
		cache: make(map[string]string, constants.PathCacheSize),
	}
}

// Get retrieves a cached translation
func (pc *PathCache) Get(key string) (string, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	pc.total++
	val, ok := pc.cache[key]
	if ok {
		pc.hits++
	}
	return val, ok
}

// Set stores a translation in the cache
func (pc *PathCache) Set(key, value string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// If cache is too large, evict ~25% of entries (LRU-inspired partial eviction)
	if len(pc.cache) >= constants.PathCacheSize {
		pc.evictions++
		// Remove ~25% of entries to make room while preserving most of the cache
		removeCount := constants.PathCacheSize / 4
		removed := 0
		for k := range pc.cache {
			delete(pc.cache, k)
			removed++
			if removed >= removeCount {
				break
			}
		}
	}

	pc.cache[key] = value
}

// Clear removes all entries from the cache
func (pc *PathCache) Clear() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.cache = make(map[string]string, constants.PathCacheSize)
	pc.hits = 0
	pc.total = 0
	pc.evictions = 0
}

// Stats returns cache statistics
func (pc *PathCache) Stats() (hits, total, evictions uint64, size int, hitRate float64) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	hits = pc.hits
	total = pc.total
	evictions = pc.evictions
	size = len(pc.cache)
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}
	return
}
