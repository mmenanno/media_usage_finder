package stats

import (
	"sync"
	"time"
)

// Cache provides a simple time-based cache for statistics
type Cache struct {
	mu         sync.RWMutex
	stats      *Stats
	cachedAt   time.Time
	ttl        time.Duration
}

// NewCache creates a new stats cache with the given TTL
func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		ttl: ttl,
	}
}

// Get retrieves cached stats if not expired, otherwise returns nil
func (c *Cache) Get() *Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.stats == nil {
		return nil
	}

	if time.Since(c.cachedAt) > c.ttl {
		return nil
	}

	return c.stats
}

// Set stores stats in the cache
func (c *Cache) Set(stats *Stats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats = stats
	c.cachedAt = time.Now()
}

// Invalidate clears the cache
func (c *Cache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats = nil
}

