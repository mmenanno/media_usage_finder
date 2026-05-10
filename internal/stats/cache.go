package stats

import (
	"log"
	"sync"
	"time"
)

// staleMultiplier is the upper bound on how stale a cached value can be
// served. Past TTL * staleMultiplier the cache forces a synchronous
// recompute even if a background refresh is in flight — this prevents
// unbounded staleness if the recompute keeps failing.
const staleMultiplier = 5

// Cache provides stale-while-revalidate caching for stats.
//
// Get(compute) semantics:
//
//   - Cache empty (cold start): block on compute(), store, return.
//   - Cache fresh (age < ttl):  return cached immediately.
//   - Cache stale but exists (ttl <= age < ttl*staleMultiplier):
//     return the stale value immediately AND trigger a background
//     recompute (single-flighted — concurrent stale reads share one
//     refresh).
//   - Cache hard-expired (age >= ttl*staleMultiplier): treat as cold
//     start (block on compute).
//
// Concurrent compute calls are coalesced: only one refresh runs at a
// time. The rest see the in-flight refresh and return stale (or, in the
// hard-expired case, wait for the running refresh to finish).
type Cache struct {
	mu       sync.Mutex
	stats    *Stats
	cachedAt time.Time
	ttl      time.Duration

	// Single-flight state for background refresh.
	refreshing bool
	// Channel closed when a hard-expired blocking refresh finishes; lets
	// other hard-expired callers wait for the in-flight refresh instead
	// of all kicking off their own.
	refreshDone chan struct{}
}

// NewCache returns a cache with the given freshness TTL. Stale values
// are served for up to ttl*staleMultiplier before a recompute is forced.
func NewCache(ttl time.Duration) *Cache {
	return &Cache{ttl: ttl}
}

// Get returns the cached stats, recomputing as needed via `compute`.
// `compute` is invoked at most once per refresh window thanks to the
// internal single-flight gate.
func (c *Cache) Get(compute func() (*Stats, error)) (*Stats, error) {
	c.mu.Lock()

	now := time.Now()
	cached := c.stats
	age := now.Sub(c.cachedAt)

	switch {
	case cached == nil:
		// Cold start: block on compute under the lock-released path.
		return c.blockingRefresh(compute)

	case age < c.ttl:
		// Fresh — fast path.
		c.mu.Unlock()
		return cached, nil

	case age < c.ttl*staleMultiplier:
		// Stale but serviceable. Return now, refresh in the background
		// if no other refresh is already in flight.
		if !c.refreshing {
			c.refreshing = true
			c.refreshDone = make(chan struct{})
			go c.backgroundRefresh(compute)
		}
		c.mu.Unlock()
		return cached, nil

	default:
		// Hard-expired. Coalesce with any in-flight refresh — wait for
		// it instead of kicking off another.
		if c.refreshing {
			done := c.refreshDone
			c.mu.Unlock()
			<-done
			c.mu.Lock()
			result := c.stats
			c.mu.Unlock()
			return result, nil
		}
		return c.blockingRefresh(compute)
	}
}

// blockingRefresh assumes c.mu is held. It releases the lock during
// compute(), then re-acquires it to store the result. Returns the new
// value (or the stale value + error if compute fails).
func (c *Cache) blockingRefresh(compute func() (*Stats, error)) (*Stats, error) {
	c.refreshing = true
	c.refreshDone = make(chan struct{})
	stale := c.stats
	c.mu.Unlock()

	fresh, err := compute()

	c.mu.Lock()
	c.refreshing = false
	if c.refreshDone != nil {
		close(c.refreshDone)
		c.refreshDone = nil
	}
	if err != nil {
		c.mu.Unlock()
		// Surface the error but keep returning whatever stale value
		// we had so callers can degrade gracefully.
		return stale, err
	}
	c.stats = fresh
	c.cachedAt = time.Now()
	c.mu.Unlock()
	return fresh, nil
}

// backgroundRefresh runs in its own goroutine. Errors are logged; the
// stale value remains until the next successful refresh.
func (c *Cache) backgroundRefresh(compute func() (*Stats, error)) {
	fresh, err := compute()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.refreshing = false
	if c.refreshDone != nil {
		close(c.refreshDone)
		c.refreshDone = nil
	}
	if err != nil {
		log.Printf("stats cache: background refresh failed: %v", err)
		return
	}
	c.stats = fresh
	c.cachedAt = time.Now()
}

// Set is retained for callers that compute stats out-of-band (e.g.,
// the scan completion hook). Refreshes the cached value and timestamp.
func (c *Cache) Set(stats *Stats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats = stats
	c.cachedAt = time.Now()
}

// Invalidate clears the cached value. The next Get blocks on compute.
func (c *Cache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats = nil
	c.cachedAt = time.Time{}
}

// Age returns how long ago the cache was last refreshed, or 0 if empty.
// Useful for surfacing freshness in the UI.
func (c *Cache) Age() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stats == nil {
		return 0
	}
	return time.Since(c.cachedAt)
}
