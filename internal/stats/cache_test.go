package stats

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCache_ColdStartBlocks verifies the first Get call blocks on
// compute and stores the result.
func TestCache_ColdStartBlocks(t *testing.T) {
	c := NewCache(50 * time.Millisecond)
	var calls int32
	want := &Stats{TotalFiles: 7}

	got, err := c.Get(func() (*Stats, error) {
		atomic.AddInt32(&calls, 1)
		return want, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if calls != 1 {
		t.Fatalf("compute called %d times, want 1", calls)
	}

	// Second call within TTL: cache hit, no compute.
	got, err = c.Get(func() (*Stats, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("should not be called")
	})
	if err != nil {
		t.Fatalf("unexpected error on cache hit: %v", err)
	}
	if got != want {
		t.Fatalf("cache-hit got %v, want %v", got, want)
	}
	if calls != 1 {
		t.Fatalf("compute called %d times after cache hit, want 1", calls)
	}
}

// TestCache_StaleServesAndRefreshes confirms that once the value is
// stale (older than TTL) Get returns the stale value immediately and
// triggers exactly one background refresh.
func TestCache_StaleServesAndRefreshes(t *testing.T) {
	c := NewCache(20 * time.Millisecond)

	stale := &Stats{TotalFiles: 1}
	if _, err := c.Get(func() (*Stats, error) { return stale, nil }); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	// Wait past the TTL but before hard expiry.
	time.Sleep(30 * time.Millisecond)

	var refreshes int32
	fresh := &Stats{TotalFiles: 2}
	refreshDone := make(chan struct{})
	got, err := c.Get(func() (*Stats, error) {
		atomic.AddInt32(&refreshes, 1)
		// Slow recompute so we observe the stale return.
		time.Sleep(20 * time.Millisecond)
		close(refreshDone)
		return fresh, nil
	})
	if err != nil {
		t.Fatalf("stale Get returned error: %v", err)
	}
	if got != stale {
		t.Fatalf("expected immediate stale value, got %v", got)
	}

	<-refreshDone
	// Give the goroutine a tick to write the result into the cache.
	time.Sleep(5 * time.Millisecond)

	got, err = c.Get(func() (*Stats, error) {
		t.Fatalf("compute should not be called after background refresh")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("post-refresh Get error: %v", err)
	}
	if got != fresh {
		t.Fatalf("expected fresh value after refresh, got %v", got)
	}
	if refreshes != 1 {
		t.Fatalf("background refresh ran %d times, want 1", refreshes)
	}
}

// TestCache_ConcurrentStaleSingleFlights makes sure many simultaneous
// stale reads coalesce into one refresh, not N.
func TestCache_ConcurrentStaleSingleFlights(t *testing.T) {
	c := NewCache(10 * time.Millisecond)
	if _, err := c.Get(func() (*Stats, error) { return &Stats{TotalFiles: 1}, nil }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	var refreshes int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = c.Get(func() (*Stats, error) {
				atomic.AddInt32(&refreshes, 1)
				time.Sleep(30 * time.Millisecond)
				return &Stats{TotalFiles: 2}, nil
			})
		}()
	}
	close(start)
	wg.Wait()

	// Wait for any in-flight refresh to settle.
	time.Sleep(40 * time.Millisecond)

	if got := atomic.LoadInt32(&refreshes); got != 1 {
		t.Fatalf("expected single-flight (1 refresh), got %d", got)
	}
}

// TestCache_HardExpiryBlocks confirms that past TTL*staleMultiplier the
// cache forces a synchronous recompute instead of returning stale.
func TestCache_HardExpiryBlocks(t *testing.T) {
	ttl := 5 * time.Millisecond
	c := NewCache(ttl)
	if _, err := c.Get(func() (*Stats, error) { return &Stats{TotalFiles: 1}, nil }); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Wait past TTL * staleMultiplier (5x).
	time.Sleep(ttl*staleMultiplier + 5*time.Millisecond)

	fresh := &Stats{TotalFiles: 99}
	var computed bool
	got, err := c.Get(func() (*Stats, error) {
		computed = true
		return fresh, nil
	})
	if err != nil {
		t.Fatalf("hard-expiry Get error: %v", err)
	}
	if !computed {
		t.Fatalf("hard expiry should have forced compute")
	}
	if got != fresh {
		t.Fatalf("hard expiry returned %v, want fresh %v", got, fresh)
	}
}

// TestCache_ComputeErrorReturnsStale verifies that a failed background
// refresh doesn't lose the previously-cached value.
func TestCache_ComputeErrorReturnsStale(t *testing.T) {
	ttl := 5 * time.Millisecond
	c := NewCache(ttl)

	original := &Stats{TotalFiles: 42}
	if _, err := c.Get(func() (*Stats, error) { return original, nil }); err != nil {
		t.Fatalf("seed: %v", err)
	}

	time.Sleep(ttl + 2*time.Millisecond)

	// Stale read triggers background refresh that fails.
	got, err := c.Get(func() (*Stats, error) {
		return nil, errors.New("boom")
	})
	if err != nil {
		t.Fatalf("stale read should not surface bg error, got %v", err)
	}
	if got != original {
		t.Fatalf("stale read returned %v, want %v", got, original)
	}

	// Give bg refresh time to fail.
	time.Sleep(10 * time.Millisecond)

	// Subsequent read still gets the original (cache wasn't clobbered).
	got, _ = c.Get(func() (*Stats, error) {
		return &Stats{TotalFiles: 100}, nil
	})
	// Either the stale (still valid age) or a freshly-computed value is
	// fine, but the cache must not have been corrupted.
	if got == nil {
		t.Fatalf("cache lost its value after a failed bg refresh")
	}
}
