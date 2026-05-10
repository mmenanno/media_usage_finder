package scanner

import (
	"testing"
	"time"
)

// TestGetSnapshot_ETASuppressedDuringWarmup verifies ETA stays at zero
// until the warmup window passes (elapsed or % complete).
func TestGetSnapshot_ETASuppressedDuringWarmup(t *testing.T) {
	p := &Progress{
		StartTime:  time.Now().Add(-1 * time.Second), // tiny elapsed
		IsRunning:  true,
		TotalFiles: 1000,
	}
	// Manually set ProcessedFiles small enough that pct is well under 1%.
	p.ProcessedFiles = 1

	snap := p.GetSnapshot()
	if snap.ETA != 0 {
		t.Fatalf("ETA should be suppressed during warmup; got %s", snap.ETA)
	}
}

// TestGetSnapshot_ETAUsesEMARate confirms the snapshot's ETA derives
// from the EMA rate once seeded, not from the cumulative rate.
func TestGetSnapshot_ETAUsesEMARate(t *testing.T) {
	p := &Progress{
		StartTime:  time.Now().Add(-2 * time.Minute),
		IsRunning:  true,
		TotalFiles: 1000,
	}
	p.ProcessedFiles = 500
	// Cumulative rate would be ~4.16 files/sec → ETA ~120s.
	// Seed EMA at 100 files/sec → ETA should be 5s, not 120s.
	p.emaRate = 100

	snap := p.GetSnapshot()
	if snap.ETA == 0 {
		t.Fatalf("ETA should be populated past warmup")
	}
	if snap.ETA > 10*time.Second || snap.ETA < 4*time.Second {
		t.Fatalf("expected ETA ~5s from EMA rate; got %s", snap.ETA)
	}
}

// TestIncrementFiles_SeedsEMA verifies the very first sample seeds the
// EMA directly (no bias toward zero).
func TestIncrementFiles_SeedsEMA(t *testing.T) {
	p := &Progress{
		StartTime: time.Now().Add(-1 * time.Second),
		IsRunning: true,
	}
	// Pretend the previous sample was a second ago at 0 files.
	p.lastSampleTime = time.Now().Add(-1 * time.Second)
	p.lastSampleFiles = 0

	// Process a batch then call IncrementFiles to trigger the sample.
	p.ProcessedFiles = 99 // simulate 99 already done before the call
	p.IncrementFiles(0)   // 100th file at ~1s -> 100 files/sec

	if p.emaRate < 50 || p.emaRate > 200 {
		t.Fatalf("seeded EMA rate looks wrong: %f", p.emaRate)
	}
}

// TestIncrementFiles_SubInterval samples are skipped under the minimum
// interval so per-file noise doesn't update the EMA.
func TestIncrementFiles_SubInterval(t *testing.T) {
	p := &Progress{
		StartTime: time.Now(),
		IsRunning: true,
		emaRate:   42, // pre-existing rate to detect changes
	}
	p.lastSampleTime = time.Now() // sampled "just now"
	p.lastSampleFiles = 0

	for i := 0; i < 10; i++ {
		p.IncrementFiles(0)
	}

	if p.emaRate != 42 {
		t.Fatalf("EMA changed despite sub-interval samples: %f", p.emaRate)
	}
}
