package scanner

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/constants"
	"github.com/mmenanno/media-usage-finder/internal/stats"
)

// Progress tracks the progress of a scan
type Progress struct {
	mu sync.RWMutex

	TotalFiles     int64
	ProcessedFiles int64
	TotalSize      int64
	ProcessedSize  int64
	Errors         []string
	StartTime      time.Time
	IsRunning      bool
	CurrentPhase   string

	// Progress estimation
	IsEstimated bool // True if TotalFiles is estimated from previous scan

	// Service update progress
	CurrentService int // Which service is being updated (1-based)
	TotalServices  int // Total number of configured services

	// Log streaming
	logChan      chan string
	logListeners []chan string
	stopOnce     sync.Once
}

// NewProgress creates a new progress tracker
func NewProgress() *Progress {
	p := &Progress{
		StartTime:    time.Now(),
		IsRunning:    true,
		Errors:       make([]string, 0, constants.ErrorSliceCapacity),
		logChan:      make(chan string, constants.LogChannelBuffer),
		logListeners: make([]chan string, 0),
	}

	// Start log broadcaster
	go p.broadcastLogs()

	// Start periodic cleanup of stale listeners
	go p.periodicCleanup()

	return p
}

// periodicCleanup periodically removes stale listeners
func (p *Progress) periodicCleanup() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		p.mu.RLock()
		running := p.IsRunning
		p.mu.RUnlock()

		if !running {
			return
		}

		p.CleanupStaleListeners()
	}
}

// broadcastLogs broadcasts log messages to all listeners
func (p *Progress) broadcastLogs() {
	for msg := range p.logChan {
		p.mu.RLock()
		listeners := make([]chan string, len(p.logListeners))
		copy(listeners, p.logListeners)
		p.mu.RUnlock()

		for _, listener := range listeners {
			select {
			case listener <- msg:
			default:
				// Skip if listener is blocked
			}
		}
	}
}

// Log sends a log message
func (p *Progress) Log(message string) {
	p.mu.RLock()
	running := p.IsRunning
	p.mu.RUnlock()

	if !running {
		return // Don't write to closed channel
	}

	// Additional safety: use select with default to prevent blocking
	// even if channel is closed between check and send
	defer func() {
		if r := recover(); r != nil {
			// Channel was closed during send, ignore panic
		}
	}()

	select {
	case p.logChan <- message:
	default:
		// Drop message if channel is full
	}
}

// Subscribe returns a channel that receives log messages
func (p *Progress) Subscribe() chan string {
	p.mu.Lock()
	defer p.mu.Unlock()

	listener := make(chan string, constants.LogListenerBuffer)
	p.logListeners = append(p.logListeners, listener)
	return listener
}

// Unsubscribe removes a log listener
func (p *Progress) Unsubscribe(listener chan string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, l := range p.logListeners {
		if l == listener {
			p.logListeners = append(p.logListeners[:i], p.logListeners[i+1:]...)
			close(listener)
			break
		}
	}
}

// CleanupStaleListeners removes listeners that are blocking (likely abandoned)
func (p *Progress) CleanupStaleListeners() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove listeners whose channels are full (likely abandoned/stale)
	// We use len() to check if the buffer is full, which is safer than writing test messages
	activeListeners := make([]chan string, 0, len(p.logListeners))
	for _, listener := range p.logListeners {
		// If the channel is at capacity (full buffer), it's likely stale
		// Keep listeners that still have buffer space available
		if len(listener) < cap(listener) {
			activeListeners = append(activeListeners, listener)
		} else {
			// Channel is full - close and discard
			close(listener)
		}
	}
	p.logListeners = activeListeners
}

// IncrementFiles increments the file counters
func (p *Progress) IncrementFiles(size int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ProcessedFiles++
	p.ProcessedSize += size
}

// SetTotalFiles sets the total number of files
func (p *Progress) SetTotalFiles(total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.TotalFiles = total
}

// AddError adds an error message (keeps only last 1000 errors to prevent memory issues)
func (p *Progress) AddError(err string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Errors = append(p.Errors, err)

	// Also log to SSE stream
	p.logChan <- fmt.Sprintf("ERROR: %s", err)

	// Keep only last N errors to prevent unbounded growth
	if len(p.Errors) > constants.MaxStoredErrors {
		p.Errors = p.Errors[len(p.Errors)-constants.MaxStoredErrors:]
	}
}

// SetPhase sets the current phase
func (p *Progress) SetPhase(phase string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.CurrentPhase = phase
}

// SetEstimatedTotal sets the total files as an estimate from a previous scan
func (p *Progress) SetEstimatedTotal(total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.TotalFiles = total
	p.IsEstimated = true
}

// SetServiceProgress sets the current service progress (1-based indexing)
func (p *Progress) SetServiceProgress(current, total int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.CurrentService = current
	p.TotalServices = total
}

// Stop marks the scan as completed
func (p *Progress) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.IsRunning {
		return // Already stopped
	}

	p.IsRunning = false

	// Close log channel once (protected by mutex)
	p.stopOnce.Do(func() {
		if p.logChan != nil {
			close(p.logChan)
		}
	})
}

// GetSnapshot returns a snapshot of the current progress
func (p *Progress) GetSnapshot() ProgressSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()

	elapsed := time.Since(p.StartTime)
	var eta time.Duration
	var percentComplete float64

	if p.TotalFiles > 0 {
		percentComplete = float64(p.ProcessedFiles) / float64(p.TotalFiles) * 100
		if p.ProcessedFiles > 0 {
			rate := float64(p.ProcessedFiles) / elapsed.Seconds()
			remaining := p.TotalFiles - p.ProcessedFiles
			eta = time.Duration(float64(remaining)/rate) * time.Second
		}
	}

	return ProgressSnapshot{
		TotalFiles:      p.TotalFiles,
		ProcessedFiles:  p.ProcessedFiles,
		TotalSize:       p.TotalSize,
		ProcessedSize:   p.ProcessedSize,
		ErrorCount:      len(p.Errors),
		Elapsed:         elapsed,
		ETA:             eta,
		PercentComplete: percentComplete,
		IsRunning:       p.IsRunning,
		CurrentPhase:    p.CurrentPhase,
		StartTime:       p.StartTime,
		IsEstimated:     p.IsEstimated,
		CurrentService:  p.CurrentService,
		TotalServices:   p.TotalServices,
	}
}

// ProgressSnapshot represents a point-in-time snapshot of progress
type ProgressSnapshot struct {
	TotalFiles      int64
	ProcessedFiles  int64
	TotalSize       int64
	ProcessedSize   int64
	ErrorCount      int
	Elapsed         time.Duration
	ETA             time.Duration
	PercentComplete float64
	IsRunning       bool
	CurrentPhase    string
	StartTime       time.Time
	IsEstimated     bool // True if TotalFiles is estimated from previous scan
	CurrentService  int  // Which service is being updated (1-based)
	TotalServices   int  // Total number of configured services
}

// MarshalJSON customizes JSON serialization to format durations cleanly
func (ps ProgressSnapshot) MarshalJSON() ([]byte, error) {
	type Alias ProgressSnapshot
	return json.Marshal(&struct {
		Elapsed string `json:"Elapsed"`
		ETA     string `json:"ETA"`
		*Alias
	}{
		Elapsed: stats.FormatDuration(ps.Elapsed),
		ETA:     stats.FormatDuration(ps.ETA),
		Alias:   (*Alias)(&ps),
	})
}
