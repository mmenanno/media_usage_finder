package scanner

import (
	"fmt"
	"sync"
	"time"
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
		Errors:       make([]string, 0, 100), // Pre-allocate capacity
		logChan:      make(chan string, 100),
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

	listener := make(chan string, 50)
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

// CleanupStaleListeners removes listeners that haven't been read from in a while
func (p *Progress) CleanupStaleListeners() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove listeners that are blocking (full channel buffer)
	activeListeners := make([]chan string, 0, len(p.logListeners))
	for _, listener := range p.logListeners {
		select {
		case listener <- "": // Try to send empty message
			activeListeners = append(activeListeners, listener)
		default:
			// Channel is full, likely stale/abandoned - close it
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

	// Keep only last 1000 errors to prevent unbounded growth
	const maxErrors = 1000
	if len(p.Errors) > maxErrors {
		p.Errors = p.Errors[len(p.Errors)-maxErrors:]
	}
}

// SetPhase sets the current phase
func (p *Progress) SetPhase(phase string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.CurrentPhase = phase
}

// Stop marks the scan as completed
func (p *Progress) Stop() {
	p.mu.Lock()
	p.IsRunning = false
	p.mu.Unlock()

	// Close log channel once
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
}
