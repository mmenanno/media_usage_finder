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
}

// NewProgress creates a new progress tracker
func NewProgress() *Progress {
	p := &Progress{
		StartTime:    time.Now(),
		IsRunning:    true,
		Errors:       make([]string, 0),
		logChan:      make(chan string, 100),
		logListeners: make([]chan string, 0),
	}

	// Start log broadcaster
	go p.broadcastLogs()

	return p
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
	defer p.mu.Unlock()

	p.IsRunning = false

	// Close log channel
	if p.logChan != nil {
		close(p.logChan)
	}
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
