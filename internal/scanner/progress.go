package scanner

import (
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
}

// NewProgress creates a new progress tracker
func NewProgress() *Progress {
	return &Progress{
		StartTime: time.Now(),
		IsRunning: true,
		Errors:    make([]string, 0),
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

// AddError adds an error message
func (p *Progress) AddError(err string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Errors = append(p.Errors, err)
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
