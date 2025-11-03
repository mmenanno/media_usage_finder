package scanner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/database"
)

// Worker processes files from the queue
//
// Concurrency Strategy:
// - Workers run concurrently in goroutines, consuming from a shared input channel
// - Each worker uses a shared batch accumulator for efficient database inserts
// - For incremental scans, workers use a shared in-memory file map for fast lookups
// - Progress tracking is thread-safe via mutex in Progress struct
// - Context cancellation allows graceful shutdown of all workers
type Worker struct {
	id               int
	db               *database.DB
	batchAccumulator *BatchAccumulator
	fileMap          map[string]*database.File // Optional: in-memory file index for incremental scans
	scanID           int64
	progress         *Progress
	incremental      bool
}

// NewWorker creates a new worker
func NewWorker(id int, db *database.DB, batchAccumulator *BatchAccumulator, fileMap map[string]*database.File, scanID int64, progress *Progress, incremental bool) *Worker {
	return &Worker{
		id:               id,
		db:               db,
		batchAccumulator: batchAccumulator,
		fileMap:          fileMap,
		scanID:           scanID,
		progress:         progress,
		incremental:      incremental,
	}
}

// Run starts the worker processing files from the input channel
func (w *Worker) Run(ctx context.Context, in <-chan FileInfo, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case fileInfo, ok := <-in:
			if !ok {
				return
			}

			// Check for cancellation before processing
			// This ensures workers respond quickly to cancellation
			// even when the input channel has buffered items
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := w.processFile(fileInfo); err != nil {
				w.progress.AddError(err.Error())
			}

			w.progress.IncrementFiles(fileInfo.Size)
		}
	}
}

// processFile processes a single file
func (w *Worker) processFile(fileInfo FileInfo) error {
	var existingFile *database.File

	// Use in-memory file map if available (for incremental scans)
	if w.fileMap != nil {
		existingFile = w.fileMap[fileInfo.Path]
	} else {
		// Fall back to database lookup (for full scans)
		var err error
		existingFile, err = w.db.GetFileByPath(fileInfo.Path)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}

	// If incremental scan and file hasn't changed, skip processing
	if w.incremental && existingFile != nil && existingFile.ModifiedTime.Unix() == fileInfo.ModifiedTime {
		// Just update last_verified to mark as seen
		existingFile.LastVerified = time.Now()
		existingFile.ScanID = w.scanID
		// Use batch accumulator for efficiency
		if err := w.batchAccumulator.Add(existingFile); err != nil {
			return err
		}
		return nil
	}

	file := &database.File{
		Path:         fileInfo.Path,
		Size:         fileInfo.Size,
		Inode:        fileInfo.Inode,
		DeviceID:     fileInfo.DeviceID,
		ModifiedTime: time.Unix(fileInfo.ModifiedTime, 0),
		ScanID:       w.scanID,
		LastVerified: time.Now(),
		IsOrphaned:   true, // Will be updated later when we check services
		Extension:    database.ExtractExtension(fileInfo.Path),
	}

	// Preserve existing ID if file already exists
	if existingFile != nil {
		file.ID = existingFile.ID
	}

	// Add to batch accumulator for efficient insert
	if err := w.batchAccumulator.Add(file); err != nil {
		return err
	}

	return nil
}

// WorkerPool manages a pool of workers
//
// Concurrency Model:
// - Fixed number of worker goroutines process files from a shared buffered channel
// - Workers share a batch accumulator for efficient database inserts
// - For incremental scans, workers share an in-memory file map for fast lookups
// - Buffered channel (default 100) provides backpressure to filesystem walker
// - WaitGroup ensures all workers complete before pool methods return
// - Context allows cancellation to propagate to all workers simultaneously
//
// Synchronization:
// - input channel: synchronized by Go runtime (safe for concurrent send/receive)
// - wg: coordinates worker goroutine lifecycle
// - closeOnce: ensures input channel is closed exactly once
// - ctx/cancel: provides cancellation signal to all workers
// - batchAccumulator: thread-safe for concurrent worker access
// - fileMap: read-only after creation, safe for concurrent reads
//
// Shutdown:
// - Stop(): graceful - closes input, waits for workers, flushes remaining batches
// - Cancel(): immediate - cancels context, closes input, waits, flushes remaining batches
type WorkerPool struct {
	workers          []*Worker
	input            chan FileInfo
	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
	progress         *Progress
	closeOnce        sync.Once
	batchAccumulator *BatchAccumulator
}

// NewWorkerPool creates a new worker pool
// fileMap is optional - pass nil for full scans, or a pre-loaded map for incremental scans
func NewWorkerPool(numWorkers, bufferSize int, db *database.DB, fileMap map[string]*database.File, scanID int64, progress *Progress, incremental bool) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	// Create batch accumulator with a callback to track progress
	// Batch size of 100 provides good balance between memory and performance
	const batchSize = 100
	batchAccumulator := NewBatchAccumulator(db, ctx, batchSize, nil)

	pool := &WorkerPool{
		workers:          make([]*Worker, numWorkers),
		input:            make(chan FileInfo, bufferSize),
		ctx:              ctx,
		cancel:           cancel,
		progress:         progress,
		batchAccumulator: batchAccumulator,
	}

	// Create workers with shared batch accumulator and optional file map
	for i := 0; i < numWorkers; i++ {
		pool.workers[i] = NewWorker(i, db, batchAccumulator, fileMap, scanID, progress, incremental)
	}

	return pool
}

// Start starts all workers
func (p *WorkerPool) Start() {
	for _, worker := range p.workers {
		p.wg.Add(1)
		go worker.Run(p.ctx, p.input, &p.wg)
	}
}

// Submit submits a file for processing
func (p *WorkerPool) Submit(fileInfo FileInfo) {
	p.input <- fileInfo
}

// Stop stops all workers gracefully
// Cleanup order: close input channel, wait for workers to finish, flush batches, cancel context
func (p *WorkerPool) Stop() {
	p.closeOnce.Do(func() {
		close(p.input)
	})
	p.wg.Wait()

	// Flush any remaining files in the batch accumulator
	if err := p.batchAccumulator.Flush(); err != nil {
		p.progress.AddError(fmt.Sprintf("Failed to flush final batch: %v", err))
	}

	p.cancel() // Always cancel context to prevent leaks
}

// Cancel cancels all workers immediately
// Cleanup order: cancel context (signals workers to stop), close input, wait, flush batches
func (p *WorkerPool) Cancel() {
	p.cancel()
	p.closeOnce.Do(func() {
		close(p.input)
	})
	p.wg.Wait()

	// Flush any remaining files in the batch accumulator
	// Even on cancel, we flush what was processed to avoid data loss
	if err := p.batchAccumulator.Flush(); err != nil {
		p.progress.AddError(fmt.Sprintf("Failed to flush final batch: %v", err))
	}
}

// GetInputChannel returns the input channel for direct writing
func (p *WorkerPool) GetInputChannel() chan<- FileInfo {
	return p.input
}
