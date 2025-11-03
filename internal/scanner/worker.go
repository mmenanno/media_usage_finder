package scanner

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/database"
)

// Worker processes files from the queue
//
// Concurrency Strategy:
// - Workers run concurrently in goroutines, consuming from a shared input channel
// - Each worker independently processes files and updates the database
// - Progress tracking is thread-safe via mutex in Progress struct
// - Context cancellation allows graceful shutdown of all workers
type Worker struct {
	id          int
	db          *database.DB
	scanID      int64
	progress    *Progress
	incremental bool
}

// NewWorker creates a new worker
func NewWorker(id int, db *database.DB, scanID int64, progress *Progress, incremental bool) *Worker {
	return &Worker{
		id:          id,
		db:          db,
		scanID:      scanID,
		progress:    progress,
		incremental: incremental,
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

			if err := w.processFile(fileInfo); err != nil {
				w.progress.AddError(err.Error())
			}

			w.progress.IncrementFiles(fileInfo.Size)
		}
	}
}

// processFile processes a single file
func (w *Worker) processFile(fileInfo FileInfo) error {
	// Check if file exists in database
	existingFile, err := w.db.GetFileByPath(fileInfo.Path)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	// If incremental scan and file hasn't changed, skip processing
	if w.incremental && existingFile != nil && existingFile.ModifiedTime.Unix() == fileInfo.ModifiedTime {
		// Just update last_verified to mark as seen
		existingFile.LastVerified = time.Now()
		existingFile.ScanID = w.scanID
		if err := w.db.UpsertFile(existingFile); err != nil {
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

	// Upsert file record
	if err := w.db.UpsertFile(file); err != nil {
		return err
	}

	return nil
}

// WorkerPool manages a pool of workers
//
// Concurrency Model:
// - Fixed number of worker goroutines process files from a shared buffered channel
// - Workers are created at pool initialization and run until Stop() or Cancel()
// - Buffered channel (default 100) provides backpressure to filesystem walker
// - WaitGroup ensures all workers complete before pool methods return
// - Context allows cancellation to propagate to all workers simultaneously
//
// Synchronization:
// - input channel: synchronized by Go runtime (safe for concurrent send/receive)
// - wg: coordinates worker goroutine lifecycle
// - closeOnce: ensures input channel is closed exactly once
// - ctx/cancel: provides cancellation signal to all workers
//
// Shutdown:
// - Stop(): graceful - closes input, waits for workers to finish current files
// - Cancel(): immediate - cancels context, then closes input and waits
type WorkerPool struct {
	workers   []*Worker
	input     chan FileInfo
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	progress  *Progress
	closeOnce sync.Once
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(numWorkers, bufferSize int, db *database.DB, scanID int64, progress *Progress, incremental bool) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		workers:  make([]*Worker, numWorkers),
		input:    make(chan FileInfo, bufferSize),
		ctx:      ctx,
		cancel:   cancel,
		progress: progress,
	}

	// Create workers
	for i := 0; i < numWorkers; i++ {
		pool.workers[i] = NewWorker(i, db, scanID, progress, incremental)
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
// Cleanup order: close input channel first, wait for workers to finish, then cancel context
func (p *WorkerPool) Stop() {
	p.closeOnce.Do(func() {
		close(p.input)
	})
	p.wg.Wait()
	p.cancel() // Always cancel context to prevent leaks
}

// Cancel cancels all workers immediately
// Cleanup order: cancel context first (signals workers to stop), close input, then wait
func (p *WorkerPool) Cancel() {
	p.cancel()
	p.closeOnce.Do(func() {
		close(p.input)
	})
	p.wg.Wait()
}

// GetInputChannel returns the input channel for direct writing
func (p *WorkerPool) GetInputChannel() chan<- FileInfo {
	return p.input
}
