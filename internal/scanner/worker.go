package scanner

import (
	"context"
	"sync"
	"time"

	"github.com/mmenanno/media-usage-finder/internal/database"
)

// Worker processes files from the queue
type Worker struct {
	id       int
	db       *database.DB
	scanID   int64
	progress *Progress
}

// NewWorker creates a new worker
func NewWorker(id int, db *database.DB, scanID int64, progress *Progress) *Worker {
	return &Worker{
		id:       id,
		db:       db,
		scanID:   scanID,
		progress: progress,
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
	if err != nil && err.Error() != "sql: no rows in result set" {
		return err
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
	}

	// If file exists and hasn't been modified, skip detailed processing
	if existingFile != nil && existingFile.ModifiedTime.Unix() == fileInfo.ModifiedTime {
		// Just update last_verified and scan_id
		file.ID = existingFile.ID
		file.IsOrphaned = existingFile.IsOrphaned
	}

	// Upsert file record
	if err := w.db.UpsertFile(file); err != nil {
		return err
	}

	return nil
}

// WorkerPool manages a pool of workers
type WorkerPool struct {
	workers  []*Worker
	input    chan FileInfo
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	progress *Progress
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(numWorkers int, db *database.DB, scanID int64, progress *Progress) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		workers:  make([]*Worker, numWorkers),
		input:    make(chan FileInfo, numWorkers*10), // Buffer to reduce blocking
		ctx:      ctx,
		cancel:   cancel,
		progress: progress,
	}

	// Create workers
	for i := 0; i < numWorkers; i++ {
		pool.workers[i] = NewWorker(i, db, scanID, progress)
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
func (p *WorkerPool) Stop() {
	close(p.input)
	p.wg.Wait()
}

// Cancel cancels all workers immediately
func (p *WorkerPool) Cancel() {
	p.cancel()
	close(p.input)
	p.wg.Wait()
}

// GetInputChannel returns the input channel for direct writing
func (p *WorkerPool) GetInputChannel() chan<- FileInfo {
	return p.input
}
