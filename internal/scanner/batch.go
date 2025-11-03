package scanner

import (
	"context"
	"sync"

	"github.com/mmenanno/media-usage-finder/internal/database"
)

// BatchAccumulator collects files and flushes them to the database in batches
// This provides significant performance improvement over individual inserts
//
// Thread-safety:
// - Multiple workers can safely call Add() concurrently
// - Mutex protects the buffer and ensures atomic flush operations
// - Flush() can be called concurrently and will only execute once per batch
type BatchAccumulator struct {
	db        *database.DB
	ctx       context.Context
	batchSize int
	mu        sync.Mutex
	buffer    []*database.File
	onFlush   func(count int) // Callback for progress tracking
}

// NewBatchAccumulator creates a new batch accumulator
func NewBatchAccumulator(db *database.DB, ctx context.Context, batchSize int, onFlush func(int)) *BatchAccumulator {
	return &BatchAccumulator{
		db:        db,
		ctx:       ctx,
		batchSize: batchSize,
		buffer:    make([]*database.File, 0, batchSize),
		onFlush:   onFlush,
	}
}

// Add adds a file to the batch, flushing if batch size is reached
func (ba *BatchAccumulator) Add(file *database.File) error {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	ba.buffer = append(ba.buffer, file)

	// Flush if we've reached batch size
	if len(ba.buffer) >= ba.batchSize {
		return ba.flushLocked()
	}

	return nil
}

// Flush flushes any remaining files in the buffer to the database
func (ba *BatchAccumulator) Flush() error {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	return ba.flushLocked()
}

// flushLocked performs the actual flush (must be called with mutex held)
func (ba *BatchAccumulator) flushLocked() error {
	if len(ba.buffer) == 0 {
		return nil
	}

	// Check for context cancellation before flushing
	select {
	case <-ba.ctx.Done():
		return ba.ctx.Err()
	default:
	}

	// Batch upsert all files
	if err := ba.db.BatchUpsertFiles(ba.ctx, ba.buffer); err != nil {
		return err
	}

	// Notify callback
	if ba.onFlush != nil {
		ba.onFlush(len(ba.buffer))
	}

	// Clear buffer
	ba.buffer = ba.buffer[:0]

	return nil
}

// Size returns the current number of files in the buffer
func (ba *BatchAccumulator) Size() int {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	return len(ba.buffer)
}
