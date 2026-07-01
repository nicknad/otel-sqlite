// Package batcher provides batch building functionality for the ingestion pipeline.
// It collects individual log records and groups them into batches.
package batcher

import (
	"context"
	"sync"

	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/metrics"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// Batcher collects log records and groups them into batches.
// It reads from an ingress queue and writes batches to a batch queue.
type Batcher struct {
	ingressQueue ingest.IngressQueue
	batchQueue   ingest.BatchQueue
	batchSize    int

	// Current batch being built
	currentBatch *model.LogBatch
	mu           sync.Mutex
	aMetrics     *metrics.Metrics

	// Control
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped chan struct{}
}

// BatcherConfig holds configuration for the batcher.
type BatcherConfig struct {
	BatchSize int

	// Metrics is optional. When non-nil the batcher reports batches
	// created, batch sizes and queue-depth gauges.
	Metrics *metrics.Metrics
}

// NewBatcher creates a new Batcher with the given configuration.
func NewBatcher(ingressQueue ingest.IngressQueue, batchQueue ingest.BatchQueue, config *BatcherConfig) *Batcher {
	if config == nil {
		config = &BatcherConfig{
			BatchSize: 100,
		}
	}

	return &Batcher{
		ingressQueue: ingressQueue,
		batchQueue:   batchQueue,
		batchSize:    config.BatchSize,
		currentBatch: model.NewLogBatch(config.BatchSize),
		aMetrics:     config.Metrics,
		stopped:      make(chan struct{}),
	}
}

// Start starts the batcher goroutine.
func (b *Batcher) Start(ctx context.Context) {
	b.ctx, b.cancel = context.WithCancel(ctx)
	b.wg.Add(1)

	go b.run()
}

// Stop stops the batcher and waits for it to finish.
func (b *Batcher) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
	<-b.stopped
}

// Wait waits for the batcher to finish.
func (b *Batcher) Wait() {
	b.wg.Wait()
}

// run is the main batcher loop.
func (b *Batcher) run() {
	defer b.wg.Done()
	defer close(b.stopped)

	for {
		select {
		case <-b.ctx.Done():
			// Flush any remaining records before exiting (use background ctx so Send works)
			b.flushCurrentBatch()
			return
		default:
			// Try to receive a record
			record, err := b.ingressQueue.Receive(b.ctx)
			if err != nil {
				// Queue closed or error, flush and exit
				b.flushCurrentBatch()
				return
			}

			// Add record to current batch and check size under lock
			b.mu.Lock()
			b.currentBatch.AddRecord(record)
			isFull := b.currentBatch.Size() >= b.batchSize
			b.mu.Unlock()

			b.aMetrics.UpdateIngressQueueDepth(b.ingressQueue.Len())
			b.aMetrics.UpdateBatchQueueDepth(b.batchQueue.Len())

			if isFull {
				b.flushCurrentBatch()
			}
		}
	}
}

// flushCurrentBatch sends the current batch to the batch queue and starts a new one.
// Uses context.Background() for the Send so it works even when the batcher context is canceled.
func (b *Batcher) flushCurrentBatch() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.currentBatch.IsEmpty() {
		return
	}

	// Send the current batch with a background context so flush works during shutdown
	_ = b.batchQueue.Send(context.Background(), b.currentBatch)

	b.aMetrics.IncrementBatchesCreated()
	b.aMetrics.RecordBatchSize(b.currentBatch.Size())

	// Start a new batch
	b.currentBatch = model.NewLogBatch(b.batchSize)
}

// Flush forces the current batch to be flushed.
// This is useful for ensuring all records are processed before shutdown.
func (b *Batcher) Flush() {
	b.flushCurrentBatch()
}
