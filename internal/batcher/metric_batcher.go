// Package batcher provides batch building functionality for the ingestion pipeline.
// The MetricBatcher is the metrics-signal analogue of Batcher: it collects
// metric batches from the metrics ingress queue, merges them into larger
// batches, then wraps each completed batch in a Command and submits it to an
// executor — the *sqlite.Writer, which owns its command queue. In shared
// mode that is the single log/metrics writer; in separate-DB mode it is the
// dedicated metrics writer.
package batcher

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/metrics"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

// NewWriteMetricsCommand is a function type that creates a Command from a
// MetricBatch. This indirection keeps the batcher independent of SQLite,
// exactly like NewWriteBatchCommand for logs.
type NewWriteMetricsCommand func(batch *model.MetricBatch) storage.Command

// commandExecutorWithDepth is the interface the MetricBatcher needs from its
// submit target: command submission plus queue-depth reporting for the
// batch-queue-depth gauge. *sqlite.Writer satisfies it.
type commandExecutorWithDepth interface {
	storage.CommandExecutor
	QueueDepth() int
}

// MetricBatcher collects metric batches and groups them into larger batches.
// It reads from a metrics ingress queue and submits Commands to an executor
// (the SQLite writer).
//
// Unlike the log batcher it has no error notifier: severity is a log-only
// concept. Batches are merged only when they share the same resource so the
// resource row is always inserted before its metrics (the invariant the
// writer relies on).
type MetricBatcher struct {
	ingressQueue  ingest.MetricIngressQueue
	executor      storage.CommandExecutor
	batchSize     int // counted in data points
	flushInterval time.Duration

	// Current batch being built
	currentBatch *model.MetricBatch
	mu           sync.Mutex
	aMetrics     *metrics.Metrics

	// Command factory — must be set via WithCommandFactory before Start
	newCmd NewWriteMetricsCommand

	// Control
	ctx     context.Context
	cancel  context.CancelCauseFunc
	wg      sync.WaitGroup
	stopped chan struct{}
}

// MetricBatcherConfig holds configuration for the metric batcher.
type MetricBatcherConfig struct {
	// BatchSize is the target number of data points per command.
	BatchSize int

	// FlushInterval is the maximum time to wait before flushing a partial
	// batch. Defaults to 5s when zero.
	FlushInterval time.Duration

	// Metrics is optional. When non-nil the batcher reports batches
	// created, batch sizes and queue-depth gauges.
	Metrics *metrics.Metrics
}

// NewMetricBatcher creates a new MetricBatcher with the given configuration.
// executor is the submit target for completed batches — the *sqlite.Writer
// in both shared and separate-DB modes.
func NewMetricBatcher(ingressQueue ingest.MetricIngressQueue, executor storage.CommandExecutor, config *MetricBatcherConfig) *MetricBatcher { //nolint:lll
	if config == nil {
		config = &MetricBatcherConfig{BatchSize: 100}
	}
	flushInterval := config.FlushInterval
	if flushInterval <= 0 {
		flushInterval = defaultBatcherFlushInterval
	}
	return &MetricBatcher{
		ingressQueue:  ingressQueue,
		executor:      executor,
		batchSize:     config.BatchSize,
		flushInterval: flushInterval,
		currentBatch:  model.NewMetricBatch(config.BatchSize),
		aMetrics:      config.Metrics,
		stopped:       make(chan struct{}),
	}
}

// WithCommandFactory sets the command factory used to wrap completed batches.
// Must be called before Start. Panics if fn is nil.
func (b *MetricBatcher) WithCommandFactory(fn NewWriteMetricsCommand) *MetricBatcher {
	if fn == nil {
		panic("batcher: MetricBatcher.WithCommandFactory requires a non-nil function")
	}
	b.newCmd = fn
	return b
}

// Start starts the batcher goroutine.
func (b *MetricBatcher) Start(ctx context.Context) {
	if b.newCmd == nil {
		panic("batcher: MetricBatcher.Start called before WithCommandFactory")
	}
	b.ctx, b.cancel = context.WithCancelCause(ctx)
	b.wg.Add(1)

	// #nosec G118 -- batcher uses WithCancelCause context, not Background
	go b.run()
}

// Stop stops the batcher and waits for it to finish.
func (b *MetricBatcher) Stop() {
	if b.cancel != nil {
		b.cancel(errors.New("metric batcher stopped"))
	}
	<-b.stopped
}

// Wait waits for the batcher to finish.
func (b *MetricBatcher) Wait() {
	b.wg.Wait()
}

// run is the main batcher loop.
func (b *MetricBatcher) run() {
	defer b.wg.Done()
	defer close(b.stopped)

	flushTicker := time.NewTicker(b.flushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			log.Printf("metric batcher: shutting down: %v", context.Cause(b.ctx))
			b.flushCurrentBatch()
			return

		case <-flushTicker.C:
			b.flushCurrentBatch()

		case batch, ok := <-b.ingressQueue.Chan():
			if !ok {
				b.flushCurrentBatch()
				return
			}

			b.mu.Lock()
			if !b.merge(batch) {
				// Different resource: flush the current batch first, then
				// adopt the incoming one so nothing is dropped.
				b.flushLocked()
				b.currentBatch = batch
			}
			b.aMetrics.UpdateIngressQueueDepth(b.ingressQueue.Len())
			// The executor owns the command queue; report its depth through
			// the optional QueueDepth() method (*sqlite.Writer implements it).
			if qd, ok := b.executor.(commandExecutorWithDepth); ok {
				b.aMetrics.UpdateBatchQueueDepth(qd.QueueDepth())
			}
			isFull := b.currentBatch.Size() >= b.batchSize
			b.mu.Unlock()

			if isFull {
				b.flushCurrentBatch()
			}
		}
	}
}

// merge merges an incoming batch into the current batch. It returns false
// when the incoming batch could not be merged because its resource differs
// from the current batch's resource (the caller then flushes first).
func (b *MetricBatcher) merge(batch *model.MetricBatch) bool {
	if batch == nil || batch.IsEmpty() {
		return true
	}
	if b.currentBatch.IsEmpty() {
		b.currentBatch = batch
		return true
	}
	if b.currentBatch.Resource != nil && batch.Resource != nil &&
		b.currentBatch.Resource.ID != batch.Resource.ID {
		return false
	}
	b.currentBatch.Metrics = append(b.currentBatch.Metrics, batch.Metrics...)
	return true
}

// flushCurrentBatch submits the current batch as a Command to the executor
// and starts a new batch. Uses context.Background() so the flush works even
// when the batcher context is canceled.
func (b *MetricBatcher) flushCurrentBatch() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

// flushLocked flushes the current batch; the caller must hold b.mu.
// Uses context.Background() so the flush works even when the batcher context
// is canceled. sqlite.Writer.Submit is exactly cmdQueue.Send, so backpressure
// semantics are unchanged from the pre-executor wiring.
func (b *MetricBatcher) flushLocked() {
	if b.currentBatch.IsEmpty() {
		return
	}

	cmd := b.newCmd(b.currentBatch)
	_ = b.executor.Submit(context.Background(), cmd)

	b.aMetrics.IncrementBatchesCreated()
	b.aMetrics.RecordBatchSize(b.currentBatch.Size())

	b.currentBatch = model.NewMetricBatch(b.batchSize)
}

// Flush forces the current batch to be flushed.
func (b *MetricBatcher) Flush() {
	b.flushCurrentBatch()
}
