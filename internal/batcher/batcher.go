// Package batcher provides batch building functionality for the ingestion pipeline.
// It collects log batches from the ingress queue, merges them into larger batches,
// then wraps each completed batch in a Command and submits it to a command queue.
//
// The batcher has no dependency on SQLite—it accepts a generic command factory
// that transforms a LogBatch into a storage.Command. The factory is injected
// by the application entry point (cmd/collector/main.go) or via WithCommandFactory.
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

// ErrorNotifier is the interface for notifying about error-level log records.
// The notifications.Worker implements this interface.
type ErrorNotifier interface {
	SendRecord(ctx context.Context, record *model.LogRecord) error
}

// suppressLogUntil is used to rate-limit error-notifier failure logs.
// Writes are only from the single batcher goroutine, so no mutex needed.
var suppressLogUntil time.Time

// NewWriteBatchCommand is a function type that creates a Command from a LogBatch.
// This indirection allows the batcher to be completely independent of SQLite.
// The application entry point injects the real factory; tests inject a mock.
type NewWriteBatchCommand func(batch *model.LogBatch) storage.Command

// Batcher collects log records and groups them into batches.
// It reads from an ingress queue and submits Commands to a command queue.
type Batcher struct {
	ingressQueue  ingest.IngressQueue
	cmdQueue      storage.CommandQueue
	batchSize     int
	flushInterval time.Duration

	// Current batch being built
	currentBatch *model.LogBatch
	mu           sync.Mutex
	aMetrics     *metrics.Metrics

	// Command factory — must be set via WithCommandFactory before Start
	newCmd NewWriteBatchCommand

	// Optional error notifier for severity >= Error
	errorNotifier          ErrorNotifier
	errorSeverityThreshold model.Severity

	// Control
	ctx     context.Context
	cancel  context.CancelCauseFunc
	wg      sync.WaitGroup
	stopped chan struct{}
}

// BatcherConfig holds configuration for the batcher.
type BatcherConfig struct {
	BatchSize int

	// FlushInterval is the maximum time to wait before flushing a
	// partial batch.  Defaults to 5s when zero.
	FlushInterval time.Duration

	// Metrics is optional. When non-nil the batcher reports batches
	// created, batch sizes and queue-depth gauges.
	Metrics *metrics.Metrics

	// ErrorSeverityThreshold is the minimum severity for which the
	// error notifier (if set) is called. Defaults to SeverityError.
	// Set to SeverityInfo to notify on all records during testing.
	ErrorSeverityThreshold model.Severity
}

// NewBatcher creates a new Batcher with the given configuration.
//
// The batcher sends completed batches as Command objects to the command queue.
// The command factory must be set via WithCommandFactory before calling Start.
// In production the factory should wrap sqlite.NewWriteBatchCommand; in tests
// it can be a mock.
const defaultBatcherFlushInterval = 5 * time.Second

func NewBatcher(ingressQueue ingest.IngressQueue, cmdQueue storage.CommandQueue, config *BatcherConfig) *Batcher {
	if config == nil {
		config = &BatcherConfig{
			BatchSize: 100,
		}
	}

	flushInterval := config.FlushInterval
	if flushInterval <= 0 {
		flushInterval = defaultBatcherFlushInterval
	}

	errThreshold := config.ErrorSeverityThreshold
	if errThreshold == 0 {
		errThreshold = model.SeverityError
	}

	return &Batcher{
		ingressQueue:           ingressQueue,
		cmdQueue:               cmdQueue,
		batchSize:              config.BatchSize,
		flushInterval:          flushInterval,
		currentBatch:           model.NewLogBatch(config.BatchSize),
		aMetrics:               config.Metrics,
		errorSeverityThreshold: errThreshold,
		stopped:                make(chan struct{}),
	}
}

// WithCommandFactory sets the command factory used to wrap completed batches.
// Must be called before Start. Panics if fn is nil.
func (b *Batcher) WithCommandFactory(fn NewWriteBatchCommand) *Batcher {
	if fn == nil {
		panic("batcher: WithCommandFactory requires a non-nil function")
	}
	b.newCmd = fn
	return b
}

// WithErrorNotifier sets an optional notifier that is called for every
// log record with SeverityNumber >= model.SeverityError.
// May be nil to disable error notification.
func (b *Batcher) WithErrorNotifier(n ErrorNotifier) *Batcher {
	b.errorNotifier = n
	return b
}

// Start starts the batcher goroutine.
func (b *Batcher) Start(ctx context.Context) {
	if b.newCmd == nil {
		panic("batcher: Start called before WithCommandFactory")
	}
	b.ctx, b.cancel = context.WithCancelCause(ctx)
	b.wg.Add(1)

	// #nosec G118 -- batcher uses WithCancelCause context, not Background
	go b.run()
}

// Stop stops the batcher and waits for it to finish.
func (b *Batcher) Stop() {
	if b.cancel != nil {
		b.cancel(errors.New("batcher stopped"))
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

	flushTicker := time.NewTicker(b.flushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			log.Printf("batcher: shutting down: %v", context.Cause(b.ctx))
			// Drain any in-flight batches before flushing: the shutdown
			// select can otherwise pick ctx.Done() while the ingress queue
			// still holds data, silently dropping it.
			b.drainIngress()
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
			b.addBatch(batch)
			isFull := b.currentBatch.Size() >= b.batchSize
			b.mu.Unlock()

			b.aMetrics.UpdateIngressQueueDepth(b.ingressQueue.Len())
			b.aMetrics.UpdateBatchQueueDepth(b.cmdQueue.Len())

			if isFull {
				b.flushCurrentBatch()
			}
		}
	}
}

// addBatch merges an incoming batch into the current batch, notifying the
// error notifier per record. The caller must hold b.mu.
func (b *Batcher) addBatch(batch *model.LogBatch) {
	if batch == nil || batch.IsEmpty() {
		return
	}
	// Carry resource from the incoming batch if current batch has none
	if b.currentBatch.Resource == nil && batch.Resource != nil {
		b.currentBatch.Resource = batch.Resource
	}
	for _, record := range batch.Records {
		b.currentBatch.AddRecord(record)
		b.maybeNotify(record)
	}
}

// drainIngress consumes any remaining batches from the ingress queue so that
// shutdown does not drop in-flight data. Non-blocking: concurrent producers
// are expected to have stopped (gRPC server shutdown precedes the batcher).
func (b *Batcher) drainIngress() {
	for {
		select {
		case batch, ok := <-b.ingressQueue.Chan():
			if !ok {
				return
			}
			b.mu.Lock()
			b.addBatch(batch)
			b.mu.Unlock()
		default:
			return
		}
	}
}

// flushCurrentBatch sends the current batch as a Command to the command queue
// and starts a new batch. Uses context.Background() so the flush works even
// when the batcher context is canceled.
func (b *Batcher) flushCurrentBatch() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.currentBatch.IsEmpty() {
		return
	}

	// Wrap the batch in a Command using the injected factory.
	cmd := b.newCmd(b.currentBatch)

	_ = b.cmdQueue.Send(context.Background(), cmd)

	b.aMetrics.IncrementBatchesCreated()
	b.aMetrics.RecordBatchSize(b.currentBatch.Size())

	// Start a new batch
	b.currentBatch = model.NewLogBatch(b.batchSize)
}

// maybeNotify sends a log record to the error notifier if its severity
// meets or exceeds the configured threshold.
func (b *Batcher) maybeNotify(record *model.LogRecord) {
	if b.errorNotifier == nil || record.SeverityNumber < b.errorSeverityThreshold {
		return
	}
	if err := b.errorNotifier.SendRecord(b.ctx, record); err != nil {
		if time.Now().After(suppressLogUntil) {
			log.Printf("batcher: error notifier: %v (suppressing for 10s)", err)
			suppressLogUntil = time.Now().Add(10 * time.Second)
		}
	}
}

// Flush forces the current batch to be flushed.
func (b *Batcher) Flush() {
	b.flushCurrentBatch()
}
