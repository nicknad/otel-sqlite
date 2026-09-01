package sqlite

import (
	"context"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/batcher"
	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/otlp"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

// logPipeline is a running logs ingestion pipeline: ingress queue, batcher,
// writer, and OTLP logs server.
type logPipeline struct {
	writer  *Writer
	batcher *batcher.Batcher
	server  *otlp.Server
}

// metricPipeline is a running metrics ingestion pipeline: ingress queue,
// metric batcher, writer, and OTLP metrics server.
type metricPipeline struct {
	writer  *Writer
	batcher *batcher.MetricBatcher
	server  *otlp.MetricsServer
}

// startLogPipeline wires ingress → batcher → writer on dbPath and starts
// everything. The pipeline is stopped and waited on via t.Cleanup; tests
// that must stop earlier (before querying the DB file) call stop()
// explicitly — Stop is idempotent.
func startLogPipeline(t *testing.T, dbPath string) *logPipeline {
	t.Helper()
	ingressQueue := ingest.NewIngressQueue(1000)
	cmdQueue := storage.NewCommandQueue(100)

	writer, err := NewWriter(cmdQueue, &WriterConfig{
		Path:          dbPath,
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		WALMode:       true,
	})
	if err != nil {
		t.Fatalf("NewWriter(%s): %v", dbPath, err)
	}
	writer.Start(context.Background())

	b := batcher.NewBatcher(ingressQueue, cmdQueue, &batcher.BatcherConfig{
		BatchSize:     50,
		FlushInterval: 50 * time.Millisecond,
	})
	b.WithCommandFactory(func(batch *model.LogBatch) storage.Command {
		return NewWriteBatchCommand(batch)
	})
	b.Start(context.Background())

	svr := otlp.NewServer(ingressQueue, nil)

	p := &logPipeline{writer: writer, batcher: b, server: svr}
	t.Cleanup(p.stop)
	return p
}

// startMetricPipeline wires ingress → metric batcher → writer on dbPath and
// starts everything. See startLogPipeline for lifecycle notes.
func startMetricPipeline(t *testing.T, dbPath string) *metricPipeline {
	t.Helper()
	ingressQueue := ingest.NewMetricIngressQueue(1000)
	cmdQueue := storage.NewCommandQueue(100)

	writer, err := NewWriter(cmdQueue, &WriterConfig{
		Path:          dbPath,
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		WALMode:       true,
	})
	if err != nil {
		t.Fatalf("NewWriter(%s): %v", dbPath, err)
	}
	writer.Start(context.Background())

	mb := batcher.NewMetricBatcher(ingressQueue, writer, &batcher.MetricBatcherConfig{
		BatchSize:     50,
		FlushInterval: 50 * time.Millisecond,
	})
	mb.WithCommandFactory(func(batch *model.MetricBatch) storage.Command {
		return NewWriteMetricsCommand(batch)
	})
	mb.Start(context.Background())

	svr := otlp.NewMetricsServer(ingressQueue, nil)

	p := &metricPipeline{writer: writer, batcher: mb, server: svr}
	t.Cleanup(p.stop)
	return p
}

// stop stops the batcher and writer and waits for the writer to drain.
// Batcher.Stop drains the ingress queue before flushing, so no in-flight
// batch is dropped.
func (p *logPipeline) stop() {
	p.batcher.Stop()
	p.writer.Stop()
	p.writer.Wait()
}

// stop stops the metric batcher and writer and waits for the writer to
// drain. See logPipeline.stop for the drain rationale.
func (p *metricPipeline) stop() {
	p.batcher.Stop()
	p.writer.Stop()
	p.writer.Wait()
}
