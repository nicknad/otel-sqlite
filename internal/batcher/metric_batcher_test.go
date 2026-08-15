package batcher

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

// mockWriteMetricsCommand is a lightweight stand-in for
// sqlite.WriteMetricsCommand used in batcher tests.
type mockWriteMetricsCommand struct {
	batch  *model.MetricBatch
	points int
}

func (m *mockWriteMetricsCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	return nil
}

func (m *mockWriteMetricsCommand) Size() int {
	return m.points
}

func (m *mockWriteMetricsCommand) Batch() *model.MetricBatch {
	return m.batch
}

// newMockMetricsCmd creates a mock command from a batch (same signature as
// sqlite.NewWriteMetricsCommand).
func newMockMetricsCmd(batch *model.MetricBatch) storage.Command {
	points := 0
	if batch != nil {
		points = batch.Size()
	}
	return &mockWriteMetricsCommand{batch: batch, points: points}
}

type testMetricQueues struct {
	ingress  ingest.MetricIngressQueue
	cmdQueue storage.CommandQueue
}

func newTestMetricQueues(capacity int) *testMetricQueues {
	return &testMetricQueues{
		ingress:  ingest.NewMetricIngressQueue(capacity),
		cmdQueue: storage.NewCommandQueue(capacity),
	}
}

// makeMetricBatch creates a MetricBatch with n data points across one series.
func makeMetricBatch(resourceID string, n int) *model.MetricBatch {
	batch := model.NewMetricBatch(1)
	batch.Resource = &model.Resource{ID: resourceID}
	metric := &model.Metric{
		ID:     "metric-" + resourceID,
		Type:   model.MetricTypeGauge,
		Series: []*model.MetricSeries{{ID: "series-" + resourceID}},
	}
	for i := 0; i < n; i++ {
		v := float64(i)
		metric.Series[0].DataPoints = append(metric.Series[0].DataPoints,
			&model.DataPoint{Timestamp: int64(i), DoubleValue: &v})
	}
	batch.AddMetric(metric)
	return batch
}

func TestMetricBatcherBatchesByPoints(t *testing.T) {
	q := newTestMetricQueues(100)
	b := NewMetricBatcher(q.ingress, q.cmdQueue, &MetricBatcherConfig{BatchSize: 5})
	b.WithCommandFactory(newMockMetricsCmd)

	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	defer cancel()

	// Send 3 batches of 3 points each (same resource). Batches merge until
	// the 5-point threshold: 3+3=6 triggers a flush of 6, the remaining 3
	// flush at shutdown. Total across all flushes must be 9.
	_ = q.ingress.Send(ctx, makeMetricBatch("r1", 3))
	_ = q.ingress.Send(ctx, makeMetricBatch("r1", 3))
	_ = q.ingress.Send(ctx, makeMetricBatch("r1", 3))

	// Wait for the first flush, then stop to flush the remainder.
	deadline := time.Now().Add(2 * time.Second)
	for q.cmdQueue.Len() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for batcher flush")
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.Stop()
	q.cmdQueue.Close()

	total := drainCommandQueue(q.cmdQueue)
	if total != 9 {
		t.Errorf("total points flushed = %d, want 9", total)
	}
}

// TestMetricBatcherSeparatesResources verifies that batches from different
// resources are not merged into one command — the resource row must always be
// inserted before its metrics, and a merged batch could only carry one.
func TestMetricBatcherSeparatesResources(t *testing.T) {
	q := newTestMetricQueues(100)
	b := NewMetricBatcher(q.ingress, q.cmdQueue, &MetricBatcherConfig{BatchSize: 1000})
	b.WithCommandFactory(newMockMetricsCmd)

	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	defer cancel()

	// Same resource → merged; different resource → separate flush.
	_ = q.ingress.Send(ctx, makeMetricBatch("r1", 2))
	_ = q.ingress.Send(ctx, makeMetricBatch("r1", 2))
	_ = q.ingress.Send(ctx, makeMetricBatch("r2", 2))

	// Wait for the resource-triggered flush, then stop to flush r2.
	deadline := time.Now().Add(2 * time.Second)
	for q.cmdQueue.Len() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for resource-separated flush")
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.Stop()
	q.cmdQueue.Close()

	// Drain and check per-command sizes: r1(2)+r1(2)=4 flushed when r2
	// arrives, then r2(2) at shutdown.
	var sizes []int
	for {
		cmd, err := q.cmdQueue.Receive(context.Background())
		if err == storage.ErrQueueClosed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, cmd.(interface{ Size() int }).Size())
	}
	if len(sizes) != 2 || sizes[0] != 4 || sizes[1] != 2 {
		t.Errorf("flush sizes = %v, want [4 2] (r1 merged, r2 separate)", sizes)
	}
}

// TestMetricBatcherFlushOnClose verifies shutdown flushes the partial batch.
func TestMetricBatcherFlushOnClose(t *testing.T) {
	q := newTestMetricQueues(10)
	b := NewMetricBatcher(q.ingress, q.cmdQueue, &MetricBatcherConfig{BatchSize: 1000})
	b.WithCommandFactory(newMockMetricsCmd)

	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	defer cancel()

	_ = q.ingress.Send(ctx, makeMetricBatch("r1", 3))

	// Give the batcher goroutine a moment to receive the batch before
	// flushing from the test goroutine (same pattern as the log batcher
	// TestBatcherFlush).
	time.Sleep(20 * time.Millisecond)
	b.Flush()
	q.cmdQueue.Close()

	total := drainCommandQueue(q.cmdQueue)
	if total != 3 {
		t.Errorf("total points flushed = %d, want 3", total)
	}
}
