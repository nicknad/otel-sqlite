package ingest

import (
	"context"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// metricBatchN builds a metric batch with n data points in one series.
func metricBatchN(n int) *model.MetricBatch {
	b := model.NewMetricBatch(1)
	b.Resource = &model.Resource{ID: "res"}
	m := &model.Metric{
		ID:     "metric-1",
		Type:   model.MetricTypeGauge,
		Series: []*model.MetricSeries{{ID: "series-1"}},
	}
	for i := 0; i < n; i++ {
		v := float64(i)
		m.Series[0].DataPoints = append(m.Series[0].DataPoints,
			&model.DataPoint{Timestamp: int64(i), DoubleValue: &v})
	}
	b.AddMetric(m)
	return b
}

func TestMetricIngressQueueSendReceive(t *testing.T) {
	q := NewMetricIngressQueue(10)
	ctx := context.Background()

	batch := metricBatchN(3)
	if err := q.Send(ctx, batch); err != nil {
		t.Fatalf("Send() error: %v", err)
	}
	if q.Len() != 1 || q.Cap() != 10 {
		t.Errorf("Len/Cap = %d/%d, want 1/10", q.Len(), q.Cap())
	}

	got, err := q.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive() error: %v", err)
	}
	if got != batch || got.Size() != 3 {
		t.Errorf("Receive() = %v (size %d), want the original batch (size 3)", got, got.Size())
	}
	if q.Len() != 0 {
		t.Errorf("Len() after receive = %d, want 0", q.Len())
	}
}

func TestMetricIngressQueueBlockingSend(t *testing.T) {
	q := NewMetricIngressQueue(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := q.Send(ctx, metricBatchN(1)); err != nil {
		t.Fatalf("first Send(): %v", err)
	}

	// Second send on a full queue must block until the first is received.
	done := make(chan error, 1)
	go func() {
		done <- q.Send(ctx, metricBatchN(1))
	}()
	select {
	case err := <-done:
		t.Fatalf("Send() on full queue returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := q.Receive(ctx); err != nil {
		t.Fatalf("Receive(): %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Send() after drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send() still blocked after drain")
	}
}

func TestMetricIngressQueueClose(t *testing.T) {
	q := NewMetricIngressQueue(2)
	ctx := context.Background()

	_ = q.Send(ctx, metricBatchN(1))
	_ = q.Send(ctx, metricBatchN(1))
	q.Close()

	// Receive drains the buffered items, then reports ErrQueueClosed.
	if _, err := q.Receive(ctx); err != nil {
		t.Fatalf("Receive() before drain: %v", err)
	}
	if _, err := q.Receive(ctx); err != nil {
		t.Fatalf("Receive() before drain: %v", err)
	}
	if _, err := q.Receive(ctx); err != ErrQueueClosed {
		t.Fatalf("Receive() after close = %v, want ErrQueueClosed", err)
	}
}

func TestMetricIngressQueueCancelContext(t *testing.T) {
	q := NewMetricIngressQueue(1)
	ctx, cancel := context.WithCancel(context.Background())

	if err := q.Send(ctx, metricBatchN(1)); err != nil {
		t.Fatalf("Send(): %v", err)
	}
	cancel()

	if err := q.Send(ctx, metricBatchN(1)); err == nil {
		t.Error("Send() with canceled context should error")
	}
}
