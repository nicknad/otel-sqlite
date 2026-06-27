package batcher

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nnadolski/otel-sqlite/internal/ingest"
	"github.com/nnadolski/otel-sqlite/internal/model"
)

// drainBatchQueue reads all batches until the queue is closed and returns the total count.
func drainBatchQueue(q ingest.BatchQueue) int {
	var total int
	for {
		batch, err := q.Receive(context.Background())
		if err == ingest.ErrQueueClosed {
			return total
		}
		if err != nil {
			return total
		}
		total += batch.Size()
	}
}

type testQueues struct {
	ingress ingest.IngressQueue
	batch   ingest.BatchQueue
}

func newTestQueues(capacity int) *testQueues {
	return &testQueues{
		ingress: ingest.NewIngressQueue(capacity),
		batch:   ingest.NewBatchQueue(capacity),
	}
}

func TestBatcherBatchBySize(t *testing.T) {
	q := newTestQueues(100)
	b := NewBatcher(q.ingress, q.batch, &BatcherConfig{BatchSize: 5})
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)

	// Send 12 records (2 full batches + 2 leftover)
	for i := 0; i < 12; i++ {
		if err := q.ingress.Send(ctx, &model.LogRecord{Body: "msg"}); err != nil {
			t.Fatalf("Send ingress error: %v", err)
		}
	}

	// Allow batcher to process
	time.Sleep(50 * time.Millisecond)

	// Stop batcher (flushes remaining)
	cancel()
	b.Wait()

	// Close the batch queue to unblock the drain
	q.batch.Close()

	total := drainBatchQueue(q.batch)
	if total != 12 {
		t.Errorf("expected 12 records total, got %d", total)
	}
}

func TestBatcherEmptyOnStop(t *testing.T) {
	q := newTestQueues(10)
	b := NewBatcher(q.ingress, q.batch, &BatcherConfig{BatchSize: 100})
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)

	// Send a few records
	for i := 0; i < 3; i++ {
		if err := q.ingress.Send(ctx, &model.LogRecord{Body: "msg"}); err != nil {
			t.Fatalf("Send error: %v", err)
		}
	}

	time.Sleep(20 * time.Millisecond)
	cancel()
	b.Wait()

	q.batch.Close()
	total := drainBatchQueue(q.batch)
	if total != 3 {
		t.Errorf("expected 3 records flushed on stop, got %d", total)
	}
}

func TestBatcherBackpressure(t *testing.T) {
	q := newTestQueues(2) // small batch queue
	b := NewBatcher(q.ingress, q.batch, &BatcherConfig{BatchSize: 1})

	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)

	// Fill the batch queue by sending records
	// batch queue cap is 2, so we can send up to 2 batches before blocking
	for i := 0; i < 5; i++ {
		if err := q.ingress.Send(ctx, &model.LogRecord{Body: "msg"}); err != nil {
			t.Fatalf("Send error at %d: %v", i, err)
		}
	}

	// Allow batcher to fill the batch queue
	time.Sleep(50 * time.Millisecond)

	// Drain the batch queue on a goroutine
	var drained atomic.Int32
	go func() {
		for {
			_, err := q.batch.Receive(context.Background())
			if err != nil {
				return
			}
			drained.Add(1)
		}
	}()

	// Now we can send more
	err := q.ingress.Send(ctx, &model.LogRecord{Body: "more"})
	if err != nil {
		t.Errorf("expected send to succeed after draining, got %v", err)
	}

	cancel()
	b.Wait()
	q.batch.Close()
}

func TestBatcherFlush(t *testing.T) {
	q := newTestQueues(10)
	b := NewBatcher(q.ingress, q.batch, &BatcherConfig{BatchSize: 100})
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)

	// Send a few records
	for i := 0; i < 7; i++ {
		if err := q.ingress.Send(ctx, &model.LogRecord{Body: "msg"}); err != nil {
			t.Fatalf("Send error: %v", err)
		}
	}

	time.Sleep(20 * time.Millisecond)

	// Manually trigger flush
	b.Flush()
	time.Sleep(10 * time.Millisecond)

	// Check that the batch was sent to the batch queue
	batch, err := q.batch.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive error after Flush(): %v", err)
	}
	if batch.Size() != 7 {
		t.Errorf("expected batch size 7, got %d", batch.Size())
	}

	cancel()
	b.Wait()
	q.batch.Close()
}
