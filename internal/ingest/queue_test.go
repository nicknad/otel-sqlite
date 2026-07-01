package ingest

import (
	"context"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

func TestIngressQueueSendReceive(t *testing.T) {
	q := NewIngressQueue(10)
	ctx := context.Background()

	r := &model.LogRecord{Body: "hello"}
	if err := q.Send(ctx, r); err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	if q.Len() != 1 {
		t.Errorf("Len() = %d, want 1", q.Len())
	}
	if q.Cap() != 10 {
		t.Errorf("Cap() = %d, want 10", q.Cap())
	}

	got, err := q.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive() error: %v", err)
	}
	if got.Body != "hello" {
		t.Errorf("Receive() body = %q, want %q", got.Body, "hello")
	}

	if q.Len() != 0 {
		t.Errorf("Len() after receive = %d, want 0", q.Len())
	}
}

func TestIngressQueueBlockingSend(t *testing.T) {
	q := NewIngressQueue(1)
	ctx := context.Background()

	// Fill the queue
	if err := q.Send(ctx, &model.LogRecord{}); err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	// This send should block; use a context with timeout
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	err := q.Send(ctxTimeout, &model.LogRecord{})
	if err == nil {
		t.Error("expected error on full queue, got nil")
	}
}

func TestIngressQueueClose(t *testing.T) {
	q := NewIngressQueue(10)
	ctx := context.Background()

	// Send a record then close
	if err := q.Send(ctx, &model.LogRecord{Body: "test"}); err != nil {
		t.Fatalf("Send() error: %v", err)
	}
	q.Close()

	// Receive should still work for queued items
	got, err := q.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive() after close error: %v", err)
	}
	if got.Body != "test" {
		t.Errorf("got body %q, want %q", got.Body, "test")
	}

	// Second receive should return ErrQueueClosed
	_, err = q.Receive(ctx)
	if err != ErrQueueClosed {
		t.Errorf("expected ErrQueueClosed, got %v", err)
	}
}

func TestIngressQueueCancelContext(t *testing.T) {
	q := NewIngressQueue(10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := q.Receive(ctx)
	if err == nil {
		t.Error("expected error on canceled context, got nil")
	}
}

func TestBatchQueueSendReceive(t *testing.T) {
	q := NewBatchQueue(10)
	ctx := context.Background()

	b := model.NewLogBatch(2)
	b.AddRecord(&model.LogRecord{Body: "a"})
	b.AddRecord(&model.LogRecord{Body: "b"})

	if err := q.Send(ctx, b); err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	if q.Len() != 1 {
		t.Errorf("Len() = %d, want 1", q.Len())
	}
	if q.Cap() != 10 {
		t.Errorf("Cap() = %d, want 10", q.Cap())
	}

	got, err := q.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive() error: %v", err)
	}
	if got.Size() != 2 {
		t.Errorf("batch size = %d, want 2", got.Size())
	}
}

func TestBatchQueueClose(t *testing.T) {
	q := NewBatchQueue(10)
	ctx := context.Background()
	q.Close()

	_, err := q.Receive(ctx)
	if err != ErrQueueClosed {
		t.Errorf("expected ErrQueueClosed, got %v", err)
	}
}

func TestQueueError(t *testing.T) {
	e := &QueueError{Message: "test error"}
	if e.Error() != "test error" {
		t.Errorf("Error() = %q, want %q", e.Error(), "test error")
	}
}
