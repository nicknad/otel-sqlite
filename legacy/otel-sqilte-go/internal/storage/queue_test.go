package storage

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// testCommand is a simple Command implementation for testing.
type testCommand struct {
	id       int
	executed bool
}

func (c *testCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	c.executed = true
	return nil
}

func TestCommandQueueSendReceive(t *testing.T) {
	q := NewCommandQueue(10)
	ctx := context.Background()

	cmd := &testCommand{id: 1}
	if err := q.Send(ctx, cmd); err != nil {
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
	if got != cmd {
		t.Errorf("Receive() returned different command")
	}

	if q.Len() != 0 {
		t.Errorf("Len() after receive = %d, want 0", q.Len())
	}
}

func TestCommandQueueFIFO(t *testing.T) {
	q := NewCommandQueue(10)
	ctx := context.Background()

	// Send three commands in order
	cmds := []*testCommand{
		{id: 1},
		{id: 2},
		{id: 3},
	}
	for _, cmd := range cmds {
		if err := q.Send(ctx, cmd); err != nil {
			t.Fatalf("Send() error for cmd %d: %v", cmd.id, err)
		}
	}

	// Receive in order (FIFO)
	for i, expected := range cmds {
		got, err := q.Receive(ctx)
		if err != nil {
			t.Fatalf("Receive() error at %d: %v", i, err)
		}
		if got != expected {
			t.Errorf("Receive() at %d = cmd %d, want cmd %d", i, got.(*testCommand).id, expected.id)
		}
	}
}

func TestCommandQueueBlockingSend(t *testing.T) {
	q := NewCommandQueue(1)
	ctx := context.Background()

	// Fill the queue
	if err := q.Send(ctx, &testCommand{id: 1}); err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	// This send should block; use a context with timeout
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	err := q.Send(ctxTimeout, &testCommand{id: 2})
	if err == nil {
		t.Error("expected error on full queue, got nil")
	}
}

func TestCommandQueueClose(t *testing.T) {
	q := NewCommandQueue(10)
	ctx := context.Background()

	// Send a command then close
	cmd := &testCommand{id: 1}
	if err := q.Send(ctx, cmd); err != nil {
		t.Fatalf("Send() error: %v", err)
	}
	q.Close()

	// Receive should still work for queued items
	got, err := q.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive() after close error: %v", err)
	}
	if got != cmd {
		t.Error("got different command")
	}

	// Second receive should return ErrQueueClosed
	_, err = q.Receive(ctx)
	if err != ErrQueueClosed {
		t.Errorf("expected ErrQueueClosed, got %v", err)
	}
}

func TestCommandQueueCancelContext(t *testing.T) {
	q := NewCommandQueue(10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := q.Receive(ctx)
	if err == nil {
		t.Error("expected error on canceled context, got nil")
	}
}

func TestCommandQueueChan(t *testing.T) {
	q := NewCommandQueue(3)
	ctx := context.Background()

	// Verify Chan() returns a readable channel
	ch := q.Chan()
	if ch == nil {
		t.Fatal("Chan() returned nil")
	}

	cmd := &testCommand{id: 42}
	if err := q.Send(ctx, cmd); err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	select {
	case got := <-ch:
		if got != cmd {
			t.Error("received different command via Chan()")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for command via Chan()")
	}
}

func TestCommandQueueLenCap(t *testing.T) {
	q := NewCommandQueue(5)
	ctx := context.Background()

	if q.Cap() != 5 {
		t.Errorf("Cap() = %d, want 5", q.Cap())
	}
	if q.Len() != 0 {
		t.Errorf("Len() = %d, want 0", q.Len())
	}

	for i := 0; i < 3; i++ {
		if err := q.Send(ctx, &testCommand{id: i}); err != nil {
			t.Fatalf("Send() error at %d: %v", i, err)
		}
	}

	if q.Len() != 3 {
		t.Errorf("Len() = %d, want 3", q.Len())
	}
}

func TestErrQueueClosed(t *testing.T) {
	e := ErrQueueClosed
	if e.Error() != "command queue closed" {
		t.Errorf("Error() = %q, want %q", e.Error(), "command queue closed")
	}
}
