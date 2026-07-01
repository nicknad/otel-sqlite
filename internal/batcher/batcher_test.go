package batcher

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

// drainCommandQueue reads all commands until the queue is closed and returns
// the total record count from any WriteBatchCommand payloads.
func drainCommandQueue(q storage.CommandQueue) int {
	var total int
	for {
		cmd, err := q.Receive(context.Background())
		if err == storage.ErrQueueClosed {
			return total
		}
		if err != nil {
			return total
		}
		// Extract record count via the Size() method if available.
		if sized, ok := cmd.(interface{ Size() int }); ok {
			total += sized.Size()
		} else {
			total++
		}
	}
}

// mockWriteBatchCommand is a lightweight stand-in for sqlite.WriteBatchCommand
// used in tests so that batcher tests do not depend on the sqlite package.
type mockWriteBatchCommand struct {
	batch   *model.LogBatch
	records int
}

func (m *mockWriteBatchCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	return nil
}

func (m *mockWriteBatchCommand) Size() int {
	return m.records
}

func (m *mockWriteBatchCommand) Batch() *model.LogBatch {
	return m.batch
}

// newMockCmd creates a mock command from a batch (same signature as sqlite.NewWriteBatchCommand).
func newMockCmd(batch *model.LogBatch) storage.Command {
	recs := 0
	if batch != nil {
		recs = batch.Size()
	}
	return &mockWriteBatchCommand{
		batch:   batch,
		records: recs,
	}
}

type testQueues struct {
	ingress  ingest.IngressQueue
	cmdQueue storage.CommandQueue
}

func newTestQueues(capacity int) *testQueues {
	return &testQueues{
		ingress:  ingest.NewIngressQueue(capacity),
		cmdQueue: storage.NewCommandQueue(capacity),
	}
}

func TestBatcherBatchBySize(t *testing.T) {
	q := newTestQueues(100)
	b := NewBatcher(q.ingress, q.cmdQueue, &BatcherConfig{BatchSize: 5})
	// Use mock command factory to avoid sqlite dependency
	b.WithCommandFactory(newMockCmd)

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

	// Close the command queue to unblock the drain
	q.cmdQueue.Close()

	total := drainCommandQueue(q.cmdQueue)
	if total != 12 {
		t.Errorf("expected 12 records total, got %d", total)
	}
}

func TestBatcherEmptyOnStop(t *testing.T) {
	q := newTestQueues(10)
	b := NewBatcher(q.ingress, q.cmdQueue, &BatcherConfig{BatchSize: 100})
	b.WithCommandFactory(newMockCmd)

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

	q.cmdQueue.Close()
	total := drainCommandQueue(q.cmdQueue)
	if total != 3 {
		t.Errorf("expected 3 records flushed on stop, got %d", total)
	}
}

func TestBatcherBackpressure(t *testing.T) {
	q := newTestQueues(2) // small command queue
	b := NewBatcher(q.ingress, q.cmdQueue, &BatcherConfig{BatchSize: 1})
	b.WithCommandFactory(newMockCmd)

	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)

	// Fill the command queue by sending records
	// cmd queue cap is 2, so we can send up to 2 commands before blocking
	for i := 0; i < 5; i++ {
		if err := q.ingress.Send(ctx, &model.LogRecord{Body: "msg"}); err != nil {
			t.Fatalf("Send error at %d: %v", i, err)
		}
	}

	// Allow batcher to fill the command queue
	time.Sleep(50 * time.Millisecond)

	// Drain the command queue on a goroutine
	var drained atomic.Int32
	go func() {
		for {
			_, err := q.cmdQueue.Receive(context.Background())
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
	q.cmdQueue.Close()
}

func TestBatcherFlush(t *testing.T) {
	q := newTestQueues(10)
	b := NewBatcher(q.ingress, q.cmdQueue, &BatcherConfig{BatchSize: 100})
	b.WithCommandFactory(newMockCmd)

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

	// Check that a command was sent to the command queue
	cmd, err := q.cmdQueue.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive error after Flush(): %v", err)
	}

	// The command should contain the 7 records
	if sized, ok := cmd.(interface{ Size() int }); ok {
		if sized.Size() != 7 {
			t.Errorf("expected command size 7, got %d", sized.Size())
		}
	} else {
		t.Errorf("expected Size() method, got %T", cmd)
	}

	cancel()
	b.Wait()
	q.cmdQueue.Close()
}

func TestBatcherCommandFactory(t *testing.T) {
	// Verify that WithCommandFactory chains correctly
	q := newTestQueues(10)
	b := NewBatcher(q.ingress, q.cmdQueue, &BatcherConfig{BatchSize: 10})
	b.WithCommandFactory(newMockCmd)

	if b.newCmd == nil {
		t.Fatal("newCmd should be set after WithCommandFactory")
	}

	// Verify the factory produces commands
	batch := model.NewLogBatch(1)
	batch.AddRecord(&model.LogRecord{Body: "test"})
	cmd := b.newCmd(batch)
	if cmd == nil {
		t.Fatal("newCmd returned nil")
	}
}
