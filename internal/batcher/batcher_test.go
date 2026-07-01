package batcher

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
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

// TestBatcherResourceInsertion is an integration test verifying that a
// Resource carried on a LogRecord survives the record-based ingress queue
// and is inserted into SQLite with correct service_name and host_name.
func TestBatcherResourceInsertion(t *testing.T) {
	dbpath := fmt.Sprintf("test_batcher_resource_%d.db", time.Now().UnixNano())
	defer os.Remove(dbpath)

	// Create a real SQLite writer.
	cmdQueue := storage.NewCommandQueue(100)
	writer, err := sqlite.NewWriter(cmdQueue, &sqlite.WriterConfig{
		Path:          dbpath,
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		WALMode:       false,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer.Start(ctx)
	defer func() {
		writer.Stop()
		writer.Wait()
	}()

	// Create batcher using the real sqlite command factory.
	ingressQueue := ingest.NewIngressQueue(100)
	batcher := NewBatcher(ingressQueue, cmdQueue, &BatcherConfig{
		BatchSize:     10,
		FlushInterval: 50 * time.Millisecond,
	})
	batcher.WithCommandFactory(func(batch *model.LogBatch) storage.Command {
		return sqlite.NewWriteBatchCommand(batch)
	})
	batcher.Start(ctx)
	defer func() {
		batcher.Stop()
		batcher.Wait()
	}()

	// Build a resource and attach it to each record.
	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("integration-svc"),
		"host.name":    model.NewStringValue("integration-host"),
	})
	resource.ID = "res-integration-test"

	// Send records through the ingress queue (record-based, not batch-based).
	for i := 0; i < 3; i++ {
		record := &model.LogRecord{
			Timestamp:         int64(i * 1000),
			ObservedTimestamp: int64(i*1000 + 500),
			SeverityNumber:    model.SeverityInfo,
			SeverityText:      "INFO",
			Body:              fmt.Sprintf("msg-%d", i),
			ResourceID:        resource.ID,
			Resource:          resource, // carried per record
		}
		if err := ingressQueue.Send(ctx, record); err != nil {
			t.Fatalf("ingress send: %v", err)
		}
	}

	// Wait for pipeline to flush.
	time.Sleep(300 * time.Millisecond)

	// Query SQLite directly via the writer's DB to verify resource insertion.
	var (
		serviceName string
		hostName    string
	)
	// We need access to the writer's db connection. The writer exposes it
	// indirectly — we can open a second read-only connection to the same file.
	db, err := sql.Open("sqlite", dbpath)
	if err != nil {
		t.Fatalf("open read db: %v", err)
	}
	defer db.Close()

	err = db.QueryRow(
		"SELECT service_name, host_name FROM log_resource WHERE id = ?",
		resource.ID,
	).Scan(&serviceName, &hostName)
	if err != nil {
		t.Fatalf("query log_resource: %v", err)
	}

	if serviceName != "integration-svc" {
		t.Errorf("service_name = %q, want %q", serviceName, "integration-svc")
	}
	if hostName != "integration-host" {
		t.Errorf("host_name = %q, want %q", hostName, "integration-host")
	}

	// Also verify log events reference the resource.
	var eventCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 3 {
		t.Errorf("expected 3 events, got %d", eventCount)
	}

	var eventResourceID string
	if err := db.QueryRow(
		"SELECT resource_id FROM log_event LIMIT 1",
	).Scan(&eventResourceID); err != nil {
		t.Fatalf("query event resource_id: %v", err)
	}
	if eventResourceID != resource.ID {
		t.Errorf("event resource_id = %q, want %q", eventResourceID, resource.ID)
	}
}
