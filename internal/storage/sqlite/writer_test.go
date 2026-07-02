package sqlite

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

func TestOpenDatabase(t *testing.T) {
	dbpath := fmt.Sprintf("test_open_%d.db", time.Now().UnixNano())
	defer os.Remove(dbpath)
	defer os.Remove(dbpath + "-wal")
	defer os.Remove(dbpath + "-shm")

	db, err := openDatabase(dbpath, true)
	if err != nil {
		t.Fatalf("openDatabase() error: %v", err)
	}
	defer db.Close()

	if err := initializeSchema(db); err != nil {
		t.Fatalf("initializeSchema() error: %v", err)
	}

	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if mode != "wal" && mode != "WAL" {
		t.Errorf("expected WAL mode, got %q", mode)
	}

	var tableName string
	if err := db.QueryRow(
		"SELECT name FROM sqlite_master WHERE type='table' AND name='log_event'").
		Scan(&tableName); err != nil {
		t.Errorf("log_event table not found: %v", err)
	}
}

func TestInitializeSchema(t *testing.T) {
	dbpath := fmt.Sprintf("test_schema_%d.db", time.Now().UnixNano())
	defer os.Remove(dbpath)
	defer os.Remove(dbpath + "-wal")
	defer os.Remove(dbpath + "-shm")

	db, err := openDatabase(dbpath, false)
	if err != nil {
		t.Fatalf("openDatabase() error: %v", err)
	}
	defer db.Close()

	if err := initializeSchema(db); err != nil {
		t.Fatalf("initializeSchema() error: %v", err)
	}
	// Idempotency check
	if err := initializeSchema(db); err != nil {
		t.Fatalf("initializeSchema() second call error: %v", err)
	}
}

func TestWriterWriteAndQuery(t *testing.T) {
	dbpath := fmt.Sprintf("test_write_%d.db", time.Now().UnixNano())
	defer os.Remove(dbpath)

	cmdQueue := storage.NewCommandQueue(10)
	w, err := NewWriter(cmdQueue, &WriterConfig{
		Path:          dbpath,
		BatchSize:     1,
		FlushInterval: 100 * time.Millisecond,
		WALMode:       false,
	})
	if err != nil {
		t.Fatalf("NewWriter() error: %v", err)
	}

	ctx := context.Background()
	w.Start(ctx)
	defer func() {
		w.Stop()
		w.Wait()
	}()

	// Create a resource and log record
	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("test-svc"),
	})

	record := &model.LogRecord{
		Timestamp:         1000,
		ObservedTimestamp: 2000,
		SeverityNumber:    model.SeverityInfo,
		SeverityText:      "INFO",
		Body:              "hello world",
		ScopeName:         "test-scope",
		ScopeVersion:      "1.0.0",
		Attributes: []model.Attribute{
			{Key: "key1", Str: "val1", Kind: model.ValueString},
		},
	}

	batch := model.NewLogBatch(1)
	batch.AddRecord(record)
	batch.Resource = resource

	// Wrap in WriteBatchCommand and send to the command queue
	cmd := NewWriteBatchCommand(batch)
	if err := cmdQueue.Send(ctx, cmd); err != nil {
		t.Fatalf("Send command error: %v", err)
	}

	// Wait for writer to process
	time.Sleep(200 * time.Millisecond)

	// Verify data was written
	var count int
	if err := w.db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&count); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 event, got %d", count)
	}

	// Verify resource was created
	var svcName string
	if err := w.db.QueryRow("SELECT service_name FROM log_resource WHERE id = ?", resource.ID).Scan(&svcName); err != nil {
		t.Fatalf("query resource (id=%q): %v", resource.ID, err)
	}
	if svcName != "test-svc" {
		t.Errorf("service_name = %q, want %q", svcName, "test-svc")
	}

	// Verify attribute
	var attrKey string
	if err := w.db.QueryRow("SELECT key FROM log_attr WHERE event_id = 1").Scan(&attrKey); err != nil {
		t.Fatalf("query attr: %v", err)
	}
	if attrKey != "key1" {
		t.Errorf("attr key = %q, want %q", attrKey, "key1")
	}
}

func TestWriterMultipleBatches(t *testing.T) {
	dbpath := fmt.Sprintf("test_multi_%d.db", time.Now().UnixNano())
	defer os.Remove(dbpath)

	cmdQueue := storage.NewCommandQueue(10)
	w, err := NewWriter(cmdQueue, &WriterConfig{
		Path:          dbpath,
		BatchSize:     5,
		FlushInterval: 50 * time.Millisecond,
		WALMode:       false,
	})
	if err != nil {
		t.Fatalf("NewWriter() error: %v", err)
	}

	ctx := context.Background()
	w.Start(ctx)
	defer func() {
		w.Stop()
		w.Wait()
	}()

	// Create a shared resource for all batches
	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("multi-svc"),
	})

	// Send multiple commands (each wrapping a batch)
	for i := 0; i < 3; i++ {
		batch := model.NewLogBatch(2)
		batch.AddRecord(&model.LogRecord{Timestamp: int64(i * 100), Body: "msg"})
		batch.AddRecord(&model.LogRecord{Timestamp: int64(i*100 + 1), Body: "msg2"})
		batch.Resource = resource

		cmd := NewWriteBatchCommand(batch)
		if err := cmdQueue.Send(ctx, cmd); err != nil {
			t.Fatalf("Send command %d error: %v", i, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	var count int
	if err := w.db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&count); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if count != 6 {
		t.Errorf("expected 6 events, got %d", count)
	}
}

func TestNewWriterNilConfig(t *testing.T) {
	dbpath := fmt.Sprintf("test_nilcfg_%d.db", time.Now().UnixNano())
	defer os.Remove(dbpath)

	cmdQueue := storage.NewCommandQueue(10)
	w, err := NewWriter(cmdQueue, nil)
	if err != nil {
		t.Fatalf("NewWriter(nil) error: %v", err)
	}
	if w.config.BatchSize != 100 {
		t.Errorf("default BatchSize = %d, want 100", w.config.BatchSize)
	}
	if w.config.FlushInterval != 5*time.Second {
		t.Errorf("default FlushInterval = %s, want 5s", w.config.FlushInterval)
	}
	if w.config.WALMode != true {
		t.Errorf("default WALMode = %v, want true", w.config.WALMode)
	}
	w.cleanup()
}

func TestWriteBatchCommandExecute(t *testing.T) {
	dbpath := fmt.Sprintf("test_cmd_exec_%d.db", time.Now().UnixNano())
	defer os.Remove(dbpath)

	db, err := openDatabase(dbpath, false)
	if err != nil {
		t.Fatalf("openDatabase() error: %v", err)
	}
	defer db.Close()
	if err := initializeSchema(db); err != nil {
		t.Fatalf("initializeSchema() error: %v", err)
	}

	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("cmd-svc"),
	})

	record := &model.LogRecord{
		Timestamp:         99,
		ObservedTimestamp: 199,
		SeverityNumber:    model.SeverityWarn,
		SeverityText:      "WARN",
		Body:              "command test",
		Attributes: []model.Attribute{
			{Key: "k", Str: "v", Kind: model.ValueString},
		},
	}

	batch := model.NewLogBatch(1)
	batch.AddRecord(record)
	batch.Resource = resource

	cmd := NewWriteBatchCommand(batch)

	// Execute inside a transaction
	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin tx error: %v", err)
	}

	if err := cmd.Execute(ctx, tx); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit error: %v", err)
	}

	// Verify data
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&count); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 event, got %d", count)
	}
}

func TestWriteBatchCommandSize(t *testing.T) {
	batch := model.NewLogBatch(5)
	batch.AddRecord(&model.LogRecord{Body: "a"})
	batch.AddRecord(&model.LogRecord{Body: "b"})

	cmd := NewWriteBatchCommand(batch)
	if cmd.Size() != 2 {
		t.Errorf("Size() = %d, want 2", cmd.Size())
	}

	// Empty batch
	emptyCmd := NewWriteBatchCommand(model.NewLogBatch(0))
	if emptyCmd.Size() != 0 {
		t.Errorf("empty Size() = %d, want 0", emptyCmd.Size())
	}

	// Nil batch
	nilCmd := NewWriteBatchCommand(nil)
	if nilCmd.Size() != 0 {
		t.Errorf("nil Size() = %d, want 0", nilCmd.Size())
	}
}

func TestWriteBatchCommandImmutability(t *testing.T) {
	// Verify that the command is constructed with its data and is not
	// modified by any external operation.
	batch := model.NewLogBatch(1)
	batch.AddRecord(&model.LogRecord{Body: "immutable"})

	cmd := NewWriteBatchCommand(batch)
	if cmd.Batch() != batch {
		t.Error("Batch() should return the same batch reference")
	}
	// Verify the command caches the record count correctly
	batch.AddRecord(&model.LogRecord{Body: "extra"})
	// Size() should return the original count, not the updated batch count
	if cmd.Size() != 1 {
		t.Errorf("Size() = %d, want 1 (cached at construction)", cmd.Size())
	}
}
