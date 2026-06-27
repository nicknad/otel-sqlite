package sqlite

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nnadolski/otel-sqlite/internal/ingest"
	"github.com/nnadolski/otel-sqlite/internal/model"
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

	// Initialize schema (normally done by NewWriter)
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
	if err := db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='log_event'").Scan(&tableName); err != nil { //nolint:lll
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

	batchQueue := ingest.NewBatchQueue(10)
	w, err := NewWriter(batchQueue, &WriterConfig{
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

	// Create a resource and log record (no explicit ID -> writer auto-generates)
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
		Attributes: map[string]model.AttributeValue{
			"key1": model.NewStringValue("val1"),
		},
	}

	batch := model.NewLogBatch(1)
	batch.AddRecord(record)
	batch.Resource = resource

	if err := batchQueue.Send(ctx, batch); err != nil {
		t.Fatalf("Send batch error: %v", err)
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

	batchQueue := ingest.NewBatchQueue(10)
	w, err := NewWriter(batchQueue, &WriterConfig{
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

	// Send multiple batches
	for i := 0; i < 3; i++ {
		batch := model.NewLogBatch(2)
		batch.AddRecord(&model.LogRecord{Timestamp: int64(i * 100), Body: "msg"})
		batch.AddRecord(&model.LogRecord{Timestamp: int64(i*100 + 1), Body: "msg2"})
		batch.Resource = resource
		if err := batchQueue.Send(ctx, batch); err != nil {
			t.Fatalf("Send batch %d error: %v", i, err)
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

	batchQueue := ingest.NewBatchQueue(10)
	w, err := NewWriter(batchQueue, nil)
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

func TestGetHelperFunctions(t *testing.T) {
	s := "hello"
	v := model.NewStringValue(s)
	if got := getStringValue(&v); got == nil || *got != s {
		t.Errorf("getStringValue() = %v, want %q", got, s)
	}
	var empty model.AttributeValue
	if got := getStringValue(&empty); got != nil {
		t.Errorf("getStringValue(empty) = %v, want nil", got)
	}

	i := int64(42)
	vi := model.NewIntValue(i)
	if got := getIntValue(&vi); got == nil || *got != i {
		t.Errorf("getIntValue() = %v, want %d", got, i)
	}
	if got := getIntValue(&empty); got != nil {
		t.Errorf("getIntValue(empty) = %v, want nil", got)
	}

	f := 3.14
	vf := model.NewDoubleValue(f)
	if got := getDoubleValue(&vf); got == nil || *got != f {
		t.Errorf("getDoubleValue() = %v, want %f", got, f)
	}

	b := true
	vb := model.NewBoolValue(b)
	if got := getBoolValue(&vb); got == nil || *got != b {
		t.Errorf("getBoolValue() = %v, want %v", got, b)
	}

	bytes := []byte{1, 2, 3}
	vBytes := model.NewBytesValue(bytes)
	if got := getBytesValue(&vBytes); got == nil || len(got) != 3 {
		t.Errorf("getBytesValue() = %v, want %v", got, bytes)
	}
	if got := getBytesValue(&empty); got != nil {
		t.Errorf("getBytesValue(empty) = %v, want nil", got)
	}
}
