// Package sqlite provides SQLite storage for log records.
// This package implements a single dedicated writer goroutine that handles all SQLite writes.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nnadolski/otel-sqlite/internal/ingest"
	"github.com/nnadolski/otel-sqlite/internal/metrics"
	"github.com/nnadolski/otel-sqlite/internal/model"

	_ "modernc.org/sqlite"
)

// batchQueueWithChan is an optional extension for batch queues that expose a channel.
type batchQueueWithChan interface {
	ingest.BatchQueue
	Chan() <-chan *model.LogBatch
}

// WriterConfig holds configuration for the SQLite writer.
type WriterConfig struct {
	// Database path
	Path string

	// Batch size for transactions
	BatchSize int

	// Flush interval for automatic flushing
	FlushInterval time.Duration

	// WAL mode settings
	WALMode bool

	// Metrics is optional. When non-nil the writer reports processed
	// records, batches, write latency and errors.
	Metrics *metrics.Metrics
}

// Writer handles writing log records to SQLite.
// Only one goroutine should call Write() at a time.
type Writer struct {
	config WriterConfig
	db     *sql.DB

	// Batch queue for receiving batches
	batchQueue ingest.BatchQueue

	// Control
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped chan struct{}

	// Local counters (kept for diagnostics/back-compat; metrics are
	// reported through a.metrics when configured).
	totalRecordsWritten int64
	batchesWritten      int64
	aMetrics            *metrics.Metrics
	lastWriteTime       time.Time
	writeLatency        time.Duration
}

// NewWriter creates a new SQLite Writer.
func NewWriter(batchQueue ingest.BatchQueue, config *WriterConfig) (*Writer, error) {
	if config == nil {
		config = &WriterConfig{
			BatchSize:     100,
			FlushInterval: 5 * time.Second,
			WALMode:       true,
		}
	}

	// Open database
	db, err := openDatabase(config.Path, config.WALMode)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Initialize schema
	if err := initializeSchema(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return &Writer{
		config:        *config,
		db:            db,
		batchQueue:    batchQueue,
		aMetrics:      config.Metrics,
		stopped:       make(chan struct{}),
		lastWriteTime: time.Now(),
	}, nil
}

// Start starts the writer goroutine.
func (w *Writer) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)

	go w.run()
}

// Stop stops the writer and waits for it to finish.
func (w *Writer) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	<-w.stopped
}

// Wait waits for the writer to finish.
func (w *Writer) Wait() {
	w.wg.Wait()
}

// run is the main writer loop.
func (w *Writer) run() {
	defer w.wg.Done()
	defer close(w.stopped)
	defer w.cleanup()

	// Create a ticker for flush interval
	flushTicker := time.NewTicker(w.config.FlushInterval)
	defer flushTicker.Stop()

	// Collect batches for transaction batching
	batchCollection := make([]*model.LogBatch, 0, w.config.BatchSize)

	// If the batch queue exposes a channel, use it directly in select for responsive flushing.
	// Otherwise fall back to the original default-based loop.
	if qc, ok := w.batchQueue.(batchQueueWithChan); ok {
		w.runWithChan(qc, flushTicker, batchCollection)
	} else {
		w.runDefault(flushTicker, batchCollection)
	}
}

// runWithChan uses the batch queue's channel directly in the select statement.
func (w *Writer) runWithChan(qc batchQueueWithChan, flushTicker *time.Ticker, batchCollection []*model.LogBatch) {
	for {
		select {
		case <-w.ctx.Done():
			w.writeBatches(batchCollection)
			return

		case <-flushTicker.C:
			if len(batchCollection) > 0 {
				w.writeBatches(batchCollection)
				batchCollection = batchCollection[:0]
			}

		case batch, ok := <-qc.Chan():
			if !ok {
				w.writeBatches(batchCollection)
				return
			}
			batchCollection = append(batchCollection, batch)
			if len(batchCollection) >= w.config.BatchSize {
				w.writeBatches(batchCollection)
				batchCollection = batchCollection[:0]
			}
		}
	}
}

// runDefault uses a default case with a blocking Receive (fallback for non-channel queues).
func (w *Writer) runDefault(flushTicker *time.Ticker, batchCollection []*model.LogBatch) {
	for {
		select {
		case <-w.ctx.Done():
			w.writeBatches(batchCollection)
			return

		case <-flushTicker.C:
			if len(batchCollection) > 0 {
				w.writeBatches(batchCollection)
				batchCollection = batchCollection[:0]
			}

		default:
			batch, err := w.batchQueue.Receive(w.ctx)
			if err != nil {
				w.writeBatches(batchCollection)
				return
			}
			batchCollection = append(batchCollection, batch)
			if len(batchCollection) >= w.config.BatchSize {
				w.writeBatches(batchCollection)
				batchCollection = batchCollection[:0]
			}
		}
	}
}

// maxTransactionRecords caps the number of log records written in a single
// SQLite transaction. Above this threshold the writer splits the work into
// multiple smaller transactions to keep each transaction fast and prevent
// the writer goroutine from being blocked for seconds on oversized flushes.
const maxTransactionRecords = 5000

// writeBatches writes a collection of batches, splitting into separate
// transactions if the total record count exceeds maxTransactionRecords.
func (w *Writer) writeBatches(batches []*model.LogBatch) {
	if len(batches) == 0 {
		return
	}

	// Walk the batches and flush in record-capped chunks so that a
	// single transaction never writes more than maxTransactionRecords.
	// This keeps each transaction fast and responsive.
	start := 0
	for start < len(batches) {
		// Determine the end of this chunk.
		end := start
		acc := 0
		for end < len(batches) {
			if acc+batches[end].Size() > maxTransactionRecords && acc > 0 {
				// Adding this batch would exceed the cap, and we
				// already have at least one batch accumulated.
				break
			}
			acc += batches[end].Size()
			end++
		}
		w.writeTransaction(batches[start:end], acc)
		start = end
	}
}

// writeTransaction writes a slice of batches in a single SQLite transaction.
// The caller guarantees that batches contain at most maxTransactionRecords records.
func (w *Writer) writeTransaction(batches []*model.LogBatch, totalRecords int) {
	startTime := time.Now()

	// Begin transaction
	tx, err := w.db.Begin()
	if err != nil {
		w.aMetrics.IncrementWriteErrors()
		return
	}
	rollback := func() {
		_ = tx.Rollback()
		w.aMetrics.IncrementWriteErrors()
	}

	// Prepare transaction-specific statements
	txInsertResource, err := tx.Prepare(
		`INSERT OR IGNORE INTO log_resource
		(id, service_name, host_name, schema_url, attributes_json)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		rollback()
		return
	}
	defer func() { _ = txInsertResource.Close() }()

	txInsertEvent, err := tx.Prepare(
		`INSERT INTO log_event
		(id, resource_id, timestamp_ns, observed_timestamp_ns,
		severity_number, severity_text, trace_id, span_id,
		body, event_name, flags, dropped_attributes_count,
		scope_name, scope_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		rollback()
		return
	}
	defer func() { _ = txInsertEvent.Close() }()

	txInsertAttr, err := tx.Prepare(
		`INSERT INTO log_attr
		(event_id, key, value_type, string_value, int_value, double_value, bool_value, bytes_value)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		rollback()
		return
	}
	defer func() { _ = txInsertAttr.Close() }()

	written := 0

	for _, batch := range batches {
		// Process resource
		if batch.Resource != nil {
			resourceID := w.ensureResource(tx, txInsertResource, batch.Resource)

			// Update all records in batch with resource ID
			for _, record := range batch.Records {
				record.ResourceID = resourceID
			}
		}

		// Process each record in the batch
		for _, record := range batch.Records {
			// Insert event
			eventID, err := w.insertEvent(tx, txInsertEvent, record)
			if err != nil {
				log.Printf("error inserting event: %v", err)
				continue
			}

			// Insert attributes
			if err := w.insertAttributes(tx, txInsertAttr, eventID, record.Attributes); err != nil {
				log.Printf("error inserting attributes for event %d: %v", eventID, err)
				continue
			}

			written++
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Printf("error committing transaction: %v", err)
		w.aMetrics.IncrementWriteErrors()
		return
	}

	w.totalRecordsWritten += int64(written)
	w.batchesWritten += int64(len(batches))

	if w.aMetrics != nil {
		w.aMetrics.IncrementLogsWritten(written)
		w.aMetrics.IncrementBatchesWritten()
		w.aMetrics.RecordWriteLatency(time.Since(startTime).Seconds())
	}

	w.lastWriteTime = time.Now()
	w.writeLatency = time.Since(startTime)
}

// ensureResource ensures a resource exists in the database and returns its ID.
// Uses INSERT OR IGNORE so it's safe to call multiple times with the same ID.
// Sets resource.ID to the generated or existing ID.
func (w *Writer) ensureResource(tx *sql.Tx, insertStmt *sql.Stmt, resource *model.Resource) string {
	resourceID := resource.ID
	if resourceID == "" {
		// Generate a unique ID for this resource
		resourceID = fmt.Sprintf("res-%d", time.Now().UnixNano())
		resource.ID = resourceID
	}

	// Extract service and host names
	serviceName := resource.GetServiceName()
	hostName := resource.GetHostName()

	// Insert resource (INSERT OR IGNORE handles duplicates safely)
	_, err := insertStmt.Exec(
		resourceID,
		serviceName,
		hostName,
		resource.SchemaURL,
		"{}",
	)
	if err != nil {
		log.Printf("error inserting resource %q: %v", resourceID, err)
	}

	return resourceID
}

// insertEvent inserts a log event into the database.
func (w *Writer) insertEvent(tx *sql.Tx, stmt *sql.Stmt, record *model.LogRecord) (int64, error) {
	result, err := stmt.Exec(
		nil, // ID will be auto-generated
		record.ResourceID,
		record.Timestamp,
		record.ObservedTimestamp,
		int64(record.SeverityNumber),
		record.SeverityText,
		record.TraceID,
		record.SpanID,
		record.Body,
		record.EventName,
		uint64(record.Flags),
		uint64(record.DroppedAttributesCount),
		record.ScopeName,
		record.ScopeVersion,
	)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// insertAttributes inserts attributes for a log event.
func (w *Writer) insertAttributes(tx *sql.Tx, stmt *sql.Stmt, eventID int64, attributes map[string]model.AttributeValue) error { //nolint:lll
	if len(attributes) == 0 {
		return nil
	}

	for key, value := range attributes {
		_, err := stmt.Exec(
			eventID,
			key,
			value.Type(),
			getStringValue(&value),
			getIntValue(&value),
			getDoubleValue(&value),
			getBoolValue(&value),
			getBytesValue(&value),
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// Helper functions for extracting values from AttributeValue
func getStringValue(v *model.AttributeValue) *string {
	if v.StringValue != nil {
		return v.StringValue
	}
	return nil
}

func getIntValue(v *model.AttributeValue) *int64 {
	if v.IntValue != nil {
		return v.IntValue
	}
	return nil
}

func getDoubleValue(v *model.AttributeValue) *float64 {
	if v.DoubleValue != nil {
		return v.DoubleValue
	}
	return nil
}

func getBoolValue(v *model.AttributeValue) *bool {
	if v.BoolValue != nil {
		return v.BoolValue
	}
	return nil
}

func getBytesValue(v *model.AttributeValue) []byte {
	if v.BytesValue != nil {
		return v.BytesValue
	}
	return nil
}

// cleanup closes database resources.
func (w *Writer) cleanup() {
	if w.db != nil {
		_ = w.db.Close()
	}
}

// openDatabase opens a SQLite database with appropriate settings.
func openDatabase(path string, walMode bool) (*sql.DB, error) {
	// Open database using modernc.org/sqlite driver (registered via blank import)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Enable WAL mode if requested
	if walMode {
		if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
		}

		if _, err := db.Exec("PRAGMA synchronous=NORMAL;"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
		}

		if _, err := db.Exec("PRAGMA wal_autocheckpoint=1000;"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to set autocheckpoint: %w", err)
		}
	}

	// Verify connection
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// initializeSchema creates the database schema if it doesn't exist.
func initializeSchema(db *sql.DB) error {
	schema := `
	-- Log resources table
	CREATE TABLE IF NOT EXISTS log_resource (
		id TEXT PRIMARY KEY,
		service_name TEXT NOT NULL,
		host_name TEXT,
		schema_url TEXT,
		attributes_json TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	
	-- Log events table
	CREATE TABLE IF NOT EXISTS log_event (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		resource_id TEXT NOT NULL,
		timestamp_ns INTEGER NOT NULL,
		observed_timestamp_ns INTEGER NOT NULL,
		severity_number INTEGER NOT NULL,
		severity_text TEXT,
		trace_id BLOB,
		span_id BLOB,
		body TEXT,
		event_name TEXT,
		flags INTEGER NOT NULL,
		dropped_attributes_count INTEGER NOT NULL,
		scope_name TEXT,
		scope_version TEXT,
		FOREIGN KEY (resource_id) REFERENCES log_resource(id)
	);
	
	-- Log attributes table
	CREATE TABLE IF NOT EXISTS log_attr (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		event_id INTEGER NOT NULL,
		key TEXT NOT NULL,
		value_type TEXT NOT NULL,
		string_value TEXT,
		int_value INTEGER,
		double_value REAL,
		bool_value INTEGER,
		bytes_value BLOB,
		FOREIGN KEY (event_id) REFERENCES log_event(id) ON DELETE CASCADE
	);
	
	-- Indexes for query performance
	CREATE INDEX IF NOT EXISTS idx_log_event_timestamp ON log_event(timestamp_ns);
	CREATE INDEX IF NOT EXISTS idx_log_event_severity ON log_event(severity_number);
	CREATE INDEX IF NOT EXISTS idx_log_event_trace_id ON log_event(trace_id);
	CREATE INDEX IF NOT EXISTS idx_log_event_resource_id ON log_event(resource_id);
	CREATE INDEX IF NOT EXISTS idx_log_attr_event_id ON log_attr(event_id);
	CREATE INDEX IF NOT EXISTS idx_log_attr_key ON log_attr(key);
	CREATE INDEX IF NOT EXISTS idx_log_event_severity_text ON log_event(severity_text);
	CREATE INDEX IF NOT EXISTS idx_log_event_body ON log_event(body);
	
	-- Composite indexes for common query patterns
	CREATE INDEX IF NOT EXISTS idx_log_event_resource_timestamp ON log_event(resource_id, timestamp_ns);
	CREATE INDEX IF NOT EXISTS idx_log_event_trace_timestamp ON log_event(trace_id, timestamp_ns);
	`

	// Execute schema creation
	_, err := db.Exec(schema)
	return err
}
