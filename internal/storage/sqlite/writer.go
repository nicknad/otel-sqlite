// Package sqlite provides SQLite storage for log records.
// This package implements a single dedicated writer goroutine that handles
// all SQLite writes via a generic command execution architecture.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/metrics"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"

	_ "modernc.org/sqlite"
)

// preparedStatementsSQL is the list of SQL statements prepared at Writer
// startup.  They are kept in the same order as the PreparedStatements fields
// so initPreparedStatements can prepare them in a loop.
var preparedStatementsSQL = []string{
	sqlInsertResource,
	sqlInsertEvent,
	sqlInsertAttr,
}

// WriterConfig holds configuration for the SQLite writer.
type WriterConfig struct {
	// Database path
	Path string

	// Maximum number of commands to accumulate before flushing a transaction
	BatchSize int

	// Flush interval for automatic flushing of partial batches
	FlushInterval time.Duration

	// WAL mode settings
	WALMode bool

	// Metrics is optional. When non-nil the writer reports metrics.
	Metrics *metrics.Metrics
}

// Writer handles executing Command objects against SQLite.
// Only one goroutine should submit commands; the writer owns the single
// consumer goroutine that processes the command queue.
//
// The writer owns:
//   - SQLite connection
//   - Transaction lifecycle (begin, commit, rollback)
//   - Command execution
//   - Retries (transaction-level)
//   - Metrics and logging
//
// The writer does NOT own command construction—that is the caller's
// responsibility (e.g. the Batcher builds WriteBatchCommand objects).
type Writer struct {
	config WriterConfig
	db     *sql.DB

	// Pre-prepared SQL statements, reused across transactions via tx.Stmt().
	preparedStmts *PreparedStatements

	// Command queue for receiving commands
	cmdQueue storage.CommandQueue

	// Control
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped chan struct{}

	// Local counters (kept for diagnostics; metrics are reported through
	// aMetrics when configured).
	totalRecordsWritten int64
	batchesWritten      int64
	aMetrics            *metrics.Metrics
	lastWriteTime       time.Time
	writeLatency        time.Duration
}

// NewWriter creates a new SQLite Writer.
//
// The writer consumes Command objects from cmdQueue and executes them
// inside SQLite transactions.
func NewWriter(cmdQueue storage.CommandQueue, config *WriterConfig) (*Writer, error) {
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

	// Prepare insert statements once so they are compiled only at startup
	// and reused across every transaction via tx.Stmt().
	preparedStmts, err := initPreparedStatements(db)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to prepare statements: %w", err)
	}

	return &Writer{
		config:        *config,
		db:            db,
		preparedStmts: preparedStmts,
		cmdQueue:      cmdQueue,
		aMetrics:      config.Metrics,
		stopped:       make(chan struct{}),
		lastWriteTime: time.Now(),
	}, nil
}

// Submit enqueues a command for execution.
// Implements storage.CommandExecutor.
func (w *Writer) Submit(ctx context.Context, cmd storage.Command) error {
	return w.cmdQueue.Send(ctx, cmd)
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

	flushTicker := time.NewTicker(w.config.FlushInterval)
	defer flushTicker.Stop()

	cmdCollection := make([]storage.Command, 0, w.config.BatchSize)

	for {
		select {
		case <-w.ctx.Done():
			w.executeCommands(cmdCollection)
			return

		case <-flushTicker.C:
			if len(cmdCollection) > 0 {
				w.executeCommands(cmdCollection)
				cmdCollection = cmdCollection[:0]
			}

		case cmd, ok := <-w.cmdQueue.Chan():
			if !ok {
				w.executeCommands(cmdCollection)
				return
			}
			cmdCollection = append(cmdCollection, cmd)
			if len(cmdCollection) >= w.config.BatchSize {
				w.executeCommands(cmdCollection)
				cmdCollection = cmdCollection[:0]
			}
		}
	}
}

// MaxTransactionRecords caps the number of log records written in a single
// SQLite transaction. Above this threshold the writer splits the work into
// multiple smaller transactions to keep each transaction fast and prevent
// the writer goroutine from being blocked for seconds on oversized flushes.
//
// This value can be tuned based on workload characteristics:
// - Higher values (10000-20000) reduce transaction overhead but increase memory usage
// - Lower values (2000-5000) reduce memory usage but increase transaction overhead
var MaxTransactionRecords = 5000

// executeCommands runs a collection of commands, splitting into separate
// transactions if the total estimated record count exceeds MaxTransactionRecords.
func (w *Writer) executeCommands(commands []storage.Command) {
	if len(commands) == 0 {
		return
	}

	// Walk commands and flush in record-capped chunks.
	start := 0
	for start < len(commands) {
		end := start
		acc := 0
		for end < len(commands) {
			// Estimate record count: WriteBatchCommand exposes Size(),
			// other command types default to 1 for splitting purposes.
			recs := commandRecordCount(commands[end])
			if acc+recs > MaxTransactionRecords && acc > 0 {
				break
			}
			acc += recs
			end++
		}
		w.executeTransaction(commands[start:end])
		start = end
	}
}

// commandRecordCount returns the estimated number of log records a command
// represents, used for transaction splitting.
func commandRecordCount(cmd storage.Command) int {
	if wbc, ok := cmd.(*WriteBatchCommand); ok {
		return wbc.Size()
	}
	// Non-write commands (future maintenance ops) are counted as 1 record
	// so they are never batched with other commands in a transaction.
	return 1
}

// executeTransaction runs a slice of commands in a single SQLite transaction.
// If the batch contains exactly one command that implements storage.NonTransactionalCommand,
// it is executed directly on the database connection without a transaction wrapper.
func (w *Writer) executeTransaction(commands []storage.Command) {
	// Non-transactional commands (e.g., VACUUM) run outside a transaction.
	if len(commands) == 1 {
		if ntCmd, ok := commands[0].(storage.NonTransactionalCommand); ok {
			if err := ntCmd.ExecuteNonTransactional(w.ctx, w.db); err != nil {
				log.Printf("non-transactional command %T failed: %v", commands[0], err)
				if w.aMetrics != nil {
					w.aMetrics.IncrementWriteErrors()
				}
			}
			return
		}
	}

	startTime := time.Now()

	if w.aMetrics != nil {
		w.aMetrics.UpdateCommandQueueDepth(w.cmdQueue.Len())
	}

	// Begin transaction
	tx, err := w.db.Begin()
	if err != nil {
		log.Printf("error beginning transaction: %v", err)
		w.aMetrics.IncrementWriteErrors()
		return
	}

	// Inject pre-prepared statements into WriteBatchCommand instances so
	// they use tx.Stmt() instead of re-compiling SQL every transaction.
	for _, cmd := range commands {
		if wbc, ok := cmd.(*WriteBatchCommand); ok {
			wbc.SetPreparedStatements(w.preparedStmts)
		}
	}

	executed := 0
	for _, cmd := range commands {
		if err := cmd.Execute(w.ctx, tx); err != nil {
			log.Printf("command %T failed: %v", cmd, err)
			w.aMetrics.IncrementWriteErrors()
			// Rollback the entire transaction on command failure.
			_ = tx.Rollback()
			return
		}

		executed++

		if w.aMetrics != nil {
			w.aMetrics.IncrementCommandsExecuted()
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Printf("error committing transaction: %v", err)
		w.aMetrics.IncrementWriteErrors()
		return
	}

	// Update metrics for WriteBatchCommand executions.
	var recordsInTransaction int
	for _, cmd := range commands {
		if wbc, ok := cmd.(*WriteBatchCommand); ok {
			recordsInTransaction += wbc.Size()
		}
	}

	w.totalRecordsWritten += int64(recordsInTransaction)
	w.batchesWritten += int64(executed)

	if w.aMetrics != nil {
		w.aMetrics.IncrementLogsWritten(recordsInTransaction)
		w.aMetrics.IncrementBatchesWritten()
		duration := time.Since(startTime).Seconds()
		w.aMetrics.RecordWriteLatency(duration)
		w.aMetrics.RecordCommandExecutionDuration(duration)
	}

	w.lastWriteTime = time.Now()
	w.writeLatency = time.Since(startTime)
}

// cleanup closes database resources.
func (w *Writer) cleanup() {
	if w.preparedStmts != nil {
		w.preparedStmts.Close()
	}
	if w.db != nil {
		_ = w.db.Close()
	}
}

// openDatabase opens a SQLite database with appropriate settings.
func openDatabase(path string, walMode bool) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

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

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// initPreparedStatements prepares the hot-path insert statements once
// at startup.  The returned PreparedStatements is reused across every
// transaction via tx.Stmt() so that SQLite compiles the SQL only once.
func initPreparedStatements(db *sql.DB) (*PreparedStatements, error) {
	stmts := make([]*sql.Stmt, len(preparedStatementsSQL))
	for i, query := range preparedStatementsSQL {
		stmt, err := db.Prepare(query)
		if err != nil {
			// Close any already-prepared statements on failure.
			for j := 0; j < i; j++ {
				stmts[j].Close()
			}
			return nil, fmt.Errorf("prepare statement: %w", err)
		}
		stmts[i] = stmt
	}
	return &PreparedStatements{
		InsertResource: stmts[0],
		InsertEvent:    stmts[1],
		InsertAttr:     stmts[2],
	}, nil
}

// initializeSchema creates the database schema if it doesn't exist.
func initializeSchema(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS log_resource (
		id TEXT PRIMARY KEY,
		service_name TEXT NOT NULL,
		host_name TEXT,
		schema_url TEXT,
		attributes_json TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

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
		attributes_json TEXT,
		FOREIGN KEY (resource_id) REFERENCES log_resource(id)
	);

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

	CREATE INDEX IF NOT EXISTS idx_log_event_timestamp ON log_event(timestamp_ns);
	CREATE INDEX IF NOT EXISTS idx_log_event_resource_id ON log_event(resource_id);
	CREATE INDEX IF NOT EXISTS idx_log_attr_event_id ON log_attr(event_id);
	`

	_, err := db.Exec(schema)
	return err
}
