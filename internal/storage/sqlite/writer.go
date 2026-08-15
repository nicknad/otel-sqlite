// Package sqlite provides SQLite storage for log records.
// This package implements a single dedicated writer goroutine that handles
// all SQLite writes via a generic command execution architecture.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/metrics"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// preparedStatementsSQL is the list of SQL statements prepared at Writer
// startup.  They are kept in the same order as the PreparedStatements fields
// so initPreparedStatements can prepare them in a loop.
var preparedStatementsSQL = []string{
	sqlInsertResource,
	sqlInsertEvent,
	sqlInsertScope,
	sqlInsertMetric,
	sqlInsertSeries,
	sqlInsertDataPoint,
}

// WriterConfig holds configuration for the SQLite writer.
type WriterConfig struct {
	// Database path
	Path string

	// Maximum number of commands to accumulate before flushing a transaction.
	// This is a count of Command values, not log records.
	BatchSize int

	// Flush interval for automatic flushing of partial batches
	FlushInterval time.Duration

	// WAL mode settings
	WALMode bool

	// EnforceForeignKeys controls PRAGMA foreign_keys on the writer connection.
	// Default (false) skips per-row parent lookups; the single writer always
	// inserts the resource row before events in the same transaction.
	// Set true to keep SQLite FK enforcement on the writer connection.
	EnforceForeignKeys bool

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

	// Process-local set of resource IDs successfully inserted. Only touched
	// from the writer goroutine. Avoids repeated INSERT OR IGNORE probes.
	seenResources map[string]struct{}

	// Process-local dedup caches for the metric write path (scope, metric
	// definition, series). Only touched from the writer goroutine.
	seenScopes  map[string]struct{}
	seenMetrics map[string]struct{}
	seenSeries  map[string]struct{}

	// Command queue for receiving commands
	cmdQueue storage.CommandQueue

	// Control
	ctx     context.Context
	cancel  context.CancelCauseFunc
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
	db, err := openDatabase(config.Path, config.WALMode, config.EnforceForeignKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Apply all pending database migrations.
	if err := RunMigrations(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
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
		seenResources: make(map[string]struct{}, 64),
		seenScopes:    make(map[string]struct{}, 64),
		seenMetrics:   make(map[string]struct{}, 64),
		seenSeries:    make(map[string]struct{}, 64),
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

// QueueDepth returns the current number of commands waiting in the writer's
// command queue. The metric batcher uses it to report batch-queue depth now
// that the queue is owned by the writer rather than passed to the batcher.
func (w *Writer) QueueDepth() int {
	return w.cmdQueue.Len()
}

// Start starts the writer goroutine.
func (w *Writer) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancelCause(ctx)
	w.wg.Add(1)

	go w.run()
}

// Stop stops the writer and waits for it to finish.
func (w *Writer) Stop() {
	if w.cancel != nil {
		w.cancel(errors.New("writer stopped"))
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
			// Shutdown: drain remaining commands from the channel
			// before executing one last time.
			log.Printf("sqlite writer: shutting down: %v", context.Cause(w.ctx))
			w.drainAndExecute(cmdCollection)
			return

		case <-flushTicker.C:
			if len(cmdCollection) > 0 {
				w.executeCommands(cmdCollection)
				cmdCollection = cmdCollection[:0]
			}

		case cmd, ok := <-w.cmdQueue.Chan():
			if !ok {
				// Queue closed — execute what we have and exit.
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

// drainAndExecute drains any remaining commands from the command queue
// (non-blocking) and executes them. Used during shutdown to avoid losing
// commands that were enqueued before the ctx was canceled.
func (w *Writer) drainAndExecute(cmds []storage.Command) {
	for {
		select {
		case cmd := <-w.cmdQueue.Chan():
			cmds = append(cmds, cmd)
		default:
			w.executeCommands(cmds)
			return
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
		if _, ok := commands[start].(storage.NonTransactionalCommand); ok {
			w.executeTransaction(commands[start : start+1])
			start++
			continue
		}

		end := start
		acc := 0
		for end < len(commands) {
			if _, ok := commands[end].(storage.NonTransactionalCommand); ok {
				break
			}
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

// RecordCounter is implemented by write commands that represent a countable
// number of records (log records or metric data points). The writer uses it
// to split transactions by actual work, not by command count: a single OTLP
// metrics request can carry tens of thousands of data points.
type RecordCounter interface {
	Size() int
}

// commandRecordCount returns the estimated number of records a command
// represents, used for transaction splitting.
func commandRecordCount(cmd storage.Command) int {
	if rc, ok := cmd.(RecordCounter); ok {
		return rc.Size()
	}
	// Non-write commands (future maintenance ops) are counted as 1 record
	// so they are never batched with other commands in a transaction.
	return 1
}

// executeTransaction runs a slice of commands in a single SQLite transaction.
// If the batch contains exactly one command that implements storage.NonTransactionalCommand,
// it is executed directly on the database connection without a transaction wrapper.
func (w *Writer) executeTransaction(commands []storage.Command) {
	// Use context.Background() for SQL operations so that in-flight
	// commands complete even during shutdown. The writer controls its
	// own lifecycle via the run loop; it drains pending commands after
	// the context is canceled.
	dbCtx := context.Background()

	// Non-transactional commands (e.g., VACUUM) run outside a transaction.
	if len(commands) == 1 {
		if ntCmd, ok := commands[0].(storage.NonTransactionalCommand); ok {
			if err := ntCmd.ExecuteNonTransactional(dbCtx, w.db); err != nil {
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

	// Inject pre-prepared statements and the process-local dedup caches
	// into write commands.
	for _, cmd := range commands {
		if wbc, ok := cmd.(*WriteBatchCommand); ok {
			wbc.SetPreparedStatements(w.preparedStmts)
			wbc.SetSeenResources(w.seenResources)
		}
		if wmc, ok := cmd.(*WriteMetricsCommand); ok {
			wmc.SetPreparedStatements(w.preparedStmts)
			wmc.SetSeenCaches(&MetricsSeenCaches{
				Resources: w.seenResources,
				Scopes:    w.seenScopes,
				Metrics:   w.seenMetrics,
				Series:    w.seenSeries,
			})
		}
	}

	executed := 0
	for _, cmd := range commands {
		if err := cmd.Execute(dbCtx, tx); err != nil {
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

	// Dedup caches are updated only after commit. Rows inserted by commands
	// whose transaction rolled back (command failure above, or commit
	// failure) must never be recorded as seen, or later batches would skip
	// the INSERT OR IGNORE and reference rows that do not exist.
	for _, cmd := range commands {
		switch c := cmd.(type) {
		case *WriteBatchCommand:
			c.CommitSeen()
		case *WriteMetricsCommand:
			c.CommitSeen()
		}
	}

	// Update metrics for write-command executions.
	var logRecords, metricPoints int
	for _, cmd := range commands {
		switch c := cmd.(type) {
		case *WriteBatchCommand:
			logRecords += c.Size()
		case *WriteMetricsCommand:
			metricPoints += c.Size()
		}
	}

	w.totalRecordsWritten += int64(logRecords + metricPoints)
	w.batchesWritten += int64(executed)

	if w.aMetrics != nil {
		if logRecords > 0 {
			w.aMetrics.IncrementLogsWritten(logRecords)
		}
		if metricPoints > 0 {
			w.aMetrics.IncrementMetricDataPointsWritten(metricPoints)
		}
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

// openDatabase opens a SQLite database with performance and durability
// pragmas tuned for write-heavy log ingestion workloads.
//
// enforceFK controls PRAGMA foreign_keys. The writer defaults to off because
// the single-writer insert order always writes resources before events; tests
// that assert FK rejection pass enforceFK=true.
func openDatabase(path string, walMode, enforceFK bool) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Single connection is correct for a dedicated writer: WAL-mode SQLite
	// supports one writer concurrently with many readers, but this process
	// owns the sole writer connection.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// --- Durability & data integrity ---

	fkValue := "OFF"
	if enforceFK {
		fkValue = "ON"
	}
	if _, err := db.Exec("PRAGMA foreign_keys = " + fkValue); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to set foreign_keys=%s: %w", fkValue, err)
	}

	// Busy timeout: if another connection holds a SHARED lock (e.g., a
	// long-running read query), wait up to 5s instead of failing immediately
	// with SQLITE_BUSY.  This is a safety net; under normal operation the
	// single-writer architecture prevents contention.
	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	if walMode {
		// WAL mode: writes go to the WAL file, readers see a consistent
		// snapshot.  Crash-safe even with synchronous=NORMAL because the
		// WAL header is checksummed.
		if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
		}

		// synchronous=NORMAL: the WAL is synced at each checkpoint, but
		// individual transactions are not synced.  Crash-safe in WAL mode
		// because the WAL is idempotent on recovery.  Effectively as durable
		// as FULL for WAL databases at significantly higher throughput.
		if _, err := db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
		}

		// wal_autocheckpoint: trigger a checkpoint after every 1000 pages
		// (4 MB with default 4 KB pages, 8 MB with 8 KB pages) written to
		// the WAL.  Prevents the WAL from growing without bound while keeping
		// checkpoint pauses short.
		if _, err := db.Exec("PRAGMA wal_autocheckpoint=1000"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to set autocheckpoint: %w", err)
		}

		// journal_size_limit caps the WAL file at 64 MB.  If the WAL
		// reaches this size before wal_autocheckpoint fires, SQLite will
		// force a checkpoint to reclaim disk space.  This is a safety
		// net for workloads with very large individual transactions.
		if _, err := db.Exec("PRAGMA journal_size_limit = 67108864"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to set journal_size_limit: %w", err)
		}
	}

	// --- Performance tuning ---

	// cache_size: increase the in-memory page cache from the default 2 MB
	// (~500 pages) to 64 MB (~16000 pages at 4 KB).  A larger cache reduces
	// B-tree page reads from disk, especially important for the resource
	// lookup path (INSERT OR IGNORE still reads the index).
	// Negative value means kibibytes: -65536 = 64 MB.
	if _, err := db.Exec("PRAGMA cache_size = -65536"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to set cache_size: %w", err)
	}

	// mmap_size: enable memory-mapped I/O for the database file.  Instead
	// of read() syscalls, SQLite accesses pages directly in the process
	// address space via the kernel's page cache.  This eliminates user/kernel
	// copies and reduces CPU overhead.  256 MB is large enough for most
	// deployments without pressuring virtual address space.
	if _, err := db.Exec("PRAGMA mmap_size = 268435456"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to set mmap_size: %w", err)
	}

	// temp_store = MEMORY: force temporary tables, indices, and sorting
	// buffers into memory instead of spilling to a temporary file on disk.
	// This is safe because temp objects are small and short-lived in this
	// workload (no large analytical queries).
	if _, err := db.Exec("PRAGMA temp_store = MEMORY"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to set temp_store: %w", err)
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
			for j := range i {
				_ = stmts[j].Close()
			}
			return nil, fmt.Errorf("prepare statement: %w", err)
		}
		stmts[i] = stmt
	}
	return &PreparedStatements{
		InsertResource:  stmts[0],
		InsertEvent:     stmts[1],
		InsertScope:     stmts[2],
		InsertMetric:    stmts[3],
		InsertSeries:    stmts[4],
		InsertDataPoint: stmts[5],
	}, nil
}
