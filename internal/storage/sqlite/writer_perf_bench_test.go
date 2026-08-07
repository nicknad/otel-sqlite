package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"

	_ "github.com/mattn/go-sqlite3"
)

// ---------------------------------------------------------------------------
// Benchmark: full writer pipeline with realistic batch sizes and attributes.
// Compares baseline (just WAL+synchronous) vs optimized pragmas.
// ---------------------------------------------------------------------------

const (
	benchBatchSize = 250 // production default
	benchAttrs     = 5   // typical attribute count per record
)

// openWithPragmas opens a SQLite database with a specific set of pragmas
// so we can compare baseline vs optimized.
func openWithPragmas(path, label string, optimized bool) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	pragmas := []struct{ k, v string }{
		{"foreign_keys", "ON"},
		{"journal_mode", "WAL"},
		{"synchronous", "NORMAL"},
		{"wal_autocheckpoint", "1000"},
	}

	if optimized {
		pragmas = append(pragmas,
			struct{ k, v string }{"busy_timeout", "5000"},
			struct{ k, v string }{"cache_size", "-65536"},   // 64 MB
			struct{ k, v string }{"mmap_size", "268435456"}, // 256 MB
			struct{ k, v string }{"temp_store", "MEMORY"},
			struct{ k, v string }{"journal_size_limit", "67108864"}, // 64 MB
		)
	}

	for _, p := range pragmas {
		if _, err := db.Exec(fmt.Sprintf("PRAGMA %s = %s", p.k, p.v)); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %s: %w", label, p.k, err)
		}
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func benchWriterThroughput(b *testing.B, optimized bool) {
	b.Helper()
	b.StopTimer()

	dbPath := fmt.Sprintf("bench_writer_%s_%d.db", map[bool]string{true: "opt", false: "base"}[optimized], time.Now().UnixNano())
	db, err := openWithPragmas(dbPath, "", optimized)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-wal")
	defer os.Remove(dbPath + "-shm")

	if err := RunMigrations(db); err != nil {
		b.Fatal(err)
	}

	// Prepare statements on this DB directly (not via Writer).
	preparedStmts, err := initPreparedStatements(db)
	if err != nil {
		b.Fatal(err)
	}
	defer preparedStmts.Close()

	ctx := context.Background()

	// Pre-create resource.
	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("bench-svc"),
	})

	b.StartTimer()

	totalRecords := 0
	for i := 0; i < b.N; i++ {
		// Create fresh records each iteration — the command's Execute
		// calls PutRecord which returns them to the pool, corrupting
		// reuse across iterations.
		freshRecs := make([]*model.LogRecord, benchBatchSize)
		for j := range freshRecs {
			rec := model.GetRecord()
			rec.Timestamp = time.Now().UnixNano()
			rec.ObservedTimestamp = rec.Timestamp
			rec.SeverityNumber = model.SeverityInfo
			rec.SeverityText = "INFO"
			rec.Body = fmt.Sprintf("bench log message %d", j)
			rec.ScopeName = "bench"
			for k := 0; k < benchAttrs; k++ {
				rec.Attributes = append(rec.Attributes, model.Attribute{
					Key:  fmt.Sprintf("attr_%d", k),
					Str:  fmt.Sprintf("val_%d_%d", k, j),
					Kind: model.ValueString,
				})
			}
			freshRecs[j] = rec
		}

		batch := model.NewLogBatch(benchBatchSize)
		batch.Resource = resource
		for _, rec := range freshRecs {
			batch.AddRecord(rec)
		}

		cmd := NewWriteBatchCommand(batch)
		cmd.SetPreparedStatements(preparedStmts)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		if err := cmd.Execute(ctx, tx); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
		totalRecords += benchBatchSize
	}

	b.StopTimer()

	// Verify.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&count); err != nil {
		b.Fatal(err)
	}
	if count != totalRecords {
		b.Fatalf("expected %d events, got %d", totalRecords, count)
	}

	b.ReportMetric(float64(totalRecords)/b.Elapsed().Seconds(), "records/s")
}

// BenchmarkWriter_BaselinePragmas measures throughput with only the
// essential pragmas (WAL, synchronous=NORMAL, wal_autocheckpoint).
func BenchmarkWriter_BaselinePragmas(b *testing.B) {
	benchWriterThroughput(b, false)
}

// BenchmarkWriter_OptimizedPragmas measures throughput with the full set
// of performance pragmas: cache_size=64MB, mmap_size=256MB, temp_store=MEMORY,
// journal_size_limit=64MB, busy_timeout=5000.
func BenchmarkWriter_OptimizedPragmas(b *testing.B) {
	benchWriterThroughput(b, true)
}

// ---------------------------------------------------------------------------
// Benchmark: incremental writes on a pre-filled (large) database.
// This is where pragmas like cache_size and mmap_size matter — when
// B-tree depth is real and page cache hit rate determines throughput.
// ---------------------------------------------------------------------------

const prefillRecords = 500_000 // half a million rows to create real B-tree depth

func benchIncrementalOnLargeDB(b *testing.B, optimized bool) {
	b.Helper()
	b.StopTimer()

	dbPath := fmt.Sprintf("bench_large_%s_%d.db", map[bool]string{true: "opt", false: "base"}[optimized], time.Now().UnixNano())
	db, err := openWithPragmas(dbPath, "", optimized)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-wal")
	defer os.Remove(dbPath + "-shm")

	if err := RunMigrations(db); err != nil {
		b.Fatal(err)
	}

	preparedStmts, err := initPreparedStatements(db)
	if err != nil {
		b.Fatal(err)
	}
	defer preparedStmts.Close()

	ctx := context.Background()
	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("fill-svc"),
	})

	// --- Pre-fill phase: write 500K records to create B-tree depth ---
	b.Logf("pre-filling %d records...", prefillRecords)
	fillStart := time.Now()
	const fillBatchSize = 500 // bigger batches for fill speed
	filled := 0
	for filled < prefillRecords {
		n := fillBatchSize
		if filled+n > prefillRecords {
			n = prefillRecords - filled
		}
		batch := model.NewLogBatch(n)
		batch.Resource = resource
		for j := 0; j < n; j++ {
			rec := model.GetRecord()
			rec.Timestamp = time.Now().UnixNano()
			rec.ObservedTimestamp = rec.Timestamp
			rec.SeverityNumber = model.SeverityInfo
			rec.Body = fmt.Sprintf("fill msg %d", filled+j)
			for k := 0; k < 3; k++ {
				rec.Attributes = append(rec.Attributes, model.Attribute{
					Key:  fmt.Sprintf("k%d", k),
					Str:  fmt.Sprintf("v%d", k),
					Kind: model.ValueString,
				})
			}
			batch.AddRecord(rec)
		}

		cmd := NewWriteBatchCommand(batch)
		cmd.SetPreparedStatements(preparedStmts)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		if err := cmd.Execute(ctx, tx); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
		filled += n
	}
	fillDuration := time.Since(fillStart)
	b.Logf("pre-fill complete: %d records in %v (%.0f rec/s)",
		prefillRecords, fillDuration.Round(time.Millisecond),
		float64(prefillRecords)/fillDuration.Seconds())

	var rowCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&rowCount); err != nil {
		b.Fatal(err)
	}
	b.Logf("database has %d rows after pre-fill", rowCount)

	// Force a checkpoint to flush WAL to main DB and then TRUNCATE
	// the WAL so that every subsequent write requires B-tree page
	// allocations and index updates — this is where cache_size and
	// mmap_size show their value.
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		b.Fatal(err)
	}

	// --- Benchmark phase: measure incremental write throughput ---
	// Force checkpoint after each commit so the B-tree, not the WAL,
	// is the bottleneck.
	b.StartTimer()

	totalRecords := 0
	for i := 0; i < b.N; i++ {
		batch := model.NewLogBatch(benchBatchSize)
		batch.Resource = resource
		for j := 0; j < benchBatchSize; j++ {
			rec := model.GetRecord()
			rec.Timestamp = time.Now().UnixNano()
			rec.ObservedTimestamp = rec.Timestamp
			rec.SeverityNumber = model.SeverityInfo
			rec.SeverityText = "INFO"
			rec.Body = fmt.Sprintf("incr msg %d", j)
			for k := 0; k < benchAttrs; k++ {
				rec.Attributes = append(rec.Attributes, model.Attribute{
					Key:  fmt.Sprintf("attr_%d", k),
					Str:  fmt.Sprintf("val_%d_%d", k, j),
					Kind: model.ValueString,
				})
			}
			batch.AddRecord(rec)
		}

		cmd := NewWriteBatchCommand(batch)
		cmd.SetPreparedStatements(preparedStmts)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		if err := cmd.Execute(ctx, tx); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
		// Flush WAL to B-tree after each transaction so the benchmark
		// measures B-tree update cost, not WAL append speed.
		if _, err := db.Exec("PRAGMA wal_checkpoint(PASSIVE)"); err != nil {
			b.Fatal(err)
		}
		totalRecords += benchBatchSize
	}

	b.StopTimer()

	var finalCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&finalCount); err != nil {
		b.Fatal(err)
	}
	b.Logf("final count: %d (prefill %d + benchmark %d)", finalCount, prefillRecords, totalRecords)

	b.ReportMetric(float64(totalRecords)/b.Elapsed().Seconds(), "records/s")
}

// BenchmarkWriter_LargeDB_Baseline measures incremental writes on a
// pre-filled database with only essential pragmas.
func BenchmarkWriter_LargeDB_Baseline(b *testing.B) {
	benchIncrementalOnLargeDB(b, false)
}

// BenchmarkWriter_LargeDB_Optimized measures incremental writes on a
// pre-filled database with full performance pragmas.
func BenchmarkWriter_LargeDB_Optimized(b *testing.B) {
	benchIncrementalOnLargeDB(b, true)
}

// ---------------------------------------------------------------------------
// Benchmark: end-to-end writer pipeline with channel + goroutine overhead.
// ---------------------------------------------------------------------------

func BenchmarkWriter_E2E_Pipeline(b *testing.B) {
	b.StopTimer()

	dbPath := fmt.Sprintf("bench_e2e_%d.db", time.Now().UnixNano())
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-wal")
	defer os.Remove(dbPath + "-shm")

	cmdQueue := storage.NewCommandQueue(benchBatchSize * 2)
	w, err := NewWriter(cmdQueue, &WriterConfig{
		Path:          dbPath,
		BatchSize:     benchBatchSize,
		FlushInterval: 100 * time.Millisecond,
		WALMode:       true,
	})
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	defer func() { w.Stop(); w.Wait() }()

	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("e2e-svc"),
	})

	b.StartTimer()

	totalRecords := 0
	for i := 0; i < b.N; i++ {
		batch := model.NewLogBatch(benchBatchSize)
		batch.Resource = resource
		for j := 0; j < benchBatchSize; j++ {
			rec := model.GetRecord()
			rec.Timestamp = time.Now().UnixNano()
			rec.ObservedTimestamp = rec.Timestamp
			rec.SeverityNumber = model.SeverityInfo
			rec.SeverityText = "INFO"
			rec.Body = fmt.Sprintf("e2e msg %d", j)
			for k := 0; k < benchAttrs; k++ {
				rec.Attributes = append(rec.Attributes, model.Attribute{
					Key:  fmt.Sprintf("a_%d", k),
					Str:  fmt.Sprintf("v_%d_%d", k, j),
					Kind: model.ValueString,
				})
			}
			batch.AddRecord(rec)
		}

		cmd := NewWriteBatchCommand(batch)
		if err := cmdQueue.Send(ctx, cmd); err != nil {
			b.Fatal(err)
		}
		totalRecords += benchBatchSize
	}

	// Wait for queue to drain.
	for cmdQueue.Len() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // give writer one last tick

	b.StopTimer()

	var count int
	if err := w.db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&count); err != nil {
		b.Fatal(err)
	}

	b.ReportMetric(float64(totalRecords)/b.Elapsed().Seconds(), "records/s")
	b.ReportMetric(float64(count), "total_stored")
}
