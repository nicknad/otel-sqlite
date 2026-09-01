package sqlite

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"

	_ "github.com/mattn/go-sqlite3"
)

// openMaintenanceTestDB opens a migrated test database.
func openMaintenanceTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", t.TempDir()+"/maintenance.db")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := RunMigrations(db); err != nil {
		_ = db.Close()
		t.Fatalf("migrate: %v", err)
	}
	return db
}

// TestCheckpointCommand_ExecuteRejectedInTransaction guards the
// NonTransactionalCommand contract: wal_checkpoint cannot run inside a
// transaction, so Execute must fail while ExecuteNonTransactional works.
func TestCheckpointCommand_ExecuteRejectedInTransaction(t *testing.T) {
	db := openMaintenanceTestDB(t)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := NewCheckpointCommand(CheckpointPassive).Execute(context.Background(), tx); err == nil {
		t.Error("Execute inside a transaction should fail (VACUUM-style guard)")
	}

	if err := NewCheckpointCommand("").ExecuteNonTransactional(context.Background(), db); err != nil {
		t.Errorf("ExecuteNonTransactional: %v", err)
	}
}

// TestVacuumCommand_ExecuteRejectedInTransaction mirrors the checkpoint
// guard for VACUUM.
func TestVacuumCommand_ExecuteRejectedInTransaction(t *testing.T) {
	db := openMaintenanceTestDB(t)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := NewVacuumCommand().Execute(context.Background(), tx); err == nil {
		t.Error("Execute inside a transaction should fail (VACUUM cannot run in a tx)")
	}

	if err := NewVacuumCommand().ExecuteNonTransactional(context.Background(), db); err != nil {
		t.Errorf("ExecuteNonTransactional: %v", err)
	}
}

// TestOptimizeCommand_ExecuteRunsInsideTransaction covers the transactional
// maintenance command: PRAGMA optimize is safe inside a transaction.
func TestOptimizeCommand_ExecuteRunsInsideTransaction(t *testing.T) {
	db := openMaintenanceTestDB(t)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := NewOptimizeCommand().Execute(context.Background(), tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("Execute: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestWriterDispatchesNonTransactionalCommands drives CheckpointCommand and
// VacuumCommand through a real Writer, exercising the
// executeTransaction NonTransactionalCommand branch (writer.go) rather than
// only the raw SQL strings.
func TestWriterDispatchesNonTransactionalCommands(t *testing.T) {
	dbPath := t.TempDir() + "/dispatch.db"
	q := storage.NewCommandQueue(10)
	w, err := NewWriter(q, &WriterConfig{
		Path: dbPath, BatchSize: 10, FlushInterval: 100 * time.Millisecond, WALMode: true,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	w.Start(context.Background())
	defer func() { w.Stop(); w.Wait() }()

	// A maintenance command alone must take the non-transactional path.
	if err := w.Submit(context.Background(), NewVacuumCommand()); err != nil {
		t.Fatalf("Submit vacuum: %v", err)
	}
	if err := w.Submit(context.Background(), NewCheckpointCommand(CheckpointPassive)); err != nil {
		t.Fatalf("Submit checkpoint: %v", err)
	}
	if err := w.Submit(context.Background(), NewOptimizeCommand()); err != nil {
		t.Fatalf("Submit optimize: %v", err)
	}

	// Give the writer time to execute, then stop and wait (Stop drains).
	deadline := time.Now().Add(3 * time.Second)
	for w.QueueDepth() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("writer did not drain maintenance commands")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestWriterNonTransactionalPathNotBatchedWithWrites verifies that a
// non-transactional command is executed alone (never batched into a
// transaction with write commands): a VacuumCommand followed by a log write
// must not cause VACUUM to run inside a transaction.
func TestWriterNonTransactionalPathNotBatchedWithWrites(t *testing.T) {
	dbPath := t.TempDir() + "/dispatch-mixed.db"
	q := storage.NewCommandQueue(10)
	w, err := NewWriter(q, &WriterConfig{
		Path: dbPath, BatchSize: 10, FlushInterval: 100 * time.Millisecond, WALMode: true,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	w.Start(context.Background())
	defer func() { w.Stop(); w.Wait() }()

	batch := model.NewLogBatch(1)
	batch.AddRecord(&model.LogRecord{Body: "after vacuum"})
	if err := w.Submit(context.Background(), NewVacuumCommand()); err != nil {
		t.Fatalf("Submit vacuum: %v", err)
	}
	if err := w.Submit(context.Background(), NewWriteBatchCommand(batch)); err != nil {
		t.Fatalf("Submit write: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for w.QueueDepth() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("writer did not drain")
		}
		time.Sleep(20 * time.Millisecond)
	}
	w.Stop()
	w.Wait()

	// The log record must have been written (the vacuum did not error).
	var n int
	if err := writerDBQuery(dbPath, "SELECT COUNT(*) FROM log_event", &n); err != nil {
		t.Fatalf("count log_event: %v", err)
	}
	if n != 1 {
		t.Errorf("log_event count = %d, want 1", n)
	}
}
