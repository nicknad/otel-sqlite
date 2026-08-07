package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// TestAllSQLStatements validates the current SQLite schema, prepared insert
// statements, purge SQL, PRAGMAs, and maintenance SQL.
func TestAllSQLStatements(t *testing.T) {
	dbpath := fmt.Sprintf("test_sql_syntax_%d.db", time.Now().UnixNano())
	defer os.Remove(dbpath)
	defer os.Remove(dbpath + "-wal")
	defer os.Remove(dbpath + "-shm")

	db, err := openDatabase(dbpath, false)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	defer db.Close()
	if err := RunMigrations(db); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	ctx := context.Background()
	t.Run("DDL_tables_and_indexes", func(t *testing.T) {
		for _, table := range []string{"log_resource", "log_event"} {
			var name string
			if err := db.QueryRow(
				"SELECT name FROM sqlite_master WHERE type='table' AND name=?", table,
			).Scan(&name); err != nil {
				t.Errorf("table %q missing: %v", table, err)
			}
		}
		var legacyCount int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='log_attr'",
		).Scan(&legacyCount); err != nil {
			t.Fatal(err)
		}
		if legacyCount != 0 {
			t.Error("log_attr must not exist after migration 005")
		}

		for _, index := range []string{
			"idx_log_event_timestamp",
			"idx_log_event_resource_id",
			"idx_log_resource_service_name",
		} {
			var name string
			if err := db.QueryRow(
				"SELECT name FROM sqlite_master WHERE type='index' AND name=?", index,
			).Scan(&name); err != nil {
				t.Errorf("index %q missing: %v", index, err)
			}
		}
	})

	t.Run("PRAGMAs", func(t *testing.T) {
		for _, pragma := range []string{
			"PRAGMA synchronous=NORMAL",
			"PRAGMA wal_autocheckpoint=1000",
			"PRAGMA optimize",
		} {
			if _, err := db.ExecContext(ctx, pragma); err != nil {
				t.Errorf("PRAGMA %q failed: %v", pragma, err)
			}
		}
		for _, mode := range []CheckpointMode{
			CheckpointPassive, CheckpointFull, CheckpointRestart, CheckpointTruncate,
		} {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA wal_checkpoint(%s)", mode)); err != nil {
				t.Errorf("wal_checkpoint(%s) failed: %v", mode, err)
			}
		}
	})

	t.Run("INSERTs", func(t *testing.T) {
		resource := model.NewResource(map[string]model.AttributeValue{
			"service.name": model.NewStringValue("test-svc"),
			"host.name":    model.NewStringValue("test-host"),
		})
		record := &model.LogRecord{
			Timestamp: 1000, ObservedTimestamp: 2000,
			SeverityNumber: model.SeverityInfo, SeverityText: "INFO",
			Body: "test log message", EventName: "test.event",
			Attributes: []model.Attribute{
				{Key: "string.key", Str: "string-val", Kind: model.ValueString},
				{Key: "int.key", Num: 42, Kind: model.ValueInt},
				{Key: "double.key", Dbl: 3.14, Kind: model.ValueDouble},
				{Key: "bool.key", Flag: true, Kind: model.ValueBool},
				{Key: "bytes.key", Raw: []byte{0xAA, 0xBB}, Kind: model.ValueBytes},
			},
		}
		batch := model.NewLogBatch(1)
		batch.AddRecord(record)
		batch.Resource = resource

		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if err := NewWriteBatchCommand(batch).Execute(ctx, tx); err != nil {
			_ = tx.Rollback()
			t.Fatalf("unprepared write: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		stmts, err := initPreparedStatements(db)
		if err != nil {
			t.Fatal(err)
		}
		defer stmts.Close()
		record2 := &model.LogRecord{Timestamp: 3000, ObservedTimestamp: 4000, Body: "error"}
		batch2 := model.NewLogBatch(1)
		batch2.AddRecord(record2)
		batch2.Resource = resource
		cmd2 := NewWriteBatchCommand(batch2)
		cmd2.SetPreparedStatements(stmts)
		tx2, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd2.Execute(ctx, tx2); err != nil {
			_ = tx2.Rollback()
			t.Fatalf("prepared write: %v", err)
		}
		if err := tx2.Commit(); err != nil {
			t.Fatal(err)
		}

		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("expected 2 events, got %d", count)
		}
	})

	t.Run("DELETEs", func(t *testing.T) {
		if _, err := db.ExecContext(ctx, "INSERT OR IGNORE INTO log_resource(id, service_name) VALUES(?, ?)", "res-delete", "delete-svc"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO log_event(
			id, resource_id, timestamp_ns, observed_timestamp_ns, severity_number,
			severity_text, flags, dropped_attributes_count, attributes_json)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			100, "res-delete", 1, 1, 1, "DEBUG", 0, 0, `{"old.key":"old.value"}`); err != nil {
			t.Fatal(err)
		}
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM log_event WHERE rowid IN (
			SELECT rowid FROM log_event WHERE timestamp_ns < ? ORDER BY rowid LIMIT ?)`, 500, 100)
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if n, _ := result.RowsAffected(); n != 1 {
			t.Errorf("expected 1 event deleted, got %d", n)
		}
		result, err = tx.ExecContext(ctx, `DELETE FROM log_resource WHERE id NOT IN (
			SELECT DISTINCT resource_id FROM log_event)`)
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if n, _ := result.RowsAffected(); n != 1 {
			t.Errorf("expected 1 orphan resource deleted, got %d", n)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("PurgeLogsCommand_Execute", func(t *testing.T) {
		if _, err := db.ExecContext(ctx, "INSERT OR IGNORE INTO log_resource(id, service_name) VALUES(?, ?)", "res-purge", "purge-svc"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO log_event(
			id, resource_id, timestamp_ns, observed_timestamp_ns, severity_number,
			severity_text, flags, dropped_attributes_count, attributes_json)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			200, "res-purge", 2, 2, 1, "DEBUG", 0, 0, `{"another.key":"another.value"}`); err != nil {
			t.Fatal(err)
		}
		cmd := NewPurgeLogsCommand(time.Hour, 100)
		cmd.cutoffNanos = time.Now().Add(time.Hour).UnixNano()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Execute(ctx, tx); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("VACUUM", func(t *testing.T) {
		if err := NewVacuumCommand().ExecuteNonTransactional(ctx, db); err != nil {
			t.Error(err)
		}
	})
}

func TestSQLStatementsIsolated(t *testing.T) {
	tests := []struct {
		name string
		fn   func(*testing.T, *sql.DB)
	}{
		{"insertResource_sql", testInsertResourceSQL},
		{"insertEvent_sql", testInsertEventSQL},
		{"deleteEvent_with_rowid_subquery", testDeleteEventWithRowidSubquery},
		{"deleteOrphanResources", testDeleteOrphanResources},
		{"checkpoint_passive", testCheckpointPassive},
		{"checkpoint_full", testCheckpointFull},
		{"checkpoint_restart", testCheckpointRestart},
		{"checkpoint_truncate", testCheckpointTruncate},
		{"pragma_optimize", testPragmaOptimize},
		{"vacuum", testVacuum},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbpath := fmt.Sprintf("test_isolated_%s_%d.db", tt.name, time.Now().UnixNano())
			defer os.Remove(dbpath)
			defer os.Remove(dbpath + "-wal")
			defer os.Remove(dbpath + "-shm")
			db, err := openDatabase(dbpath, false)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := RunMigrations(db); err != nil {
				t.Fatal(err)
			}
			tt.fn(t, db)
		})
	}
}

func testInsertResourceSQL(t *testing.T, db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(sqlInsertResource, "res-1", "svc-a", "host-a", "http://schema", `{"key":"val"}`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func testInsertEventSQL(t *testing.T, db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(sqlInsertResource, "res-1", "svc-a", nil, nil, `{}`); err != nil {
		t.Fatal(err)
	}
	args := []any{nil, "res-1", int64(1000), int64(2000), int64(1), "INFO", []byte{1}, []byte{2}, "body", "event", uint64(0), uint64(0), "scope", "v1", `{"k":"v"}`}
	if _, err := tx.Exec(sqlInsertEvent, args...); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func testDeleteEventWithRowidSubquery(t *testing.T, db *sql.DB) {
	if _, err := db.Exec(sqlInsertResource, "res-1", "svc", nil, nil, `{}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(sqlInsertEvent, nil, "res-1", int64(10), int64(10), int64(1), "INFO", nil, nil, "body", "event", uint64(0), uint64(0), "", "", `{}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM log_event WHERE rowid IN (
		SELECT rowid FROM log_event WHERE timestamp_ns < ? ORDER BY rowid LIMIT ?)`, 500, 100); err != nil {
		t.Fatal(err)
	}
}

func testDeleteOrphanResources(t *testing.T, db *sql.DB) {
	if _, err := db.Exec(sqlInsertResource, "orphan", "orphan-svc", nil, nil, `{}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM log_resource WHERE id NOT IN (
		SELECT DISTINCT resource_id FROM log_event)`); err != nil {
		t.Fatal(err)
	}
}

func testCheckpointPassive(t *testing.T, db *sql.DB)  { testCheckpoint(t, db, "PASSIVE") }
func testCheckpointFull(t *testing.T, db *sql.DB)     { testCheckpoint(t, db, "FULL") }
func testCheckpointRestart(t *testing.T, db *sql.DB)  { testCheckpoint(t, db, "RESTART") }
func testCheckpointTruncate(t *testing.T, db *sql.DB) { testCheckpoint(t, db, "TRUNCATE") }

func testCheckpoint(t *testing.T, db *sql.DB, mode string) {
	if _, err := db.Exec("PRAGMA wal_checkpoint(" + mode + ")"); err != nil {
		t.Fatal(err)
	}
}

func testPragmaOptimize(t *testing.T, db *sql.DB) {
	if _, err := db.Exec("PRAGMA optimize"); err != nil {
		t.Fatal(err)
	}
}

func testVacuum(t *testing.T, db *sql.DB) {
	if err := NewVacuumCommand().ExecuteNonTransactional(context.Background(), db); err != nil {
		t.Fatal(err)
	}
}
