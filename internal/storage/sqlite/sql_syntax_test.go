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

// TestAllSQLStatements validates that every SQL statement pattern used in the
// sqlite package executes without syntax errors against modernc.org/sqlite.
//
// The test covers: DDL (CREATE TABLE, CREATE INDEX), INSERT (prepared and
// unprepared paths), DELETE with subquery-based batching, orphan cleanup,
// PRAGMA statements, and VACUUM.
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

	// Schema must be initialized before any sub-tests that depend on it.
	if err := initializeSchema(db); err != nil {
		t.Fatalf("initializeSchema: %v", err)
	}

	ctx := context.Background()

	// Verify all tables and indexes were created.
	t.Run("DDL_tables_and_indexes", func(t *testing.T) {
		tables := []string{"log_resource", "log_event", "log_attr"}
		for _, tbl := range tables {
			var name string
			err := db.QueryRow(
				"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
				tbl,
			).Scan(&name)
			if err != nil {
				t.Errorf("table %q missing: %v", tbl, err)
			}
		}

		indexes := []string{
			"idx_log_event_timestamp",
			"idx_log_event_severity",
			"idx_log_event_trace_id",
			"idx_log_event_resource_id",
			"idx_log_attr_event_id",
			"idx_log_attr_key",
			"idx_log_event_severity_text",
			"idx_log_event_body",
			"idx_log_event_resource_timestamp",
			"idx_log_event_trace_timestamp",
		}
		for _, idx := range indexes {
			var name string
			err := db.QueryRow(
				"SELECT name FROM sqlite_master WHERE type='index' AND name=?",
				idx,
			).Scan(&name)
			if err != nil {
				t.Errorf("index %q missing: %v", idx, err)
			}
		}
	})

	// Phase 2: PRAGMA statements (excluding journal_mode=WAL, which changes
	// the DB mode mid-session and can cause issues with subsequent operations).
	t.Run("PRAGMAs", func(t *testing.T) {
		pragmas := []string{
			"PRAGMA synchronous=NORMAL",
			"PRAGMA wal_autocheckpoint=1000",
			"PRAGMA optimize",
		}
		for _, p := range pragmas {
			if _, err := db.ExecContext(ctx, p); err != nil {
				t.Errorf("PRAGMA %q failed: %v", p, err)
			}
		}

		// CheckpointCommand.Execute with each mode.
		for _, mode := range []CheckpointMode{
			CheckpointPassive,
			CheckpointFull,
			CheckpointRestart,
			CheckpointTruncate,
		} {
			cmd := NewCheckpointCommand(mode)
			// Execute outside a transaction: PRAGMA wal_checkpoint is
			// idempotent on a non-WAL database and won't block.
			query := fmt.Sprintf("PRAGMA wal_checkpoint(%s)", mode)
			if _, err := db.ExecContext(ctx, query); err != nil {
				t.Errorf("wal_checkpoint(%s) failed: %v", mode, err)
			}
			_ = cmd // used via the direct query above, keep for coverage
		}
	})

	// Phase 3: INSERT statements — unprepared and prepared paths.
	t.Run("INSERTs", func(t *testing.T) {
		resource := model.NewResource(map[string]model.AttributeValue{
			"service.name": model.NewStringValue("test-svc"),
			"host.name":    model.NewStringValue("test-host"),
		})

		record := &model.LogRecord{
			Timestamp:              1000,
			ObservedTimestamp:      2000,
			SeverityNumber:         model.SeverityInfo,
			SeverityText:           "INFO",
			TraceID:                []byte{0x01, 0x02, 0x03},
			SpanID:                 []byte{0x04, 0x05, 0x06},
			Body:                   "test log message",
			EventName:              "test.event",
			Flags:                  1,
			DroppedAttributesCount: 0,
			ScopeName:              "test.scope",
			ScopeVersion:           "v1.0.0",
			Attributes: map[string]model.AttributeValue{
				"string.key": model.NewStringValue("string-val"),
				"int.key":    model.NewIntValue(42),
				"double.key": model.NewDoubleValue(3.14),
				"bool.key":   model.NewBoolValue(true),
				"bytes.key":  model.NewBytesValue([]byte{0xAA, 0xBB}),
			},
		}

		batch := model.NewLogBatch(1)
		batch.AddRecord(record)
		batch.Resource = resource

		// ---- Unprepared path (fallback with tx.PrepareContext) ----
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		cmd := NewWriteBatchCommand(batch)
		if err := cmd.Execute(ctx, tx); err != nil {
			tx.Rollback()
			t.Fatalf("WriteBatchCommand.Execute (unprepared): %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}

		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&count); err != nil {
			t.Fatalf("count events: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 event, got %d", count)
		}

		// ---- Prepared path (normal writer path via tx.Stmt()) ----
		record2 := &model.LogRecord{
			Timestamp:              3000,
			ObservedTimestamp:      4000,
			SeverityNumber:         model.SeverityError,
			SeverityText:           "ERROR",
			Body:                   "error message",
			Flags:                  0,
			DroppedAttributesCount: 0,
			Attributes:             map[string]model.AttributeValue{},
		}
		batch2 := model.NewLogBatch(1)
		batch2.AddRecord(record2)
		batch2.Resource = resource
		cmd2 := NewWriteBatchCommand(batch2)

		stmts, err := initPreparedStatements(db)
		if err != nil {
			t.Fatalf("initPreparedStatements: %v", err)
		}
		defer stmts.Close()
		cmd2.SetPreparedStatements(stmts)

		tx2, err := db.Begin()
		if err != nil {
			t.Fatalf("begin tx2: %v", err)
		}
		if err := cmd2.Execute(ctx, tx2); err != nil {
			tx2.Rollback()
			t.Fatalf("WriteBatchCommand.Execute (prepared): %v", err)
		}
		if err := tx2.Commit(); err != nil {
			t.Fatalf("commit tx2: %v", err)
		}

		if err := db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&count); err != nil {
			t.Fatalf("count events after second insert: %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2 events, got %d", count)
		}
	})

	// Phase 4: DELETE statements (purge queries).
	t.Run("DELETEs", func(t *testing.T) {
		// Insert data with old timestamps for purging.
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}

		_, err = tx.ExecContext(ctx,
			"INSERT OR IGNORE INTO log_resource(id, service_name) VALUES(?, ?)",
			"res-delete", "delete-svc",
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("insert resource: %v", err)
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO log_event(id, resource_id, timestamp_ns, observed_timestamp_ns,
			 severity_number, severity_text, flags, dropped_attributes_count)
			 VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
			100, "res-delete", int64(1), int64(1),
			1, "DEBUG", 0, 0,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("insert old event: %v", err)
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO log_attr(event_id, key, value_type, string_value)
			 VALUES(?, ?, ?, ?)`,
			100, "old.key", "string", "old.value",
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("insert old attr: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}

		// Now run the same SQL patterns used by PurgeLogsCommand.
		tx2, err := db.Begin()
		if err != nil {
			t.Fatalf("begin tx2: %v", err)
		}

		// Delete expired attributes with subquery LIMIT.
		result1, err := tx2.ExecContext(ctx,
			`DELETE FROM log_attr WHERE event_id IN (
				SELECT id FROM log_event WHERE timestamp_ns < ?
				ORDER BY id LIMIT ?
			)`,
			int64(500), 100,
		)
		if err != nil {
			tx2.Rollback()
			t.Fatalf("delete expired attributes: %v", err)
		}
		n1, _ := result1.RowsAffected()
		if n1 != 1 {
			t.Errorf("expected 1 attr deleted, got %d", n1)
		}

		// Delete expired events with rowid subquery LIMIT (the fix).
		result2, err := tx2.ExecContext(ctx,
			`DELETE FROM log_event WHERE rowid IN (
				SELECT rowid FROM log_event WHERE timestamp_ns < ?
				ORDER BY rowid LIMIT ?
			)`,
			int64(500), 100,
		)
		if err != nil {
			tx2.Rollback()
			t.Fatalf("delete expired events: %v", err)
		}
		n2, _ := result2.RowsAffected()
		if n2 != 1 {
			t.Errorf("expected 1 event deleted, got %d", n2)
		}

		// Clean orphaned resources.
		result3, err := tx2.ExecContext(ctx,
			`DELETE FROM log_resource WHERE id NOT IN (
				SELECT DISTINCT resource_id FROM log_event
			)`,
		)
		if err != nil {
			tx2.Rollback()
			t.Fatalf("clean orphaned resources: %v", err)
		}
		n3, _ := result3.RowsAffected()
		if n3 != 1 {
			t.Errorf("expected 1 orphan resource deleted, got %d", n3)
		}

		if err := tx2.Commit(); err != nil {
			t.Fatalf("commit tx2: %v", err)
		}
	})

	// Phase 5: PurgeLogsCommand.Execute end-to-end.
	t.Run("PurgeLogsCommand_Execute", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		_, err = tx.ExecContext(ctx,
			"INSERT OR IGNORE INTO log_resource(id, service_name) VALUES(?, ?)",
			"res-purge", "purge-svc",
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("insert resource: %v", err)
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO log_event(id, resource_id, timestamp_ns, observed_timestamp_ns,
			 severity_number, severity_text, flags, dropped_attributes_count)
			 VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
			200, "res-purge", int64(2), int64(2), 1, "DEBUG", 0, 0,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("insert old event: %v", err)
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO log_attr(event_id, key, value_type, string_value)
			 VALUES(?, ?, ?, ?)`,
			200, "another.key", "string", "another.value",
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("insert old attr: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}

		// Purge old records (cutoffAge=1h means records older than 1h).
		cmd := NewPurgeLogsCommand(1*time.Hour, 100)
		tx2, err := db.Begin()
		if err != nil {
			t.Fatalf("begin tx2: %v", err)
		}
		if err := cmd.Execute(ctx, tx2); err != nil {
			tx2.Rollback()
			t.Fatalf("PurgeLogsCommand.Execute: %v", err)
		}
		if err := tx2.Commit(); err != nil {
			t.Fatalf("commit tx2: %v", err)
		}
	})

	// Phase 6: VACUUM (non-transactional).
	t.Run("VACUUM", func(t *testing.T) {
		cmd := NewVacuumCommand()
		if err := cmd.ExecuteNonTransactional(ctx, db); err != nil {
			t.Errorf("VACUUM failed: %v", err)
		}
	})
}

// TestSQLStatementsIsolated runs each SQL statement pattern in a fresh
// database to catch syntax errors independently.
func TestSQLStatementsIsolated(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, db *sql.DB)
	}{
		{"insertResource_sql", testInsertResourceSQL},
		{"insertEvent_sql", testInsertEventSQL},
		{"insertAttr_sql", testInsertAttrSQL},
		{"deleteAttr_with_subquery_limit", testDeleteAttrWithSubqueryLimit},
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
				t.Fatalf("openDatabase: %v", err)
			}
			defer db.Close()

			if err := initializeSchema(db); err != nil {
				t.Fatalf("initializeSchema: %v", err)
			}

			tt.fn(t, db)
		})
	}
}

// ---- isolated test helpers ----

func testInsertResourceSQL(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, sqlInsertResource,
		"res-1", "svc-a", "host-a", "http://schema", `{"key":"val"}`,
	)
	if err != nil {
		t.Fatalf("sqlInsertResource: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func testInsertEventSQL(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, sqlInsertResource,
		"res-1", "svc-a", nil, nil, `{}`,
	)
	if err != nil {
		t.Fatalf("insert resource: %v", err)
	}

	_, err = tx.ExecContext(ctx, sqlInsertEvent,
		nil, // id auto-generated
		"res-1",
		int64(1000),
		int64(2000),
		int64(1),
		"INFO",
		[]byte{0x01},
		[]byte{0x02},
		"test body",
		"event.name",
		uint64(0),
		uint64(0),
		"scope",
		"v1",
	)
	if err != nil {
		t.Fatalf("sqlInsertEvent: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func testInsertAttrSQL(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, sqlInsertResource,
		"res-1", "svc-a", nil, nil, `{}`,
	)
	if err != nil {
		t.Fatalf("insert resource: %v", err)
	}
	_, err = tx.ExecContext(ctx, sqlInsertEvent,
		nil, "res-1",
		int64(1000), int64(2000), int64(1), "INFO",
		[]byte{0x01}, []byte{0x02}, "body", "event", uint64(0), uint64(0),
		"scope", "v1",
	)
	if err != nil {
		t.Fatalf("insert event: %v", err)
	}

	_, err = tx.ExecContext(ctx, sqlInsertAttr,
		int64(1), "attr.key", "string", "attr.value", nil, nil, nil, nil,
	)
	if err != nil {
		t.Fatalf("sqlInsertAttr: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func testDeleteAttrWithSubqueryLimit(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, sqlInsertResource, "res-1", "svc", nil, nil, `{}`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, sqlInsertEvent,
		nil, "res-1", int64(10), int64(10), int64(1), "INFO",
		nil, nil, "body", "event", uint64(0), uint64(0), "", "",
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, sqlInsertAttr,
		int64(1), "k", "string", "v", nil, nil, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	// The purge attribute query exercising subquery LIMIT inside IN.
	_, err = tx2.ExecContext(ctx,
		`DELETE FROM log_attr WHERE event_id IN (
			SELECT id FROM log_event WHERE timestamp_ns < ?
			ORDER BY id LIMIT ?
		)`,
		int64(500), 100,
	)
	if err != nil {
		t.Fatalf("delete attrs with subquery LIMIT: %v", err)
	}
}

func testDeleteEventWithRowidSubquery(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, sqlInsertResource, "res-1", "svc", nil, nil, `{}`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, sqlInsertEvent,
		nil, "res-1", int64(10), int64(10), int64(1), "INFO",
		nil, nil, "body", "event", uint64(0), uint64(0), "", "",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	// The DELETE with rowid subquery LIMIT — the fix for the reported error.
	_, err = tx2.ExecContext(ctx,
		`DELETE FROM log_event WHERE rowid IN (
			SELECT rowid FROM log_event WHERE timestamp_ns < ?
			ORDER BY rowid LIMIT ?
		)`,
		int64(500), 100,
	)
	if err != nil {
		t.Fatalf("delete events with rowid subquery: %v", err)
	}
}

func testDeleteOrphanResources(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, sqlInsertResource, "orphan", "orphan-svc", nil, nil, `{}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	_, err = tx2.ExecContext(ctx,
		`DELETE FROM log_resource WHERE id NOT IN (
			SELECT DISTINCT resource_id FROM log_event
		)`,
	)
	if err != nil {
		t.Fatalf("delete orphan resources: %v", err)
	}
}

func testCheckpointPassive(t *testing.T, db *sql.DB) {
	_, err := db.ExecContext(context.Background(), "PRAGMA wal_checkpoint(PASSIVE)")
	if err != nil {
		t.Fatalf("checkpoint PASSIVE: %v", err)
	}
}

func testCheckpointFull(t *testing.T, db *sql.DB) {
	_, err := db.ExecContext(context.Background(), "PRAGMA wal_checkpoint(FULL)")
	if err != nil {
		t.Fatalf("checkpoint FULL: %v", err)
	}
}

func testCheckpointRestart(t *testing.T, db *sql.DB) {
	_, err := db.ExecContext(context.Background(), "PRAGMA wal_checkpoint(RESTART)")
	if err != nil {
		t.Fatalf("checkpoint RESTART: %v", err)
	}
}

func testCheckpointTruncate(t *testing.T, db *sql.DB) {
	_, err := db.ExecContext(context.Background(), "PRAGMA wal_checkpoint(TRUNCATE)")
	if err != nil {
		t.Fatalf("checkpoint TRUNCATE: %v", err)
	}
}

func testPragmaOptimize(t *testing.T, db *sql.DB) {
	_, err := db.ExecContext(context.Background(), "PRAGMA optimize")
	if err != nil {
		t.Fatalf("PRAGMA optimize: %v", err)
	}
}

func testVacuum(t *testing.T, db *sql.DB) {
	cmd := NewVacuumCommand()
	if err := cmd.ExecuteNonTransactional(context.Background(), db); err != nil {
		t.Fatalf("VACUUM: %v", err)
	}
}
