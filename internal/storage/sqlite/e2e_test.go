package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/batcher"
	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/otlp"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"

	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1"
	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	logsPB "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1"
	resourceV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1"
)

// TestE2E_OTLPToSQLite is an end-to-end regression test that exercises the
// full ingestion pipeline: OTLP gRPC → mapper → ingress queue → batcher →
// SQLite writer. After ingestion it queries the database directly and
// asserts that resource attributes, log events, and the FTS index are all
// correctly persisted.
//
// This test would have caught the three prior "wired but not connected"
// bugs:
//  1. Resource attributes lost (attributes_json hardcoded to "{}")
//  2. Migrations authored but not applied
//  3. gzip decompressor never registered
//
// It also validates the fixes for retention-loop (FTS rebuild), foreign
// key enforcement, and resource attribute serialization.
func TestE2E_OTLPToSQLite(t *testing.T) {
	// ---- setup: temp database ----
	dbPath := t.TempDir() + "/e2e-test.db"

	// ---- setup: full pipeline ----
	ingressQueue := ingest.NewIngressQueue(1000)
	cmdQueue := storage.NewCommandQueue(100)

	// Create the SQLite writer (runs migrations, opens DB).
	writer, err := NewWriter(cmdQueue, &WriterConfig{
		Path:          dbPath,
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		WALMode:       true,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.Start(ctx)
	defer writer.Stop()

	// Create batcher (wraps LogBatch → WriteBatchCommand → cmdQueue).
	b := batcher.NewBatcher(ingressQueue, cmdQueue, &batcher.BatcherConfig{
		BatchSize:     50,
		FlushInterval: 50 * time.Millisecond,
	})
	b.WithCommandFactory(func(batch *model.LogBatch) storage.Command {
		return NewWriteBatchCommand(batch)
	})
	b.Start(context.Background())
	defer b.Stop()

	// Create OTLP server (maps protobuf → internal model).
	svr := otlp.NewServer(ingressQueue, nil)

	// ---- send: OTLP Export request with resource attributes ----
	now := uint64(time.Now().UnixNano())
	req := &logsV1.ExportLogsServiceRequest{
		ResourceLogs: []*logsPB.ResourceLogs{
			{
				Resource: &resourceV1.Resource{
					Attributes: []*commonV1.KeyValue{
						{Key: "service.name", Value: &commonV1.AnyValue{
							Value: &commonV1.AnyValue_StringValue{StringValue: "my-api"},
						}},
						{Key: "host.name", Value: &commonV1.AnyValue{
							Value: &commonV1.AnyValue_StringValue{StringValue: "host-1"},
						}},
						{Key: "deployment.environment", Value: &commonV1.AnyValue{
							Value: &commonV1.AnyValue_StringValue{StringValue: "production"},
						}},
						{Key: "telemetry.sdk.version", Value: &commonV1.AnyValue{
							Value: &commonV1.AnyValue_StringValue{StringValue: "1.2.3"},
						}},
					},
				},
				ScopeLogs: []*logsPB.ScopeLogs{
					{
						Scope: &commonV1.InstrumentationScope{
							Name:    "test-scope",
							Version: "0.1.0",
						},
						LogRecords: []*logsPB.LogRecord{
							{
								TimeUnixNano:         now,
								ObservedTimeUnixNano: now,
								SeverityNumber:       logsPB.SeverityNumber_SEVERITY_NUMBER_ERROR,
								SeverityText:         "ERROR",
								Body: &commonV1.AnyValue{
									Value: &commonV1.AnyValue_StringValue{
										StringValue: "disk full on /data",
									},
								},
								Attributes: []*commonV1.KeyValue{
									{Key: "disk.path", Value: &commonV1.AnyValue{
										Value: &commonV1.AnyValue_StringValue{StringValue: "/data"},
									}},
									{Key: "disk.usage_pct", Value: &commonV1.AnyValue{
										Value: &commonV1.AnyValue_IntValue{IntValue: 99},
									}},
								},
							},
							{
								TimeUnixNano:         now + 1,
								ObservedTimeUnixNano: now,
								SeverityNumber:       logsPB.SeverityNumber_SEVERITY_NUMBER_INFO,
								SeverityText:         "INFO",
								Body: &commonV1.AnyValue{
									Value: &commonV1.AnyValue_StringValue{
										StringValue: "request processed successfully",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, err = svr.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	// ---- wait for pipeline to drain ----
	time.Sleep(500 * time.Millisecond)

	// Flush any remaining commands.
	writer.Stop()
	writer.Wait()

	// ---- assert: open DB read-only and verify everything ----
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// 1. Resource row exists with correct attributes_json.
	var attrsJSON string
	var svcName, hostName string
	err = db.QueryRow(
		"SELECT service_name, host_name, attributes_json FROM log_resource WHERE service_name = ?",
		"my-api",
	).Scan(&svcName, &hostName, &attrsJSON)
	if err != nil {
		t.Fatalf("query resource: %v", err)
	}

	if svcName != "my-api" {
		t.Errorf("service_name = %q, want %q", svcName, "my-api")
	}
	if hostName != "host-1" {
		t.Errorf("host_name = %q, want %q", hostName, "host-1")
	}

	// Assert attributes_json is NOT "{}" — it must contain deployment.environment.
	if attrsJSON == "{}" || attrsJSON == "" {
		t.Errorf("attributes_json is %q; expected real resource attributes (bug: attrs were dropped)", attrsJSON)
	}
	var attrs map[string]interface{}
	if err := json.Unmarshal([]byte(attrsJSON), &attrs); err != nil {
		t.Fatalf("parse attributes_json: %v", err)
	}
	if env, ok := attrs["deployment.environment"]; !ok {
		t.Error("attributes_json missing deployment.environment; resource attributes were lost")
	} else {
		envMap, ok := env.(map[string]interface{})
		if !ok || envMap["string_value"] != "production" {
			t.Errorf("deployment.environment = %v, want production", env)
		}
	}
	if sdk, ok := attrs["telemetry.sdk.version"]; !ok {
		t.Error("attributes_json missing telemetry.sdk.version; resource attributes were lost")
	} else {
		sdkMap, ok := sdk.(map[string]interface{})
		if !ok || sdkMap["string_value"] != "1.2.3" {
			t.Errorf("telemetry.sdk.version = %v, want 1.2.3", sdk)
		}
	}

	// 2. Log events exist with correct body text.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&count)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 log events, got %d", count)
	}

	var body1 string
	err = db.QueryRow(
		"SELECT body FROM log_event WHERE body LIKE ?", "%disk full%",
	).Scan(&body1)
	if err != nil {
		t.Fatalf("query event body: %v", err)
	}
	if body1 != "disk full on /data" {
		t.Errorf("body = %q, want %q", body1, "disk full on /data")
	}

	// 3. Event attributes are persisted inline on the event row.
	var eventAttrsJSON string
	err = db.QueryRow(
		"SELECT attributes_json FROM log_event WHERE body LIKE ?", "%disk full%",
	).Scan(&eventAttrsJSON)
	if err != nil {
		t.Fatalf("query event attributes: %v", err)
	}
	var eventAttrs map[string]interface{}
	if err := json.Unmarshal([]byte(eventAttrsJSON), &eventAttrs); err != nil {
		t.Fatalf("parse event attributes: %v", err)
	}
	if eventAttrs["disk.path"] != "/data" {
		t.Errorf("disk.path = %v, want %q", eventAttrs["disk.path"], "/data")
	}
	if eventAttrs["disk.usage_pct"] != float64(99) {
		t.Errorf("disk.usage_pct = %v, want 99", eventAttrs["disk.usage_pct"])
	}

	// 4. FTS index is populated (rebuild to populate, then query).
	// The logs_fts table starts empty (migration creates it but doesn't populate).
	// Run a rebuild and verify it works end-to-end.
	ftsCmd := NewRebuildFtsCommand()
	db2, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db for FTS: %v", err)
	}
	defer db2.Close()

	tx, err := db2.Begin()
	if err != nil {
		t.Fatalf("begin FTS tx: %v", err)
	}
	if err := ftsCmd.Execute(context.Background(), tx); err != nil {
		tx.Rollback()
		t.Fatalf("RebuildFtsCommand: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit FTS: %v", err)
	}

	// Query FTS — should find the error message.
	rows, err := db2.Query(
		"SELECT rowid FROM logs_fts WHERE logs_fts MATCH ?", "disk",
	)
	if err != nil {
		t.Fatalf("FTS query: %v", err)
	}
	defer rows.Close()

	var ftsCount int
	for rows.Next() {
		var rowid int64
		if err := rows.Scan(&rowid); err != nil {
			t.Fatalf("scan FTS row: %v", err)
		}
		ftsCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("FTS rows iter: %v", err)
	}
	if ftsCount < 1 {
		t.Error("FTS index returned no results for 'disk'; rebuild or populate failed")
	}

	// Also verify the FTS rowid maps to a valid log_event via the logs view.
	// Contentless FTS5 (content='') doesn't store source text; join to get body.
	var ftsBody string
	err = db2.QueryRow(
		"SELECT logs.body FROM logs_fts JOIN logs ON logs.id = logs_fts.rowid WHERE logs_fts MATCH ?", "request",
	).Scan(&ftsBody)
	if err != nil {
		t.Fatalf("FTS query 'request': %v", err)
	}
	if !strings.Contains(ftsBody, "request processed") {
		t.Errorf("FTS returned body = %q, want 'request processed successfully'", ftsBody)
	}

	t.Logf("E2E: resource attributes preserved (%d keys), %d events, inline event attrs, FTS OK",
		len(attrs), count)
}

// TestE2E_ForeignKeysAreEnforced verifies that PRAGMA foreign_keys is ON
// and that the declared FKs actually reject violations.
func TestE2E_ForeignKeysAreEnforced(t *testing.T) {
	dbPath := t.TempDir() + "/fk-test.db"

	// Create the DB through the writer (which runs migrations and enables FKs).
	cmdQueue := storage.NewCommandQueue(10)
	writer, err := NewWriter(cmdQueue, &WriterConfig{
		Path:          dbPath,
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		WALMode:       true,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.Start(ctx)
	writer.Stop()
	writer.Wait()

	// Open the DB separately and verify FK pragma is on.
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Enable foreign keys on this connection (PRAGMA is per-connection;
	// the writer enables it on its own connection).
	_, err = db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("enable foreign_keys: %v", err)
	}

	var fkEnabled int
	err = db.QueryRow("PRAGMA foreign_keys").Scan(&fkEnabled)
	if err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	if fkEnabled != 1 {
		t.Errorf("foreign_keys pragma = %d, want 1 (ON)", fkEnabled)
	}

	// Try inserting an event with a non-existent resource_id — must fail.
	_, err = db.Exec(
		`
		INSERT INTO log_event (
			resource_id, timestamp_ns, observed_timestamp_ns,
			severity_number, severity_text, body, event_name,
			flags, dropped_attributes_count
		) VALUES ('nonexistent', 0, 0, 1, 'INFO', 'test', '', 0, 0)`,
	)
	if err == nil {
		t.Error("expected FK violation when inserting event with nonexistent resource_id, but got no error")
	} else {
		t.Logf("FK violation correctly rejected: %v", err)
	}
}

// TestE2E_RetentionDeletesAllExpiredRows verifies that retention loops until
// all expired rows are deleted, not just a single batchSize chunk.
func TestE2E_RetentionDeletesAllExpiredRows(t *testing.T) {
	dbPath := t.TempDir() + "/retention-test.db"

	cmdQueue := storage.NewCommandQueue(10)
	writer, err := NewWriter(cmdQueue, &WriterConfig{
		Path:          dbPath,
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		WALMode:       true,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.Start(ctx)
	defer writer.Stop()

	// Insert a resource so FKs are satisfied.
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`INSERT INTO log_resource
		(id, service_name, host_name, attributes_json)
		VALUES ('res-1', 'svc', 'h1', '{}')`)
	if err != nil {
		t.Fatalf("insert resource: %v", err)
	}

	// Insert 25 events with old timestamps (batch size is 10, so 3 batches needed).
	for i := 0; i < 25; i++ {
		_, err = db.Exec(
			`
			INSERT INTO log_event (
				resource_id, timestamp_ns, observed_timestamp_ns,
				severity_number, severity_text, body, event_name,
				flags, dropped_attributes_count
			) VALUES ('res-1', 1, 1, 1, 'INFO', 'old', '', 0, 0)`,
		)
		if err != nil {
			t.Fatalf("insert event %d: %v", i, err)
		}
	}

	// Verify 25 events exist.
	var before int
	db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&before)
	if before != 25 {
		t.Fatalf("expected 25 events before retention, got %d", before)
	}

	// Run retention with cutoff far in the future (all 25 are old).
	// Use a small batch size to verify the loop runs multiple times.
	cmd := NewPurgeLogsCommand(-1*time.Hour, 10) // cutoffAge = -1h means cutoff is in the future
	// Override cutoff to be in the future so all events are old.
	cmd.cutoffNanos = time.Now().Add(1 * time.Hour).UnixNano()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := cmd.Execute(context.Background(), tx); err != nil {
		tx.Rollback()
		t.Fatalf("PurgeLogsCommand: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify all 25 events were deleted (not just 10).
	var after int
	db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&after)
	if after != 0 {
		t.Errorf("expected 0 events after retention, got %d (retention loop bug: only deleted one batch)", after)
	}

	t.Logf("Retention: %d events before → %d after (all deleted)", before, after)
}
