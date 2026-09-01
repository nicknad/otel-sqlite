package sqlite

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// seedPurgeDB inserts one resource, one scope, one metric and two series
// with a total of 5 data points at timestamps 100..500.
func seedPurgeDB(t *testing.T, db *sql.DB) {
	t.Helper()

	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("purge-svc"),
	})
	resource.ID = "res-purge"
	batch := model.NewMetricBatch(1)
	batch.Resource = resource
	batch.AddMetric(&model.Metric{
		ID:         "metric-purge",
		ResourceID: resource.ID,
		ScopeID:    "scope-purge",
		ScopeName:  "purge-scope",
		Name:       "purge.gauge",
		Type:       model.MetricTypeGauge,
		Series: []*model.MetricSeries{
			{
				ID: "series-purge-a",
				DataPoints: []*model.DataPoint{
					{Timestamp: 100, DoubleValue: float64Ptr(1)},
					{Timestamp: 200, DoubleValue: float64Ptr(2)},
					{Timestamp: 300, DoubleValue: float64Ptr(3)},
				},
			},
			{
				ID: "series-purge-b",
				DataPoints: []*model.DataPoint{
					{Timestamp: 400, DoubleValue: float64Ptr(4)},
					{Timestamp: 500, DoubleValue: float64Ptr(5)},
				},
			},
		},
	})

	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := NewWriteMetricsCommand(batch).Execute(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("execute: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestPurgeMetricDataPointsCommand_DeletesExpiredAndOrphans(t *testing.T) {
	db, err := openDatabase(t.TempDir()+"/purge.db", true, false)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	defer db.Close()
	if err := RunMigrations(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	seedPurgeDB(t, db)

	ctx := context.Background()

	// Cutoff = 350 → points with timestamp 100,200,300 are expired (3 rows).
	cmd := NewPurgeMetricDataPointsCommand(time.Hour, 100)
	cmd.cutoffNanos = 350

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := cmd.Execute(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("execute: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Expired points gone, fresh points kept.
	var points int
	if err := db.QueryRow("SELECT COUNT(*) FROM metric_data_point").Scan(&points); err != nil {
		t.Fatalf("count points: %v", err)
	}
	if points != 2 {
		t.Errorf("expected 2 points after purge, got %d", points)
	}
	var kept int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM metric_data_point WHERE timestamp_ns IN (400, 500)",
	).Scan(&kept); err != nil {
		t.Fatalf("count kept: %v", err)
	}
	if kept != 2 {
		t.Errorf("expected timestamps 400,500 kept, got %d", kept)
	}

	// Series A has no remaining points → orphaned and removed.
	var series int
	if err := db.QueryRow("SELECT COUNT(*) FROM metric_series").Scan(&series); err != nil {
		t.Fatalf("count series: %v", err)
	}
	if series != 1 {
		t.Errorf("expected 1 series (orphan removed), got %d", series)
	}

	// The metric, scope and resource still exist (series B keeps them alive).
	var metricCount, scopeCount, resourceCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM metric").Scan(&metricCount); err != nil {
		t.Fatalf("count metric: %v", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM scope").Scan(&scopeCount); err != nil {
		t.Fatalf("count scope: %v", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM log_resource").Scan(&resourceCount); err != nil {
		t.Fatalf("count resource: %v", err)
	}
	if metricCount != 1 || scopeCount != 1 || resourceCount != 1 {
		t.Errorf("metric/scope/resource = %d/%d/%d, want 1/1/1",
			metricCount, scopeCount, resourceCount)
	}

	// Purge the rest: everything becomes orphaned and is removed.
	cmd.cutoffNanos = 100000
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := cmd.Execute(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("execute 2: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	var all int
	if err := db.QueryRow(
		"SELECT (SELECT COUNT(*) FROM metric_data_point) + (SELECT COUNT(*) FROM metric_series) + (SELECT COUNT(*) FROM metric) + (SELECT COUNT(*) FROM scope) + (SELECT COUNT(*) FROM log_resource)",
	).Scan(&all); err != nil {
		t.Fatalf("count all: %v", err)
	}
	if all != 0 {
		t.Errorf("expected full cleanup, got %d rows remaining", all)
	}
}

func TestPurgeMetricDataPointsCommand_MultipleBatches(t *testing.T) {
	db, err := openDatabase(t.TempDir()+"/purge-loop.db", true, false)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	defer db.Close()
	if err := RunMigrations(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	seedPurgeDB(t, db)

	ctx := context.Background()

	// batchSize 2 with 3 expired rows exercises the internal loop.
	cmd := NewPurgeMetricDataPointsCommand(time.Hour, 2)
	cmd.cutoffNanos = 350

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := cmd.Execute(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("execute: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var points int
	if err := db.QueryRow("SELECT COUNT(*) FROM metric_data_point").Scan(&points); err != nil {
		t.Fatalf("count: %v", err)
	}
	if points != 2 {
		t.Errorf("expected 2 points after loop purge, got %d (loop only deleted one batch?)", points)
	}
}

// TestPurgeMetricDataPointsCommand_SharedResourceKept ensures a resource
// still referenced by log events is not removed by metric retention.
func TestPurgeMetricDataPointsCommand_SharedResourceKept(t *testing.T) {
	db, err := openDatabase(t.TempDir()+"/purge-shared.db", true, false)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	defer db.Close()
	if err := RunMigrations(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	seedPurgeDB(t, db)

	// A log event references the same resource.
	if _, err := db.ExecContext(context.Background(),
		`INSERT OR IGNORE INTO log_resource(id, service_name) VALUES(?, ?)`, "res-purge", "purge-svc"); err != nil {
		t.Fatalf("insert log resource: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO log_event(
			id, resource_id, timestamp_ns, observed_timestamp_ns, severity_number,
			severity_text, flags, dropped_attributes_count, attributes_json)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		999, "res-purge", 1, 1, 1, "INFO", 0, 0, `{}`); err != nil {
		t.Fatalf("insert log event: %v", err)
	}

	// Purge everything metric-related.
	cmd := NewPurgeMetricDataPointsCommand(time.Hour, 100)
	cmd.cutoffNanos = 100000

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := cmd.Execute(context.Background(), tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("execute: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Resource survives because the log event references it.
	var resourceCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM log_resource").Scan(&resourceCount); err != nil {
		t.Fatalf("count resource: %v", err)
	}
	if resourceCount != 1 {
		t.Errorf("expected shared resource to survive, got %d", resourceCount)
	}
	var eventCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM log_event").Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Errorf("expected log event to survive, got %d", eventCount)
	}
}
