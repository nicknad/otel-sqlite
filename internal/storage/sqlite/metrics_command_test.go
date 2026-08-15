package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// buildMetricBatch constructs a hand-built MetricBatch with known IDs:
// one resource, one scope, two metrics (gauge + sum), two series.
func buildMetricBatch() *model.MetricBatch {
	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("metrics-svc"),
		"host.name":    model.NewStringValue("host-1"),
	})
	resource.ID = "res-metrics-test"

	gauge := &model.Metric{
		ID:           "metric-gauge",
		ResourceID:   resource.ID,
		ScopeID:      "scope-test",
		ScopeName:    "test-scope",
		ScopeVersion: "1.0.0",
		Name:         "process.memory.usage",
		Unit:         "By",
		Type:         model.MetricTypeGauge,
		Series: []*model.MetricSeries{
			{
				ID: "series-gauge-empty",
				DataPoints: []*model.DataPoint{
					{Timestamp: 100, DoubleValue: float64Ptr(123)},
					{Timestamp: 101, DoubleValue: float64Ptr(456)},
				},
			},
			{
				ID:         "series-gauge-pid",
				Attributes: []model.Attribute{{Key: "pid", Num: 1, Kind: model.ValueInt}},
				DataPoints: []*model.DataPoint{
					{Timestamp: 100, DoubleValue: float64Ptr(789)},
				},
			},
		},
	}

	sum := &model.Metric{
		ID:           "metric-sum",
		ResourceID:   resource.ID,
		ScopeID:      "scope-test",
		ScopeName:    "test-scope",
		ScopeVersion: "1.0.0",
		Name:         "http.server.requests",
		Type:         model.MetricTypeSum,
		IsMonotonic:  true,
		Temporality:  model.TemporalityCumulative,
		Series: []*model.MetricSeries{
			{
				ID: "series-sum-200",
				Attributes: []model.Attribute{
					{Key: "method", Str: "GET", Kind: model.ValueString},
					{Key: "status_code", Num: 200, Kind: model.ValueInt},
				},
				DataPoints: []*model.DataPoint{
					{Timestamp: 100, IntValue: int64Ptr(42)},
				},
			},
		},
	}

	batch := model.NewMetricBatch(2)
	batch.Resource = resource
	batch.AddMetric(gauge)
	batch.AddMetric(sum)
	return batch
}

func float64Ptr(v float64) *float64 { return &v }
func int64Ptr(v int64) *int64       { return &v }

func TestWriteMetricsCommand_RowsInserted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics-command.db")
	db, err := openDatabase(path, true, false)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	defer db.Close()
	if err := RunMigrations(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	cmd := NewWriteMetricsCommand(buildMetricBatch())
	if err := cmd.Execute(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("Execute: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	assertCount(t, db, "scope", 1)
	assertCount(t, db, "metric", 2)
	assertCount(t, db, "metric_series", 3)
	assertCount(t, db, "metric_data_point", 4)

	// Resource row shared with logs.
	assertCount(t, db, "log_resource", 1)

	// Gauge series attribute identity stored as canonical JSON.
	var attrsJSON string
	if err := db.QueryRow(
		"SELECT attributes_json FROM metric_series WHERE id = ?", "series-gauge-pid",
	).Scan(&attrsJSON); err != nil {
		t.Fatalf("query series attrs: %v", err)
	}
	if attrsJSON != `{"pid":1}` {
		t.Errorf("series attributes_json = %s", attrsJSON)
	}

	// Sum metadata (temporality, monotonic) persisted.
	var isMonotonic, temporality int
	if err := db.QueryRow(
		"SELECT is_monotonic, aggregation_temporality FROM metric WHERE id = ?", "metric-sum",
	).Scan(&isMonotonic, &temporality); err != nil {
		t.Fatalf("query metric: %v", err)
	}
	if isMonotonic != 1 || temporality != int(model.TemporalityCumulative) {
		t.Errorf("sum metadata = monotonic:%d temporality:%d", isMonotonic, temporality)
	}

	// Data point values round-trip.
	var doubleVal sql.NullFloat64
	if err := db.QueryRow(
		"SELECT double_value FROM metric_data_point WHERE series_id = ? AND timestamp_ns = 101", "series-gauge-empty",
	).Scan(&doubleVal); err != nil {
		t.Fatalf("query double: %v", err)
	}
	if !doubleVal.Valid || doubleVal.Float64 != 456 {
		t.Errorf("double_value = %v", doubleVal)
	}

	var intVal sql.NullInt64
	if err := db.QueryRow(
		"SELECT int_value FROM metric_data_point WHERE series_id = ?", "series-sum-200",
	).Scan(&intVal); err != nil {
		t.Fatalf("query int: %v", err)
	}
	if !intVal.Valid || intVal.Int64 != 42 {
		t.Errorf("int_value = %v", intVal)
	}

	// Data points can be queried by series + time (the series-time index).
	var count int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM metric_data_point WHERE series_id = ? AND timestamp_ns >= 100 AND timestamp_ns < 200",
		"series-gauge-empty",
	).Scan(&count); err != nil {
		t.Fatalf("range query: %v", err)
	}
	if count != 2 {
		t.Errorf("range query returned %d rows, want 2", count)
	}
}

func TestWriteMetricsCommand_DedupByIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics-dedup.db")
	db, err := openDatabase(path, true, false)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	defer db.Close()
	if err := RunMigrations(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ctx := context.Background()

	// Execute the same batch twice (as two commands). Identical metric /
	// series identities must not create duplicate rows; data points are
	// append-only.
	seen := &MetricsSeenCaches{
		Resources: map[string]struct{}{},
		Scopes:    map[string]struct{}{},
		Metrics:   map[string]struct{}{},
		Series:    map[string]struct{}{},
	}
	for i := 0; i < 2; i++ {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		cmd := NewWriteMetricsCommand(buildMetricBatch())
		cmd.SetSeenCaches(seen)
		if err := cmd.Execute(ctx, tx); err != nil {
			_ = tx.Rollback()
			t.Fatalf("Execute: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	assertCount(t, db, "scope", 1)
	assertCount(t, db, "metric", 2)
	assertCount(t, db, "metric_series", 3)
	// Data points duplicated (8 total: 4 per execution).
	assertCount(t, db, "metric_data_point", 8)

	// Dedup must also hold without the process-local cache (INSERT OR IGNORE
	// alone is sufficient) — clear caches and write again.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	cmd := NewWriteMetricsCommand(buildMetricBatch())
	cmd.SetSeenCaches(&MetricsSeenCaches{})
	if err := cmd.Execute(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("Execute: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	assertCount(t, db, "scope", 1)
	assertCount(t, db, "metric", 2)
	assertCount(t, db, "metric_series", 3)
	assertCount(t, db, "metric_data_point", 12)
}

func TestWriteMetricsCommand_ExemplarsJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics-exemplars.db")
	db, err := openDatabase(path, true, false)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	defer db.Close()
	if err := RunMigrations(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	batch := buildMetricBatch()
	series := batch.Metrics[0].Series[0]
	series.DataPoints[0].Exemplars = []model.Exemplar{
		{
			Timestamp:   100,
			TraceID:     [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:      [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
			HasTrace:    true,
			DoubleValue: float64Ptr(0.35),
		},
	}

	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := NewWriteMetricsCommand(batch).Execute(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("Execute: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var exemplarsJSON string
	if err := db.QueryRow(
		"SELECT exemplars_json FROM metric_data_point WHERE series_id = ? AND timestamp_ns = 100",
		"series-gauge-empty",
	).Scan(&exemplarsJSON); err != nil {
		t.Fatalf("query exemplars: %v", err)
	}
	var exemplars []map[string]any
	if err := json.Unmarshal([]byte(exemplarsJSON), &exemplars); err != nil {
		t.Fatalf("parse exemplars: %v", err)
	}
	if len(exemplars) != 1 {
		t.Fatalf("exemplars = %d", len(exemplars))
	}
	if exemplars[0]["trace_id"] != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("trace_id = %v", exemplars[0]["trace_id"])
	}
	if exemplars[0]["double_value"] != 0.35 {
		t.Errorf("double_value = %v", exemplars[0]["double_value"])
	}
}

func TestMigration006_TablesExist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration006.db")
	db, err := openDatabase(path, true, false)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	defer db.Close()
	if err := RunMigrations(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	for _, table := range []string{"scope", "metric", "metric_series", "metric_data_point"} {
		var name string
		if err := db.QueryRow(
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?", table,
		).Scan(&name); err != nil {
			t.Errorf("table %q missing after migration 006: %v", table, err)
		}
	}
	for _, index := range []string{
		"idx_metric_dp_series_time",
		"idx_metric_series_metric",
		"idx_metric_scope",
	} {
		var name string
		if err := db.QueryRow(
			"SELECT name FROM sqlite_master WHERE type='index' AND name=?", index,
		).Scan(&name); err != nil {
			t.Errorf("index %q missing after migration 006: %v", index, err)
		}
	}

	// Migration must be recorded.
	var applied string
	if err := db.QueryRow(
		"SELECT version FROM schema_migrations WHERE version = '006'",
	).Scan(&applied); err != nil {
		t.Errorf("migration 006 not recorded: %v", err)
	}
}

func assertCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Errorf("count(%s) = %d, want %d", table, got, want)
	}
}
