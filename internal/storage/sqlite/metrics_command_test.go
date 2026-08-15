package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
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
func uint64Ptr(v uint64) *uint64    { return &v }

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
		// Caches only become visible after commit, mirroring the writer.
		cmd.CommitSeen()
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
	cmd.CommitSeen()
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
		{
			Timestamp:   101,
			DoubleValue: float64Ptr(math.NaN()),
		},
		{
			Timestamp:   102,
			DoubleValue: float64Ptr(math.Inf(1)),
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
	if len(exemplars) != 3 {
		t.Fatalf("exemplars = %d", len(exemplars))
	}
	if exemplars[0]["trace_id"] != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("trace_id = %v", exemplars[0]["trace_id"])
	}
	if exemplars[0]["double_value"] != 0.35 {
		t.Errorf("double_value = %v", exemplars[0]["double_value"])
	}

	// NaN and +Inf exemplar values must survive as string markers instead
	// of failing json.Marshal (which previously rolled back the whole batch).
	if exemplars[1]["double_value"] != "NaN" {
		t.Errorf("NaN exemplar double_value = %v, want \"NaN\" marker", exemplars[1]["double_value"])
	}
	if exemplars[2]["double_value"] != "+Inf" {
		t.Errorf("+Inf exemplar double_value = %v, want \"+Inf\" marker", exemplars[2]["double_value"])
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

// TestWriteMetricsCommand_RollbackDoesNotPoisonCaches verifies the fix for
// the dedup-cache rollback hazard: IDs inserted by Execute are only merged
// into the live caches after commit (via CommitSeen). If the surrounding
// transaction rolls back, the caches must stay empty so the next write of
// the same batch re-inserts every row instead of skipping them.
func TestWriteMetricsCommand_RollbackDoesNotPoisonCaches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics-rollback.db")
	db, err := openDatabase(path, true, false)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	defer db.Close()
	if err := RunMigrations(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ctx := context.Background()
	seen := &MetricsSeenCaches{
		Resources: map[string]struct{}{},
		Scopes:    map[string]struct{}{},
		Metrics:   map[string]struct{}{},
		Series:    map[string]struct{}{},
	}

	// Execute in a transaction that is rolled back (simulating a later
	// command in the same transaction failing). CommitSeen is deliberately
	// NOT called — the writer only calls it after a successful commit.
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
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// No rows committed, and the caches must not claim any.
	assertCount(t, db, "metric_data_point", 0)
	if len(seen.Resources) != 0 || len(seen.Scopes) != 0 || len(seen.Metrics) != 0 || len(seen.Series) != 0 {
		t.Fatalf("dedup caches poisoned by rolled-back transaction: resources=%d scopes=%d metrics=%d series=%d",
			len(seen.Resources), len(seen.Scopes), len(seen.Metrics), len(seen.Series))
	}

	// A fresh transaction must re-insert everything; after commit the
	// caches reflect the committed rows.
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	cmd2 := NewWriteMetricsCommand(buildMetricBatch())
	cmd2.SetSeenCaches(seen)
	if err := cmd2.Execute(ctx, tx2); err != nil {
		_ = tx2.Rollback()
		t.Fatalf("Execute: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	cmd2.CommitSeen()

	assertCount(t, db, "scope", 1)
	assertCount(t, db, "metric", 2)
	assertCount(t, db, "metric_series", 3)
	assertCount(t, db, "metric_data_point", 4)

	if len(seen.Resources) != 1 || len(seen.Scopes) != 1 || len(seen.Metrics) != 2 || len(seen.Series) != 3 {
		t.Fatalf("caches not populated after commit: resources=%d scopes=%d metrics=%d series=%d",
			len(seen.Resources), len(seen.Scopes), len(seen.Metrics), len(seen.Series))
	}
}

// TestWriteMetricsCommand_NanMask verifies the NaN persistence policy: NaN
// float columns are stored as NULL with the corresponding nan_mask bit set
// (SQLite REAL cannot store NaN), ±Inf survives as a real value, and finite
// points carry nan_mask = 0.
func TestWriteMetricsCommand_NanMask(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics-nanmask.db")
	db, err := openDatabase(path, true, false)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	defer db.Close()
	if err := RunMigrations(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	batch := buildMetricBatch()

	// Gauge point: double_value = NaN, everything else finite/absent.
	gaugeDP := batch.Metrics[0].Series[0].DataPoints[0]
	gaugeDP.DoubleValue = float64Ptr(math.NaN())

	// Histogram point: sum = NaN, min = NaN, max = +Inf (finite, survives).
	batch.AddMetric(&model.Metric{
		ID:           "metric-hist",
		ResourceID:   "res-metrics-test",
		ScopeID:      "scope-test",
		ScopeName:    "test-scope",
		ScopeVersion: "1.0.0",
		Name:         "hist",
		Type:         model.MetricTypeHistogram,
		Temporality:  model.TemporalityDelta,
		Series: []*model.MetricSeries{
			{
				ID: "series-hist",
				DataPoints: []*model.DataPoint{
					{
						Timestamp: 200,
						Count:     uint64Ptr(2),
						Sum:       float64Ptr(math.NaN()),
						Min:       float64Ptr(math.NaN()),
						Max:       float64Ptr(math.Inf(1)),
					},
					{
						Timestamp: 201,
						Count:     uint64Ptr(3),
						Sum:       float64Ptr(4.5),
						Min:       float64Ptr(1),
						Max:       float64Ptr(8),
					},
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
		t.Fatalf("Execute: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Gauge row: NaN double_value stored as NULL + bit 0.
	var doubleValue sql.NullFloat64
	var mask int
	if err := db.QueryRow(
		"SELECT double_value, nan_mask FROM metric_data_point WHERE series_id = ? AND timestamp_ns = 100",
		"series-gauge-empty",
	).Scan(&doubleValue, &mask); err != nil {
		t.Fatalf("query gauge row: %v", err)
	}
	if doubleValue.Valid {
		t.Errorf("double_value = %v, want NULL for NaN", doubleValue)
	}
	if mask != nanMaskDoubleValue {
		t.Errorf("gauge nan_mask = %d, want %d (double_value NaN)", mask, nanMaskDoubleValue)
	}

	// Histogram row 1: sum + min NaN → bits 1|2, max +Inf preserved.
	var sumV, minV, maxV sql.NullFloat64
	if err := db.QueryRow(
		"SELECT sum, min, max, nan_mask FROM metric_data_point WHERE series_id = ? AND timestamp_ns = 200",
		"series-hist",
	).Scan(&sumV, &minV, &maxV, &mask); err != nil {
		t.Fatalf("query hist row: %v", err)
	}
	if sumV.Valid || minV.Valid {
		t.Errorf("sum/min = %v/%v, want NULL for NaN", sumV, minV)
	}
	if !maxV.Valid || maxV.Float64 != math.Inf(1) {
		t.Errorf("max = %v, want +Inf preserved", maxV)
	}
	if mask != nanMaskSum|nanMaskMin {
		t.Errorf("hist nan_mask = %d, want %d (sum|min NaN)", mask, nanMaskSum|nanMaskMin)
	}

	// Histogram row 2: all finite → nan_mask 0.
	if err := db.QueryRow(
		"SELECT nan_mask FROM metric_data_point WHERE series_id = ? AND timestamp_ns = 201",
		"series-hist",
	).Scan(&mask); err != nil {
		t.Fatalf("query finite row: %v", err)
	}
	if mask != 0 {
		t.Errorf("finite row nan_mask = %d, want 0", mask)
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
