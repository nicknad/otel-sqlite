package query_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/query"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

func f64(v float64) *float64 { return &v }
func i64(v int64) *int64     { return &v }
func u64(v uint64) *uint64   { return &v }

// seedDB builds a database with known metric data through the real
// WriteMetricsCommand so the query layer is tested against stored rows.
func seedDB(t *testing.T, path string) {
	t.Helper()

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := sqlite.RunMigrations(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("query-svc"),
		"host.name":    model.NewStringValue("host-9"),
	})
	resource.ID = "res-query-test"

	nan := math.NaN()
	histJSON, _ := json.Marshal(map[string]any{
		"bounds": []any{1.0, 5.0, 10.0},
		"counts": []uint64{3, 7, 2, 9}, // +1 entry: implicit +Inf overflow bucket
	})
	exemplars := []model.Exemplar{
		{
			Timestamp:   500,
			DoubleValue: f64(7.5),
			TraceID:     [16]byte{0xaa, 0xbb, 0x01},
			SpanID:      [8]byte{0xcc, 0xdd},
			HasTrace:    true,
		},
	}

	gauge := &model.Metric{
		ID:           "metric-gauge",
		ResourceID:   resource.ID,
		ScopeID:      "scope-query",
		ScopeName:    "query-scope",
		ScopeVersion: "2.0.0",
		Name:         "process.cpu.usage",
		Unit:         "1",
		Type:         model.MetricTypeGauge,
		Series: []*model.MetricSeries{
			{
				ID: "series-gauge-a",
				DataPoints: []*model.DataPoint{
					{Timestamp: 1000, DoubleValue: f64(1.5)},
					{Timestamp: 2000, DoubleValue: f64(2.5)},
					{Timestamp: 3000, DoubleValue: f64(math.Inf(1))},
					{Timestamp: 4000, DoubleValue: &nan}, // NaN -> NULL + nan_mask
				},
			},
			{
				ID:         "series-gauge-b",
				Attributes: []model.Attribute{{Key: "host", Str: "b", Kind: model.ValueString}},
				DataPoints: []*model.DataPoint{
					{Timestamp: 1000, IntValue: i64(99)},
				},
			},
		},
	}

	hist := &model.Metric{
		ID:           "metric-hist",
		ResourceID:   resource.ID,
		ScopeID:      "scope-query",
		ScopeName:    "query-scope",
		ScopeVersion: "2.0.0",
		Name:         "http.server.request.duration",
		Unit:         "s",
		Type:         model.MetricTypeHistogram,
		Temporality:  model.TemporalityDelta,
		Series: []*model.MetricSeries{
			{
				ID: "series-hist",
				Attributes: []model.Attribute{
					{Key: "method", Str: "GET", Kind: model.ValueString},
				},
				DataPoints: []*model.DataPoint{
					{
						Timestamp:     1000,
						Count:         u64(12),
						Sum:           f64(42.5),
						Min:           f64(0.5),
						Max:           f64(9.5),
						HistogramJSON: histJSON,
						Exemplars:     exemplars,
					},
				},
			},
		},
	}

	batch := model.NewMetricBatch(2)
	batch.Resource = resource
	batch.AddMetric(gauge)
	batch.AddMetric(hist)

	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	cmd := sqlite.NewWriteMetricsCommand(batch)
	if err := cmd.Execute(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("execute: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func openStore(t *testing.T, path string) *query.Store {
	t.Helper()
	s, err := query.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSeries_Filters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.db")
	seedDB(t, path)
	store := openStore(t, path)
	ctx := context.Background()

	// All series.
	all, err := store.Series(ctx, query.SeriesFilter{})
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 series, got %d", len(all))
	}

	// By metric name.
	gauges, err := store.Series(ctx, query.SeriesFilter{MetricName: "process.cpu.usage"})
	if err != nil {
		t.Fatalf("Series by metric: %v", err)
	}
	if len(gauges) != 2 {
		t.Fatalf("expected 2 gauge series, got %d", len(gauges))
	}
	if gauges[0].Type != model.MetricTypeGauge {
		t.Errorf("type = %v", gauges[0].Type)
	}

	// By service name.
	svc, err := store.Series(ctx, query.SeriesFilter{ServiceName: "query-svc"})
	if err != nil {
		t.Fatalf("Series by service: %v", err)
	}
	if len(svc) != 3 {
		t.Fatalf("expected 3 series for query-svc, got %d", len(svc))
	}

	// By scope.
	scoped, err := store.Series(ctx, query.SeriesFilter{ScopeName: "query-scope"})
	if err != nil {
		t.Fatalf("Series by scope: %v", err)
	}
	if len(scoped) != 3 {
		t.Fatalf("expected 3 series for query-scope, got %d", len(scoped))
	}

	// Combined + limit.
	one, err := store.Series(ctx, query.SeriesFilter{
		MetricName:  "process.cpu.usage",
		ServiceName: "query-svc",
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("Series limit: %v", err)
	}
	if len(one) != 1 {
		t.Fatalf("expected 1 series, got %d", len(one))
	}
}

func TestDataPoints_TimeWindowAndNaN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.db")
	seedDB(t, path)
	store := openStore(t, path)
	ctx := context.Background()

	// Full window ascending.
	pts, err := store.DataPoints(ctx, query.DataPointFilter{
		SeriesID:  "series-gauge-a",
		Ascending: true,
	})
	if err != nil {
		t.Fatalf("DataPoints: %v", err)
	}
	if len(pts) != 4 {
		t.Fatalf("expected 4 points, got %d", len(pts))
	}
	if pts[0].TimestampNs != 1000 || pts[3].TimestampNs != 4000 {
		t.Errorf("order = %d..%d", pts[0].TimestampNs, pts[3].TimestampNs)
	}

	// Values round-trip: +Inf survives, NaN comes back as NaN via nan_mask.
	if pts[2].DoubleValue == nil || !math.IsInf(*pts[2].DoubleValue, 1) {
		t.Errorf("point 3 double = %v, want +Inf", pts[2].DoubleValue)
	}
	if pts[3].DoubleValue == nil || !math.IsNaN(*pts[3].DoubleValue) {
		t.Errorf("point 4 double = %v, want NaN", pts[3].DoubleValue)
	}

	// Time window with the series-time index.
	window, err := store.DataPoints(ctx, query.DataPointFilter{
		SeriesID: "series-gauge-a",
		FromNs:   1500,
		ToNs:     3500,
	})
	if err != nil {
		t.Fatalf("DataPoints window: %v", err)
	}
	if len(window) != 2 {
		t.Fatalf("expected 2 points in window, got %d", len(window))
	}
	if window[0].TimestampNs != 3000 || window[1].TimestampNs != 2000 {
		t.Errorf("default descending order = %d, %d", window[0].TimestampNs, window[1].TimestampNs)
	}

	// Absent value stays absent (nil), not NaN.
	if pts[0].DoubleValue == nil || *pts[0].DoubleValue != 1.5 {
		t.Errorf("point 1 double = %v", pts[0].DoubleValue)
	}

	// Int-valued series.
	ints, err := store.DataPoints(ctx, query.DataPointFilter{SeriesID: "series-gauge-b"})
	if err != nil {
		t.Fatalf("DataPoints int: %v", err)
	}
	if len(ints) != 1 || ints[0].IntValue == nil || *ints[0].IntValue != 99 {
		t.Errorf("int point = %+v", ints)
	}
}

func TestBuckets_Normalized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.db")
	seedDB(t, path)
	store := openStore(t, path)
	ctx := context.Background()

	buckets, err := store.Buckets(ctx, "series-hist", 0, 0, 0)
	if err != nil {
		t.Fatalf("Buckets: %v", err)
	}
	if len(buckets) != 4 {
		t.Fatalf("expected 4 buckets (incl. overflow), got %d", len(buckets))
	}
	wantBounds := []float64{1, 5, 10}
	wantCounts := []uint64{3, 7, 2, 9}
	for i, b := range buckets {
		if b.Index != i {
			t.Errorf("bucket %d index = %d", i, b.Index)
		}
		if b.Count != wantCounts[i] {
			t.Errorf("bucket %d count = %d, want %d", i, b.Count, wantCounts[i])
		}
		if i < len(wantBounds) {
			if b.Bound == nil || *b.Bound != wantBounds[i] {
				t.Errorf("bucket %d bound = %v, want %v", i, b.Bound, wantBounds[i])
			}
		} else {
			// Overflow bucket: implicit +Inf, numeric bound is NULL.
			if b.Bound != nil {
				t.Errorf("overflow bucket bound = %v, want nil (+Inf)", *b.Bound)
			}
			if b.BoundJSON != `"+Inf"` {
				t.Errorf("overflow bucket bound_json = %s, want %q", b.BoundJSON, `"+Inf"`)
			}
		}
		if b.SeriesID != "series-hist" || b.DataPointID == 0 {
			t.Errorf("bucket %d context = %+v", i, b)
		}
	}
}

func TestByExemplarTrace_Correlation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.db")
	seedDB(t, path)
	store := openStore(t, path)
	ctx := context.Background()

	// The seeded exemplar trace id is aabb01 followed by zero bytes.
	traceID := "aabb0100000000000000000000000000"
	hits, err := store.ByExemplarTrace(ctx, traceID, 0)
	if err != nil {
		t.Fatalf("ByExemplarTrace: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(hits))
	}
	h := hits[0]
	if h.MetricName != "http.server.request.duration" {
		t.Errorf("metric = %q", h.MetricName)
	}
	if h.TraceID != traceID {
		t.Errorf("trace = %q", h.TraceID)
	}
	if h.SpanID != "ccdd000000000000" {
		t.Errorf("span = %q", h.SpanID)
	}
	if h.ExemplarDouble == nil || *h.ExemplarDouble != 7.5 {
		t.Errorf("exemplar double = %v", h.ExemplarDouble)
	}
	if h.ExemplarTimestamp != 500 {
		t.Errorf("exemplar ts = %d", h.ExemplarTimestamp)
	}

	// Unknown trace: no hits.
	none, err := store.ByExemplarTrace(ctx, "ffffffffffffffffffffffffffffffff", 0)
	if err != nil {
		t.Fatalf("ByExemplarTrace none: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("expected no hits, got %d", len(none))
	}
}

// TestQueryConcurrentWithWriter proves WAL allows the read handle to work
// while the writer is active (the query sidecar scenario): the writer's
// connection and the query Store run against the same file simultaneously.
func TestQueryConcurrentWithWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.db")

	q := storage.NewCommandQueue(16)
	w, err := sqlite.NewWriter(q, &sqlite.WriterConfig{
		Path:          path,
		BatchSize:     10,
		FlushInterval: 20 * time.Millisecond,
		WALMode:       true,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	// Seed data through the writer, then read through the query store while
	// the writer is still running.
	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("concurrent-svc"),
	})
	resource.ID = "res-concurrent"
	batch := model.NewMetricBatch(1)
	batch.Resource = resource
	batch.AddMetric(&model.Metric{
		ID:         "metric-c",
		ResourceID: resource.ID,
		ScopeID:    "scope-c",
		ScopeName:  "c",
		Name:       "concurrent.gauge",
		Type:       model.MetricTypeGauge,
		Series: []*model.MetricSeries{
			{ID: "series-c", DataPoints: []*model.DataPoint{{Timestamp: 1, DoubleValue: f64(1)}}},
		},
	})
	if err := w.Submit(ctx, sqlite.NewWriteMetricsCommand(batch)); err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Poll until the point is visible from the read handle (writer runs
	// async); then keep reading while submitting more writes.
	store := openStore(t, path)
	deadline := time.Now().Add(5 * time.Second)
	for {
		pts, err := store.DataPoints(ctx, query.DataPointFilter{SeriesID: "series-c"})
		if err != nil {
			t.Fatalf("DataPoints: %v", err)
		}
		if len(pts) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for data point via read handle")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Write more while the read handle is open.
	for i := int64(2); i <= 5; i++ {
		b := model.NewMetricBatch(1)
		b.Resource = resource
		b.AddMetric(&model.Metric{
			ID:         "metric-c",
			ResourceID: resource.ID,
			ScopeID:    "scope-c",
			ScopeName:  "c",
			Name:       "concurrent.gauge",
			Type:       model.MetricTypeGauge,
			Series: []*model.MetricSeries{
				{ID: "series-c", DataPoints: []*model.DataPoint{{Timestamp: i, DoubleValue: f64(float64(i))}}},
			},
		})
		if err := w.Submit(ctx, sqlite.NewWriteMetricsCommand(b)); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	deadline = time.Now().Add(5 * time.Second)
	for {
		pts, err := store.DataPoints(ctx, query.DataPointFilter{SeriesID: "series-c"})
		if err != nil {
			t.Fatalf("DataPoints: %v", err)
		}
		if len(pts) == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: read %d/5 points", len(pts))
		}
		time.Sleep(20 * time.Millisecond)
	}

	w.Stop()
	w.Wait()
}
