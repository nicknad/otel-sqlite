package sqlite

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/otlp"
	"codeberg.org/nicknad/otel-sqlite/internal/testutil"

	metricsCollectorV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/metrics/v1"
	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	metricsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/metrics/v1"
	resourceV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1"
)

// TestE2E_OTLPMetricsToSQLite exercises the full metrics ingestion pipeline:
// OTLP gRPC handler → mapper → metric ingress queue → metric batcher →
// shared command queue → SQLite writer. After ingestion it queries the
// database directly and asserts that scope/metric/series/data point rows are
// persisted with the correct values and identity.
func TestE2E_OTLPMetricsToSQLite(t *testing.T) {
	dbPath := t.TempDir() + "/metrics-e2e.db"

	// Full pipeline: metric ingress queue → metric batcher → shared cmd queue → writer.
	p := startMetricPipeline(t, dbPath)

	// ---- send: OTLP ExportMetrics request ----
	now := uint64(time.Now().UnixNano())
	req := &metricsCollectorV1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsV1.ResourceMetrics{
			{
				Resource: &resourceV1.Resource{
					Attributes: []*commonV1.KeyValue{
						{Key: "service.name", Value: &commonV1.AnyValue{
							Value: &commonV1.AnyValue_StringValue{StringValue: "metrics-api"},
						}},
						{Key: "host.name", Value: &commonV1.AnyValue{
							Value: &commonV1.AnyValue_StringValue{StringValue: "host-2"},
						}},
					},
				},
				SchemaUrl: "https://opentelemetry.io/schemas/1.21.0",
				ScopeMetrics: []*metricsV1.ScopeMetrics{
					{
						Scope: &commonV1.InstrumentationScope{Name: "e2e-scope", Version: "0.1.0"},
						Metrics: []*metricsV1.Metric{
							{
								Name: "process.memory.usage",
								Unit: "By",
								Data: &metricsV1.Metric_Gauge{Gauge: &metricsV1.Gauge{
									DataPoints: []*metricsV1.NumberDataPoint{
										{TimeUnixNano: now, Value: &metricsV1.NumberDataPoint_AsDouble{AsDouble: 12345}},
										{
											TimeUnixNano: now + 1,
											Attributes: []*commonV1.KeyValue{
												{Key: "process.pid", Value: &commonV1.AnyValue{
													Value: &commonV1.AnyValue_IntValue{IntValue: 42},
												}},
											},
											Value: &metricsV1.NumberDataPoint_AsDouble{AsDouble: 54321},
										},
									},
								}},
							},
							{
								Name: "http.server.requests",
								Data: &metricsV1.Metric_Sum{Sum: &metricsV1.Sum{
									IsMonotonic:            true,
									AggregationTemporality: metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									DataPoints: []*metricsV1.NumberDataPoint{
										{
											TimeUnixNano: now,
											Attributes: []*commonV1.KeyValue{
												{Key: "http.method", Value: &commonV1.AnyValue{
													Value: &commonV1.AnyValue_StringValue{StringValue: "GET"},
												}},
											},
											Value: &metricsV1.NumberDataPoint_AsInt{AsInt: 7},
										},
									},
								}},
							},
						},
					},
				},
			},
		},
	}

	if _, err := p.server.Export(context.Background(), req); err != nil {
		t.Fatalf("ExportMetrics: %v", err)
	}

	// ---- wait for pipeline to drain ----
	p.stop()

	// ---- assert: open DB read-only and verify everything ----
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// 1. Resource row (shared with the log path).
	var svcName string
	if err := db.QueryRow(
		"SELECT service_name FROM log_resource WHERE service_name = ?", "metrics-api",
	).Scan(&svcName); err != nil {
		t.Fatalf("query resource: %v", err)
	}

	// 2. Scope row.
	var scopeID string
	if err := db.QueryRow(
		"SELECT id FROM scope WHERE name = ? AND version = ?", "e2e-scope", "0.1.0",
	).Scan(&scopeID); err != nil {
		t.Fatalf("query scope: %v", err)
	}

	// 3. Metric rows (2 definitions).
	testutil.AssertTableCount(t, db, "metric", 2)
	var unit string
	var typeVal int
	if err := db.QueryRow(
		"SELECT unit, type FROM metric WHERE name = ?", "process.memory.usage",
	).Scan(&unit, &typeVal); err != nil {
		t.Fatalf("query metric: %v", err)
	}
	if unit != "By" || typeVal != int(model.MetricTypeGauge) {
		t.Errorf("gauge metric = unit %q type %d", unit, typeVal)
	}
	var monotonic, temporality int
	if err := db.QueryRow(
		"SELECT is_monotonic, aggregation_temporality FROM metric WHERE name = ?", "http.server.requests",
	).Scan(&monotonic, &temporality); err != nil {
		t.Fatalf("query sum metric: %v", err)
	}
	if monotonic != 1 || temporality != int(model.TemporalityCumulative) {
		t.Errorf("sum metadata = monotonic:%d temporality:%d", monotonic, temporality)
	}

	// 4. Series rows: gauge has 2 series, sum has 1.
	testutil.AssertTableCount(t, db, "metric_series", 3)
	var seriesID string
	if err := db.QueryRow(
		"SELECT ms.id FROM metric_series ms JOIN metric m ON m.id = ms.metric_id WHERE m.name = ? AND ms.attributes_json = ?",
		"process.memory.usage", `{"process.pid":42}`,
	).Scan(&seriesID); err != nil {
		t.Fatalf("query series: %v", err)
	}

	// 5. Data points: 3 total, values preserved.
	testutil.AssertTableCount(t, db, "metric_data_point", 3)
	var doubleVal float64
	if err := db.QueryRow(
		"SELECT double_value FROM metric_data_point WHERE series_id = ? AND timestamp_ns = ?",
		seriesID, now+1,
	).Scan(&doubleVal); err != nil {
		t.Fatalf("query data point: %v", err)
	}
	if doubleVal != 54321 {
		t.Errorf("double_value = %v, want 54321", doubleVal)
	}
	var intVal int64
	if err := db.QueryRow(
		"SELECT dp.int_value FROM metric_data_point dp JOIN metric_series ms ON ms.id = dp.series_id JOIN metric m ON m.id = ms.metric_id WHERE m.name = ?",
		"http.server.requests",
	).Scan(&intVal); err != nil {
		t.Fatalf("query int data point: %v", err)
	}
	if intVal != 7 {
		t.Errorf("int_value = %d, want 7", intVal)
	}

	t.Logf("E2E metrics: %d metrics, %d series, %d data points persisted",
		2, 3, 3)
}

// TestE2E_MetricsDisabled returns Unavailable when metric ingestion is off.
func TestE2E_MetricsDisabled(t *testing.T) {
	svr := otlp.NewMetricsServer(nil, nil)
	_, err := svr.Export(context.Background(), &metricsCollectorV1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsV1.ResourceMetrics{{}},
	})
	if err == nil {
		t.Fatal("expected Unavailable when metric ingestion is disabled, got nil")
	}
}

// TestE2E_NilResourceMetricsStayVisible guards the fix for resource-less
// senders: OTLP allows ResourceMetrics with Resource unset. The mapper gives
// such batches a deterministic resource ID, so the scope row references a
// real log_resource row and the data points remain visible through the
// read-side `metrics` view. Before the fix, scope.resource_id was empty, the
// view's inner join to log_resource dropped every row, and the points were
// written but permanently unqueryable.
func TestE2E_NilResourceMetricsStayVisible(t *testing.T) {
	dbPath := t.TempDir() + "/nil-resource.db"

	p := startMetricPipeline(t, dbPath)

	// Resource is nil — the spec-legal "no resource info is known" case.
	now := uint64(time.Now().UnixNano())
	req := &metricsCollectorV1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsV1.ResourceMetrics{
			{
				Resource: nil,
				ScopeMetrics: []*metricsV1.ScopeMetrics{
					{
						Scope: &commonV1.InstrumentationScope{Name: "nil-e2e-scope", Version: "0.1.0"},
						Metrics: []*metricsV1.Metric{
							{
								Name: "nil.resource.gauge",
								Data: &metricsV1.Metric_Gauge{Gauge: &metricsV1.Gauge{
									DataPoints: []*metricsV1.NumberDataPoint{
										{TimeUnixNano: now, Value: &metricsV1.NumberDataPoint_AsDouble{AsDouble: 42.5}},
									},
								}},
							},
						},
					},
				},
			},
		},
	}
	if _, err := p.server.Export(context.Background(), req); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Let the pipeline flush + write.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var n int
		if err := writerDBQuery(dbPath, "SELECT COUNT(*) FROM metric_data_point", &n); err != nil {
			t.Fatalf("query data point count: %v", err)
		}
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for data point to be written (count=%d)", n)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The write must be internally consistent: no scope row referencing a
	// missing resource (the invariant the e2e verifier checks).
	var orphans int
	if err := writerDBQuery(dbPath, `SELECT COUNT(*) FROM scope s LEFT JOIN log_resource r ON r.id = s.resource_id WHERE r.id IS NULL`, &orphans); err != nil {
		t.Fatalf("query orphan scopes: %v", err)
	}
	if orphans != 0 {
		t.Errorf("found %d scope rows referencing a missing resource", orphans)
	}

	// And the data point must be VISIBLE through the read-side `metrics` view
	// (the actual regression this test guards).
	var visible int
	if err := writerDBQuery(dbPath, `SELECT COUNT(*) FROM metrics WHERE metric_name = 'nil.resource.gauge'`, &visible); err != nil {
		t.Fatalf("query metrics view: %v", err)
	}
	if visible != 1 {
		t.Errorf("data point invisible to `metrics` view: visible=%d, want 1", visible)
	}
}

// writerDBQuery opens a short-lived read-only connection to the writer's
// database file. WAL mode permits concurrent readers next to the writer.
func writerDBQuery(dbPath, query string, dst ...any) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return db.QueryRow(query).Scan(dst...)
}
