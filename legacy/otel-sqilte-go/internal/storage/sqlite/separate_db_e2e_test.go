package sqlite

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/query"
	"codeberg.org/nicknad/otel-sqlite/internal/testutil"

	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1"
	metricsCollectorV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/metrics/v1"
	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	logsPB "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1"
	metricsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/metrics/v1"
	resourceV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1"
)

// TestE2E_OTLPMetricsSeparateDB is the separate-DB sibling of
// TestE2E_OTLPMetricsToSQLite: metrics land in their own SQLite database
// (own writer + batcher pipeline) while logs keep using the log database.
// It asserts:
//   - two DB files exist after ingestion;
//   - metric rows are in the metrics file and visible through query.Open;
//   - log rows are in the log file;
//   - neither signal leaked into the other's file.
func TestE2E_OTLPMetricsSeparateDB(t *testing.T) {
	dir := t.TempDir()
	logPath := dir + "/logs.db"
	metricsPath := dir + "/metrics.db"

	// ---- log pipeline: ingress → log batcher → log writer (logPath) ----
	logP := startLogPipeline(t, logPath)

	// ---- metric pipeline: ingress → metric batcher → metric writer (metricsPath) ----
	metricP := startMetricPipeline(t, metricsPath)

	// ---- send: one log record ----
	now := uint64(time.Now().UnixNano())
	if _, err := logP.server.Export(context.Background(), &logsV1.ExportLogsServiceRequest{
		ResourceLogs: []*logsPB.ResourceLogs{
			{
				Resource: &resourceV1.Resource{
					Attributes: []*commonV1.KeyValue{
						{Key: "service.name", Value: &commonV1.AnyValue{
							Value: &commonV1.AnyValue_StringValue{StringValue: "separate-db-api"},
						}},
					},
				},
				ScopeLogs: []*logsPB.ScopeLogs{
					{
						Scope: &commonV1.InstrumentationScope{Name: "e2e-scope", Version: "0.1.0"},
						LogRecords: []*logsPB.LogRecord{
							{
								TimeUnixNano:         now,
								ObservedTimeUnixNano: now,
								SeverityNumber:       logsPB.SeverityNumber_SEVERITY_NUMBER_INFO,
								SeverityText:         "INFO",
								Body: &commonV1.AnyValue{
									Value: &commonV1.AnyValue_StringValue{StringValue: "log stays in the log db"},
								},
							},
						},
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("Export logs: %v", err)
	}

	// ---- send: one metric data point ----
	if _, err := metricP.server.Export(context.Background(), &metricsCollectorV1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsV1.ResourceMetrics{
			{
				Resource: &resourceV1.Resource{
					Attributes: []*commonV1.KeyValue{
						{Key: "service.name", Value: &commonV1.AnyValue{
							Value: &commonV1.AnyValue_StringValue{StringValue: "separate-db-api"},
						}},
					},
				},
				ScopeMetrics: []*metricsV1.ScopeMetrics{
					{
						Scope: &commonV1.InstrumentationScope{Name: "e2e-scope", Version: "0.1.0"},
						Metrics: []*metricsV1.Metric{
							{
								Name: "separate.db.gauge",
								Unit: "1",
								Data: &metricsV1.Metric_Gauge{Gauge: &metricsV1.Gauge{
									DataPoints: []*metricsV1.NumberDataPoint{
										{TimeUnixNano: now, Value: &metricsV1.NumberDataPoint_AsDouble{AsDouble: 7.5}},
									},
								}},
							},
						},
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("Export metrics: %v", err)
	}

	// ---- wait for both pipelines to drain, then stop the writers ----
	logP.stop()
	metricP.stop()

	// ---- assert: two DB files exist ----
	for _, p := range []string{logPath, metricsPath} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("expected DB file %s to exist: %v", p, err)
		}
	}

	// ---- metrics file: metric rows present and visible through query.Open ----
	q, err := query.Open(metricsPath)
	if err != nil {
		t.Fatalf("query.Open(metricsPath): %v", err)
	}
	defer func() { _ = q.Close() }()

	series, err := q.Series(context.Background(), query.SeriesFilter{
		MetricName:  "separate.db.gauge",
		ServiceName: "separate-db-api",
	})
	if err != nil {
		t.Fatalf("query series in metrics db: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("metrics db series = %d, want 1 (data point landed in the metrics file)", len(series))
	}
	if series[0].DataPointCount != 1 {
		t.Errorf("metrics db data point count = %d, want 1", series[0].DataPointCount)
	}
	if series[0].ServiceName != "separate-db-api" {
		t.Errorf("metrics db service = %q, want separate-db-api", series[0].ServiceName)
	}

	// ---- metrics file: no log rows leaked in ----
	assertDBCount(t, metricsPath, "log_event", 0)

	// ---- log file: log rows present, no metric rows leaked in ----
	assertDBCount(t, logPath, "log_event", 1)
	assertDBCount(t, logPath, "metric_data_point", 0)
}

// assertDBCount opens path and asserts COUNT(*) on table.
func assertDBCount(t *testing.T, path, table string, want int) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = db.Close() }()
	testutil.AssertTableCount(t, db, table, want)
}
