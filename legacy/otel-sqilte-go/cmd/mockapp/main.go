// Command mockapp is a deterministic mock application used by the e2e
// verification script (scripts/e2e-mixed.sh).
//
// For the configured duration it exports BOTH OTLP logs and OTLP metrics to
// a running collector on every tick, tagging every record/point with the
// request sequence so a verifier can spot-check exact values:
//
//   - logs:    one ExportLogsServiceRequest with `logs` records per tick,
//     body "mockapp seq=<seq> idx=<i>", INFO severity, each with a
//     deterministic trace/span id
//   - metrics: one ExportMetricsServiceRequest with `points` data points
//     per tick — a gauge (uptime), a cumulative monotonic sum
//     (requests.total) and `points-2` histogram points whose
//     exemplars reference the SAME trace ids as the log records of
//     the same request (trace↔metric correlation)
//
// Every ack is counted; at the end a JSON summary of what was sent is
// printed and optionally written to -summary for the verifier to compare
// against the collector's SQLite storage.
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logsCollectorV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1"
	metricsCollectorV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/metrics/v1"
	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1"
	metricsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/metrics/v1"
	resourceV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1"
)

const (
	scopeName    = "mockapp"
	scopeVersion = "1.0.0"
	schemaURL    = "https://opentelemetry.io/schemas/1.21.0"

	gaugeMetricName     = "mockapp.uptime"
	sumMetricName       = "mockapp.requests.total"
	histogramMetricName = "mockapp.request.duration"

	// Histogram shape shared by every histogram data point.
	// (package-level vars: Go has no const slices)
)

var (
	histBounds = []float64{0.05, 0.1, 0.25, 0.5, 1.0, 2.5}
	histCounts = []uint64{1, 1, 1, 1, 1, 1, 1} // len(bounds)+1
)

// summary is the machine-readable handoff from mockapp to the verifier.
type summary struct {
	App              string   `json:"app"`
	DurationSeconds  float64  `json:"duration_seconds"`
	RequestsSent     int      `json:"requests_sent"`
	LogRecordsSent   int      `json:"log_records_sent"`
	MetricPointsSent int      `json:"metric_points_sent"`
	ExportErrors     int      `json:"export_errors"`
	Resources        []string `json:"resources"`
	ScopeName        string   `json:"scope_name"`
	ScopeVersion     string   `json:"scope_version"`
	LogsPerRequest   int      `json:"logs_per_request"`
	PointsPerRequest int      `json:"points_per_request"`
	LogMetricName    string   `json:"log_metric_name"`
	SumMetricName    string   `json:"sum_metric_name"`
	HistogramMetric  string   `json:"histogram_metric_name"`
	SampleTraceID    string   `json:"sample_trace_id"` // hex of traceID(0,0)
	LastTraceID      string   `json:"last_trace_id"`   // hex of the last trace id used
}

func main() {
	addr := flag.String("addr", "localhost:4317", "collector gRPC address")
	duration := flag.Duration("duration", 2*time.Minute, "how long to export (default 2m)")
	interval := flag.Duration("interval", 50*time.Millisecond, "tick between export rounds")
	logs := flag.Int("logs", 100, "log records per export request")
	points := flag.Int("points", 100, "metric data points per export request (>= 2)")
	resources := flag.Int("resources", 4, "number of distinct simulated services")
	summaryPath := flag.String("summary", "", "write JSON summary to this file (empty = stdout)")
	flag.Parse()

	if *logs <= 0 || *points < 2 || *resources <= 0 {
		log.Fatal("logs, points (>=2) and resources must be positive")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer func() { _ = conn.Close() }()
	logClient := logsCollectorV1.NewLogsServiceClient(conn)
	metricClient := metricsCollectorV1.NewMetricsServiceClient(conn)

	resourcesList := make([]*resourceV1.Resource, *resources)
	for i := range *resources {
		resourcesList[i] = makeResource(i)
	}
	resourceNames := make([]string, *resources)
	for i := range *resources {
		resourceNames[i] = fmt.Sprintf("mockapp-%d", i)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	start := time.Now()
	seq := 0
	var sentLogs, sentPoints, exportErrors int

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			goto done
		case <-ticker.C:
			if err := exportRound(logClient, metricClient, resourcesList, seq, *logs, *points); err != nil {
				exportErrors++
				log.Printf("export round %d failed: %v", seq, err)
				continue
			}
			sentLogs += *logs
			sentPoints += *points
			seq++
		}
	}

done:
	elapsed := time.Since(start).Seconds()
	s := summary{
		App:              "mockapp",
		DurationSeconds:  elapsed,
		RequestsSent:     seq,
		LogRecordsSent:   sentLogs,
		MetricPointsSent: sentPoints,
		ExportErrors:     exportErrors,
		Resources:        resourceNames,
		ScopeName:        scopeName,
		ScopeVersion:     scopeVersion,
		LogsPerRequest:   *logs,
		PointsPerRequest: *points,
		LogMetricName:    gaugeMetricName,
		SumMetricName:    sumMetricName,
		HistogramMetric:  histogramMetricName,
		SampleTraceID:    hexTraceID(traceID(0, 0)),
		LastTraceID:      hexTraceID(traceID(seq-1, 0)),
	}

	out := os.Stdout
	if *summaryPath != "" {
		f, err := os.Create(*summaryPath)
		if err != nil {
			log.Fatalf("create summary: %v", err) //nolint:gocritic // ticker.Stop is best-effort on exit
		}
		defer func() { _ = f.Close() }()
		out = f
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(s); err != nil {
		log.Fatalf("write summary: %v", err) //nolint:gocritic // ticker.Stop is best-effort on exit
	}

	log.Printf("mockapp done: %d requests (%d log records, %d metric points) in %.1fs, %d export errors",
		seq, sentLogs, sentPoints, elapsed, exportErrors)
	if exportErrors > 0 {
		os.Exit(1)
	}
}

// exportRound sends one log request and one metric request for the given
// sequence. Log record idx=i and histogram exemplar idx=i share the same
// trace id so the verifier can prove trace↔metric correlation.
//
// Each round gets a fresh context: the duration context may expire mid-round
// (the last tick races the deadline) and must not cancel in-flight exports.
func exportRound(
	logClient logsCollectorV1.LogsServiceClient,
	metricClient metricsCollectorV1.MetricsServiceClient,
	resources []*resourceV1.Resource,
	seq, logs, points int,
) error {
	resource := resources[seq%len(resources)]

	logReq := buildLogRequest(resource, seq, logs)
	logCtx, cancelLog := context.WithTimeout(context.Background(), 10*time.Second)
	_, errLog := logClient.Export(logCtx, logReq)
	cancelLog()
	if errLog != nil {
		return fmt.Errorf("log export seq=%d: %w", seq, errLog)
	}

	metricReq := buildMetricRequest(resource, seq, points)
	metricCtx, cancelMetric := context.WithTimeout(context.Background(), 10*time.Second)
	_, errMetric := metricClient.Export(metricCtx, metricReq)
	cancelMetric()
	if errMetric != nil {
		return fmt.Errorf("metric export seq=%d: %w", seq, errMetric)
	}
	return nil
}

func buildLogRequest(resource *resourceV1.Resource, seq, logs int) *logsCollectorV1.ExportLogsServiceRequest {
	now := uint64(time.Now().UnixNano()) //nolint:gosec // G115: timestamp fits
	records := make([]*logsV1.LogRecord, logs)
	for i := range logs {
		ui := uint64(i) //nolint:gosec // G115: loop counter
		tid := traceID(seq, i)
		sid := spanID(seq, i)
		records[i] = &logsV1.LogRecord{
			TimeUnixNano:         now + ui,
			ObservedTimeUnixNano: now,
			SeverityNumber:       logsV1.SeverityNumber_SEVERITY_NUMBER_INFO,
			SeverityText:         "INFO",
			Body: &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{
				StringValue: fmt.Sprintf("mockapp seq=%d idx=%d", seq, i),
			}},
			TraceId: tid[:],
			SpanId:  sid[:],
			Attributes: []*commonV1.KeyValue{
				{Key: "seq", Value: &commonV1.AnyValue{Value: &commonV1.AnyValue_IntValue{IntValue: int64(seq)}}}, //nolint:gosec,lll // G115: bounded
				{Key: "idx", Value: &commonV1.AnyValue{Value: &commonV1.AnyValue_IntValue{IntValue: int64(i)}}},   //nolint:gosec,lll // G115: bounded
				{Key: "env", Value: &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: "e2e"}}},
			},
			DroppedAttributesCount: 0,
			Flags:                  1,
			EventName:              "mockapp.event",
		}
	}

	return &logsCollectorV1.ExportLogsServiceRequest{
		ResourceLogs: []*logsV1.ResourceLogs{
			{
				// Same schema URL as the metric path: resource identity hashes
				// (service.name, host.name, schema_url), so a mismatch would
				// create duplicate log_resource rows for one service.
				Resource:  resource,
				SchemaUrl: schemaURL,
				ScopeLogs: []*logsV1.ScopeLogs{
					{
						Scope:      &commonV1.InstrumentationScope{Name: scopeName, Version: scopeVersion},
						LogRecords: records,
					},
				},
			},
		},
	}
}

func buildMetricRequest(
	resource *resourceV1.Resource, seq, points int,
) *metricsCollectorV1.ExportMetricsServiceRequest {
	now := uint64(time.Now().UnixNano()) //nolint:gosec // G115: timestamp fits
	elapsed := time.Since(mockStart).Seconds()

	// Gauge: one point.
	gaugePoint := &metricsV1.NumberDataPoint{
		TimeUnixNano: now,
		Value:        &metricsV1.NumberDataPoint_AsDouble{AsDouble: elapsed},
	}

	// Sum: one cumulative monotonic point.
	sumPoint := &metricsV1.NumberDataPoint{
		TimeUnixNano: now,
		Value:        &metricsV1.NumberDataPoint_AsInt{AsInt: int64(seq + 1)}, //nolint:gosec // G115: bounded
	}

	// Histogram: points-2 data points, each with an exemplar sharing the
	// trace id of log record (i % logs) of the same request.
	histPoints := make([]*metricsV1.HistogramDataPoint, 0, points-2)
	for j := range points - 2 {
		ui := uint64(j) //nolint:gosec // G115: loop counter
		// Deterministic duration sample: 5..99 ms.
		ms := float64(5 + j%95) //nolint:gosec // G115: bounded
		tid := traceID(seq, j)
		sid := spanID(seq, j)
		histPoints = append(histPoints, &metricsV1.HistogramDataPoint{
			TimeUnixNano:   now + ui,
			Count:          1,
			Sum:            &ms,
			ExplicitBounds: histBounds,
			BucketCounts:   histCounts,
			Exemplars: []*metricsV1.Exemplar{
				{
					TimeUnixNano: now + ui,
					TraceId:      tid[:],
					SpanId:       sid[:],
					Value:        &metricsV1.Exemplar_AsDouble{AsDouble: ms},
					FilteredAttributes: []*commonV1.KeyValue{
						{Key: "seq", Value: &commonV1.AnyValue{Value: &commonV1.AnyValue_IntValue{IntValue: int64(seq)}}}, //nolint:gosec,lll // G115: bounded
					},
				},
			},
		})
	}

	metrics := []*metricsV1.Metric{
		{
			Name: gaugeMetricName,
			Unit: "s",
			Data: &metricsV1.Metric_Gauge{
				Gauge: &metricsV1.Gauge{DataPoints: []*metricsV1.NumberDataPoint{gaugePoint}},
			},
		},
		{
			Name: sumMetricName,
			Unit: "1",
			Data: &metricsV1.Metric_Sum{Sum: &metricsV1.Sum{
				IsMonotonic:            true,
				AggregationTemporality: metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				DataPoints:             []*metricsV1.NumberDataPoint{sumPoint},
			}},
		},
		{
			Name: histogramMetricName,
			Unit: "ms",
			Data: &metricsV1.Metric_Histogram{Histogram: &metricsV1.Histogram{
				AggregationTemporality: metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
				DataPoints:             histPoints,
			}},
		},
	}

	return &metricsCollectorV1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsV1.ResourceMetrics{
			{
				Resource:  resource,
				SchemaUrl: schemaURL,
				ScopeMetrics: []*metricsV1.ScopeMetrics{
					{
						Scope:   &commonV1.InstrumentationScope{Name: scopeName, Version: scopeVersion},
						Metrics: metrics,
					},
				},
			},
		},
	}
}

var mockStart = time.Now()

// hexTraceID returns the lowercase hex encoding of a trace id.
func hexTraceID(id [16]byte) string {
	return hex.EncodeToString(id[:])
}

// traceID returns a deterministic 16-byte trace id: seq (BE, 8 bytes)
// followed by idx (BE, 8 bytes).
func traceID(seq, idx int) [16]byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(seq))  //nolint:gosec // G115: bounded
	binary.BigEndian.PutUint64(b[8:16], uint64(idx)) //nolint:gosec // G115: bounded
	return b
}

// spanID returns a deterministic 8-byte span id derived from seq and idx.
func spanID(seq, idx int) [8]byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(seq)*1_000_003+uint64(idx)) //nolint:gosec // G115: bounded
	return b
}

func makeResource(i int) *resourceV1.Resource {
	strVal := func(s string) *commonV1.AnyValue {
		return &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: s}}
	}
	return &resourceV1.Resource{
		Attributes: []*commonV1.KeyValue{
			{Key: "service.name", Value: strVal(fmt.Sprintf("mockapp-%d", i))},
			{Key: "host.name", Value: strVal(fmt.Sprintf("host-%d", i%4))},
			{Key: "deployment.environment", Value: strVal("e2e")},
		},
	}
}
