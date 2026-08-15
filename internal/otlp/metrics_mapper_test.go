package otlp

import (
	"encoding/json"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"

	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	metricsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/metrics/v1"
	resourceV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1"
)

func strValue(s string) *commonV1.AnyValue {
	return &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: s}}
}

func intValue(i int64) *commonV1.AnyValue {
	return &commonV1.AnyValue{Value: &commonV1.AnyValue_IntValue{IntValue: i}}
}

func buildResourceMetrics() *metricsV1.ResourceMetrics {
	now := uint64(time.Now().UnixNano())
	return &metricsV1.ResourceMetrics{
		Resource: &resourceV1.Resource{
			Attributes: []*commonV1.KeyValue{
				{Key: "service.name", Value: strValue("api")},
				{Key: "host.name", Value: strValue("host-1")},
			},
		},
		SchemaUrl: "https://opentelemetry.io/schemas/1.21.0",
		ScopeMetrics: []*metricsV1.ScopeMetrics{
			{
				Scope: &commonV1.InstrumentationScope{Name: "test-scope", Version: "1.0.0"},
				Metrics: []*metricsV1.Metric{
					{
						Name:        "process.memory.usage",
						Description: "memory usage",
						Unit:        "By",
						Data: &metricsV1.Metric_Gauge{Gauge: &metricsV1.Gauge{
							DataPoints: []*metricsV1.NumberDataPoint{
								{TimeUnixNano: now, Value: &metricsV1.NumberDataPoint_AsDouble{AsDouble: 123}},
								{
									TimeUnixNano: now + 1,
									Attributes:   []*commonV1.KeyValue{{Key: "pid", Value: intValue(1)}},
									Value:        &metricsV1.NumberDataPoint_AsDouble{AsDouble: 456},
								},
								{
									TimeUnixNano: now + 2,
									Attributes:   []*commonV1.KeyValue{{Key: "pid", Value: intValue(1)}},
									Value:        &metricsV1.NumberDataPoint_AsDouble{AsDouble: 789},
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
								{TimeUnixNano: now, Value: &metricsV1.NumberDataPoint_AsInt{AsInt: 42}},
							},
						}},
					},
					{
						Name: "http.server.request.duration",
						Unit: "s",
						Data: &metricsV1.Metric_Histogram{Histogram: &metricsV1.Histogram{
							AggregationTemporality: metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
							DataPoints: []*metricsV1.HistogramDataPoint{
								{
									TimeUnixNano: now, Count: 3, Sum: float64ptr(1.5),
									ExplicitBounds: []float64{0.1, 1.0},
									BucketCounts:   []uint64{1, 1, 1},
								},
							},
						}},
					},
					{
						Name: "process.cpu.time",
						Data: &metricsV1.Metric_Summary{Summary: &metricsV1.Summary{
							DataPoints: []*metricsV1.SummaryDataPoint{
								{
									TimeUnixNano: now, Count: 10, Sum: 5,
									QuantileValues: []*metricsV1.SummaryDataPoint_ValueAtQuantile{
										{Quantile: 0.5, Value: 0.25},
									},
								},
							},
						}},
					},
				},
			},
		},
	}
}

func float64ptr(v float64) *float64 { return &v }

func TestMapMetricsData_GaugeAndSum(t *testing.T) {
	m := NewMapper()
	batches := m.MapMetricsData([]*metricsV1.ResourceMetrics{buildResourceMetrics()})
	if len(batches) != 1 {
		t.Fatalf("got %d batches, want 1", len(batches))
	}
	batch := batches[0]

	if batch.Resource == nil || batch.Resource.ID == "" {
		t.Error("resource ID must be set (shared dedup scheme with logs)")
	}
	if batch.SchemaURL != "https://opentelemetry.io/schemas/1.21.0" {
		t.Errorf("schema URL = %q", batch.SchemaURL)
	}
	if got := batch.Size(); got != 6 {
		t.Errorf("batch size = %d data points, want 6", got)
	}

	if len(batch.Metrics) != 4 {
		t.Fatalf("got %d metrics, want 4", len(batch.Metrics))
	}

	// Gauge: two series (empty attributes, pid=1), 1 + 2 points.
	gauge := batch.Metrics[0]
	if gauge.Type != model.MetricTypeGauge {
		t.Errorf("gauge type = %v", gauge.Type)
	}
	if len(gauge.Series) != 2 {
		t.Fatalf("gauge series = %d, want 2", len(gauge.Series))
	}
	var emptySeries, pidSeries *model.MetricSeries
	for _, s := range gauge.Series {
		if len(s.Attributes) == 0 {
			emptySeries = s
		} else {
			pidSeries = s
		}
	}
	if emptySeries == nil || len(emptySeries.DataPoints) != 1 {
		t.Errorf("empty-attr series must have 1 point")
	}
	if pidSeries == nil || len(pidSeries.DataPoints) != 2 {
		t.Errorf("pid series must have 2 points")
	}
	if pidSeries.DataPoints[0].DoubleValue == nil || *pidSeries.DataPoints[0].DoubleValue != 456 {
		t.Errorf("pid series first point = %v", pidSeries.DataPoints[0].DoubleValue)
	}

	// Sum: temporality + monotonicity preserved; int value kept.
	sum := batch.Metrics[1]
	if sum.Type != model.MetricTypeSum || !sum.IsMonotonic {
		t.Errorf("sum type/monotonic = %v/%v", sum.Type, sum.IsMonotonic)
	}
	if sum.Temporality != model.TemporalityCumulative {
		t.Errorf("sum temporality = %v, want cumulative", sum.Temporality)
	}
	if len(sum.Series) != 1 || sum.Series[0].DataPoints[0].IntValue == nil || *sum.Series[0].DataPoints[0].IntValue != 42 {
		t.Errorf("sum int value not preserved")
	}
}

func TestMapMetricsData_HistogramAndSummary(t *testing.T) {
	m := NewMapper()
	batches := m.MapMetricsData([]*metricsV1.ResourceMetrics{buildResourceMetrics()})
	batch := batches[0]

	hist := batch.Metrics[2]
	if hist.Type != model.MetricTypeHistogram || hist.Temporality != model.TemporalityDelta {
		t.Errorf("histogram type/temporality = %v/%v", hist.Type, hist.Temporality)
	}
	hp := hist.Series[0].DataPoints[0]
	if hp.Count == nil || *hp.Count != 3 || hp.Sum == nil || *hp.Sum != 1.5 {
		t.Errorf("histogram count/sum = %v/%v", hp.Count, hp.Sum)
	}
	var payload struct {
		Bounds []float64 `json:"bounds"`
		Counts []uint64  `json:"counts"`
	}
	if err := json.Unmarshal(hp.HistogramJSON, &payload); err != nil {
		t.Fatalf("histogram JSON: %v", err)
	}
	if len(payload.Bounds) != 2 || len(payload.Counts) != 3 || payload.Counts[2] != 1 {
		t.Errorf("histogram payload = %s", hp.HistogramJSON)
	}

	summary := batch.Metrics[3]
	if summary.Type != model.MetricTypeSummary {
		t.Errorf("summary type = %v", summary.Type)
	}
	sp := summary.Series[0].DataPoints[0]
	if sp.Count == nil || *sp.Count != 10 || sp.Sum == nil || *sp.Sum != 5 {
		t.Errorf("summary count/sum = %v/%v", sp.Count, sp.Sum)
	}
	var summaryPayload struct {
		Quantiles []struct {
			Quantile float64 `json:"quantile"`
			Value    float64 `json:"value"`
		} `json:"quantiles"`
	}
	if err := json.Unmarshal(sp.SummaryJSON, &summaryPayload); err != nil {
		t.Fatalf("summary JSON: %v", err)
	}
	if len(summaryPayload.Quantiles) != 1 || summaryPayload.Quantiles[0].Quantile != 0.5 {
		t.Errorf("summary payload = %s", sp.SummaryJSON)
	}
}

func TestMapMetricsData_DeterministicIDs(t *testing.T) {
	m := NewMapper()
	b1 := m.MapMetricsData([]*metricsV1.ResourceMetrics{buildResourceMetrics()})[0]
	b2 := m.MapMetricsData([]*metricsV1.ResourceMetrics{buildResourceMetrics()})[0]

	if b1.Resource.ID != b2.Resource.ID {
		t.Error("resource IDs must be deterministic")
	}
	for i := range b1.Metrics {
		if b1.Metrics[i].ID != b2.Metrics[i].ID {
			t.Errorf("metric %d IDs differ: %s vs %s", i, b1.Metrics[i].ID, b2.Metrics[i].ID)
		}
		if b1.Metrics[i].ScopeID != b2.Metrics[i].ScopeID {
			t.Errorf("metric %d scope IDs differ", i)
		}
		for j := range b1.Metrics[i].Series {
			if b1.Metrics[i].Series[j].ID != b2.Metrics[i].Series[j].ID {
				t.Errorf("metric %d series %d IDs differ", i, j)
			}
		}
	}
}

func TestMapMetricsData_ExemplarsPreserved(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	rm := &metricsV1.ResourceMetrics{
		Resource: &resourceV1.Resource{},
		ScopeMetrics: []*metricsV1.ScopeMetrics{{
			Scope: &commonV1.InstrumentationScope{Name: "s"},
			Metrics: []*metricsV1.Metric{{
				Name: "http.request.duration",
				Data: &metricsV1.Metric_Gauge{Gauge: &metricsV1.Gauge{
					DataPoints: []*metricsV1.NumberDataPoint{{
						TimeUnixNano: now,
						Value:        &metricsV1.NumberDataPoint_AsDouble{AsDouble: 0.35},
						Exemplars: []*metricsV1.Exemplar{{
							TimeUnixNano: now,
							TraceId:      traceID,
							SpanId:       spanID,
							Value:        &metricsV1.Exemplar_AsDouble{AsDouble: 0.35},
							FilteredAttributes: []*commonV1.KeyValue{
								{Key: "http.method", Value: strValue("GET")},
							},
						}},
					}},
				}},
			}},
		}},
	}

	m := NewMapper()
	batch := m.MapMetricsData([]*metricsV1.ResourceMetrics{rm})[0]
	exemplars := batch.Metrics[0].Series[0].DataPoints[0].Exemplars
	if len(exemplars) != 1 {
		t.Fatalf("exemplars = %d, want 1", len(exemplars))
	}
	e := exemplars[0]
	if !e.HasTrace || e.TraceID != [16]byte(traceID) || e.SpanID != [8]byte(spanID) {
		t.Error("exemplar trace/span IDs not preserved")
	}
	if e.DoubleValue == nil || *e.DoubleValue != 0.35 {
		t.Error("exemplar value not preserved")
	}
	if len(e.Attributes) != 1 || e.Attributes[0].Key != "http.method" {
		t.Error("exemplar filtered attributes not preserved")
	}
}

func TestMapMetricsData_EmptyInputs(t *testing.T) {
	m := NewMapper()
	if got := m.MapMetricsData(nil); got != nil {
		t.Errorf("nil input returned %d batches", len(got))
	}
	if got := m.MapMetricsData([]*metricsV1.ResourceMetrics{nil}); len(got) != 0 {
		t.Errorf("nil ResourceMetrics returned %d batches, want 0", len(got))
	}
	// Metric without a data oneof must be dropped, not crash.
	rm := &metricsV1.ResourceMetrics{
		Resource: &resourceV1.Resource{},
		ScopeMetrics: []*metricsV1.ScopeMetrics{{
			Scope:   &commonV1.InstrumentationScope{Name: "s"},
			Metrics: []*metricsV1.Metric{{Name: "no-data"}},
		}},
	}
	if got := m.MapMetricsData([]*metricsV1.ResourceMetrics{rm}); len(got) != 0 {
		t.Errorf("metric without data produced %d batches, want 0", len(got))
	}
}
