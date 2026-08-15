package otlp

import (
	"encoding/json"
	"math"
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

// sumMetric builds an OTLP Sum metric with a single data point.
func sumMetric(name string, temporality metricsV1.AggregationTemporality, monotonic bool) *metricsV1.Metric {
	return &metricsV1.Metric{
		Name: name,
		Data: &metricsV1.Metric_Sum{Sum: &metricsV1.Sum{
			IsMonotonic:            monotonic,
			AggregationTemporality: temporality,
			DataPoints: []*metricsV1.NumberDataPoint{
				{TimeUnixNano: uint64(time.Now().UnixNano()), Value: &metricsV1.NumberDataPoint_AsInt{AsInt: 1}},
			},
		}},
	}
}

// TestMapMetricsData_IdentityIncludesTemporalityAndMonotonicity guards the
// metric identity hash: sums with the same name/unit/type but different
// temporality or monotonicity are distinct metrics (and therefore distinct
// series). Without this, INSERT OR IGNORE keeps the first definition and
// merges both streams into one series.
func TestMapMetricsData_IdentityIncludesTemporalityAndMonotonicity(t *testing.T) {
	m := NewMapper()

	build := func() *metricsV1.ResourceMetrics {
		return &metricsV1.ResourceMetrics{
			Resource: &resourceV1.Resource{
				Attributes: []*commonV1.KeyValue{{Key: "service.name", Value: strValue("id-svc")}},
			},
			ScopeMetrics: []*metricsV1.ScopeMetrics{{
				Scope: &commonV1.InstrumentationScope{Name: "id-scope"},
				Metrics: []*metricsV1.Metric{
					sumMetric("requests", metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA, true),
					sumMetric("requests", metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, true),
					sumMetric("requests", metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, false),
				},
			}},
		}
	}

	ids := func(batches []*model.MetricBatch) map[string]bool {
		out := make(map[string]bool, len(batches[0].Metrics))
		for _, mt := range batches[0].Metrics {
			out[mt.ID] = true
		}
		return out
	}

	b1 := ids(m.MapMetricsData([]*metricsV1.ResourceMetrics{build()}))
	b2 := ids(m.MapMetricsData([]*metricsV1.ResourceMetrics{build()}))
	if len(b1) != 3 {
		t.Fatalf("expected 3 distinct metric IDs for delta/cumulative/monotonic variants, got %d", len(b1))
	}
	for id := range b1 {
		if !b2[id] {
			t.Errorf("metric ID %s not stable across mapping runs", id)
		}
	}
}

// TestMapMetricsData_NonFiniteValuesSurvive verifies that NaN/±Inf metric
// values — legal in OTLP but rejected by encoding/json — are preserved in
// payload JSON as string markers instead of silently dropping the payload
// (histogram bounds, summary quantiles) or failing the write (exemplars).
func TestMapMetricsData_NonFiniteValuesSurvive(t *testing.T) {
	m := NewMapper()
	now := uint64(time.Now().UnixNano())
	rm := &metricsV1.ResourceMetrics{
		Resource: &resourceV1.Resource{
			Attributes: []*commonV1.KeyValue{{Key: "service.name", Value: strValue("nan-svc")}},
		},
		ScopeMetrics: []*metricsV1.ScopeMetrics{{
			Scope: &commonV1.InstrumentationScope{Name: "nan-scope"},
			Metrics: []*metricsV1.Metric{
				{
					Name: "with.inf.bound",
					Data: &metricsV1.Metric_Histogram{Histogram: &metricsV1.Histogram{
						AggregationTemporality: metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
						DataPoints: []*metricsV1.HistogramDataPoint{{
							TimeUnixNano: now, Count: 2, Sum: float64ptr(math.NaN()),
							ExplicitBounds: []float64{0.1, math.Inf(1)},
							BucketCounts:   []uint64{1, 1},
						}},
					}},
				},
				{
					Name: "with.nan.quantile",
					Data: &metricsV1.Metric_Summary{Summary: &metricsV1.Summary{
						DataPoints: []*metricsV1.SummaryDataPoint{{
							TimeUnixNano: now, Count: 1, Sum: 1,
							QuantileValues: []*metricsV1.SummaryDataPoint_ValueAtQuantile{
								{Quantile: 0.5, Value: math.NaN()},
							},
						}},
					}},
				},
			},
		}},
	}

	batches := m.MapMetricsData([]*metricsV1.ResourceMetrics{rm})

	hist := batches[0].Metrics[0].Series[0].DataPoints[0]
	if hist.Sum == nil || !math.IsNaN(*hist.Sum) {
		t.Fatalf("histogram sum = %v, want NaN preserved in-memory", hist.Sum)
	}
	if len(hist.HistogramJSON) == 0 {
		t.Fatal("histogram payload dropped: NaN sum / +Inf bound must still be serialized")
	}
	var hp struct {
		Bounds []any    `json:"bounds"`
		Counts []uint64 `json:"counts"`
	}
	if err := json.Unmarshal(hist.HistogramJSON, &hp); err != nil {
		t.Fatalf("histogram JSON: %v", err)
	}
	if hp.Bounds[1] != "+Inf" {
		t.Errorf("bounds[1] = %v, want \"+Inf\" marker", hp.Bounds[1])
	}

	sp := batches[0].Metrics[1].Series[0].DataPoints[0]
	if len(sp.SummaryJSON) == 0 {
		t.Fatal("summary payload dropped: NaN quantile must still be serialized")
	}
	var qp struct {
		Quantiles []struct {
			Quantile any `json:"quantile"`
			Value    any `json:"value"`
		} `json:"quantiles"`
	}
	if err := json.Unmarshal(sp.SummaryJSON, &qp); err != nil {
		t.Fatalf("summary JSON: %v", err)
	}
	if qp.Quantiles[0].Value != "NaN" {
		t.Errorf("quantile value = %v, want \"NaN\" marker", qp.Quantiles[0].Value)
	}
}

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

// TestMapMetricsData_ExponentialHistogram covers the exp-histogram mapping,
// which preserves the full bucket layout (scale/offset/counts) and the
// zero-threshold as a JSON payload. Non-finite zero thresholds must survive
// as string markers ("NaN"), and missing positive/negative bucket sets must
// serialize as omitted fields rather than null entries.
func TestMapMetricsData_ExponentialHistogram(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	expH := &metricsV1.ExponentialHistogram{
		AggregationTemporality: metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
		DataPoints: []*metricsV1.ExponentialHistogramDataPoint{
			{
				TimeUnixNano: now, Count: 5, Sum: float64ptr(2.5), Min: float64ptr(0.1), Max: float64ptr(1.9),
				Scale: 3, ZeroCount: 2, ZeroThreshold: math.NaN(),
				Positive: &metricsV1.ExponentialHistogramDataPoint_Buckets{
					Offset: -1, BucketCounts: []uint64{1, 2},
				},
				// Negative buckets omitted on purpose.
			},
			// nil data point must be skipped, not panic.
			nil,
		},
	}
	m := NewMapper()
	batches := m.MapMetricsData([]*metricsV1.ResourceMetrics{
		{
			Resource: &resourceV1.Resource{
				Attributes: []*commonV1.KeyValue{{Key: "service.name", Value: strValue("exp-api")}},
			},
			ScopeMetrics: []*metricsV1.ScopeMetrics{
				{
					Scope: &commonV1.InstrumentationScope{Name: "exp-scope", Version: "1.0.0"},
					Metrics: []*metricsV1.Metric{
						{
							Name: "request.size",
							Unit: "By",
							Data: &metricsV1.Metric_ExponentialHistogram{ExponentialHistogram: expH},
						},
					},
				},
			},
		},
	})
	batch := batches[0]

	metric := batch.Metrics[0]
	if metric.Type != model.MetricTypeExponentialHistogram || metric.Temporality != model.TemporalityDelta {
		t.Errorf("exp-histogram type/temporality = %v/%v", metric.Type, metric.Temporality)
	}

	dp := metric.Series[0].DataPoints[0]
	if dp.Count == nil || *dp.Count != 5 || dp.Sum == nil || *dp.Sum != 2.5 {
		t.Errorf("exp-histogram count/sum = %v/%v", dp.Count, dp.Sum)
	}
	if dp.Min == nil || *dp.Min != 0.1 || dp.Max == nil || *dp.Max != 1.9 {
		t.Errorf("exp-histogram min/max = %v/%v", dp.Min, dp.Max)
	}

	var payload struct {
		Scale         int32  `json:"scale"`
		ZeroCount     uint64 `json:"zero_count"`
		ZeroThreshold any    `json:"zero_threshold"`
		Positive      struct {
			Offset       int32    `json:"offset"`
			BucketCounts []uint64 `json:"bucket_counts"`
		} `json:"positive"`
		Negative json.RawMessage `json:"negative"`
	}
	if err := json.Unmarshal(dp.ExponentialHistogramJSON, &payload); err != nil {
		t.Fatalf("exp-histogram JSON: %v", err)
	}
	if payload.Scale != 3 || payload.ZeroCount != 2 {
		t.Errorf("exp-histogram scale/zero_count = %d/%d", payload.Scale, payload.ZeroCount)
	}
	if payload.ZeroThreshold != "NaN" {
		t.Errorf("zero_threshold = %v, want \"NaN\" marker", payload.ZeroThreshold)
	}
	if payload.Positive.Offset != -1 || len(payload.Positive.BucketCounts) != 2 || payload.Positive.BucketCounts[1] != 2 {
		t.Errorf("positive buckets = %+v", payload.Positive)
	}
	// Absent negative buckets must be omitted entirely, not "null".
	if len(payload.Negative) != 0 {
		t.Errorf("negative = %s, want omitted", payload.Negative)
	}
}

// TestMapMetricsData_ExponentialHistogramNegativeBuckets covers the
// negative-bucket branch of the exp-histogram payload (mapBucketsJSON with a
// non-nil Buckets value).
func TestMapMetricsData_ExponentialHistogramNegativeBuckets(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	m := NewMapper()
	batches := m.MapMetricsData([]*metricsV1.ResourceMetrics{
		{
			Resource: &resourceV1.Resource{
				Attributes: []*commonV1.KeyValue{{Key: "service.name", Value: strValue("exp-api")}},
			},
			ScopeMetrics: []*metricsV1.ScopeMetrics{
				{
					Scope: &commonV1.InstrumentationScope{Name: "exp-scope", Version: "1.0.0"},
					Metrics: []*metricsV1.Metric{
						{
							Name: "request.size",
							Data: &metricsV1.Metric_ExponentialHistogram{ExponentialHistogram: &metricsV1.ExponentialHistogram{
								DataPoints: []*metricsV1.ExponentialHistogramDataPoint{
									{
										TimeUnixNano: now, Count: 1,
										Negative: &metricsV1.ExponentialHistogramDataPoint_Buckets{
											Offset: 2, BucketCounts: []uint64{3},
										},
									},
								},
							}},
						},
					},
				},
			},
		},
	})
	dp := batches[0].Metrics[0].Series[0].DataPoints[0]
	var payload struct {
		Negative struct {
			Offset       int32    `json:"offset"`
			BucketCounts []uint64 `json:"bucket_counts"`
		} `json:"negative"`
	}
	if err := json.Unmarshal(dp.ExponentialHistogramJSON, &payload); err != nil {
		t.Fatalf("exp-histogram JSON: %v", err)
	}
	if payload.Negative.Offset != 2 || len(payload.Negative.BucketCounts) != 1 || payload.Negative.BucketCounts[0] != 3 {
		t.Errorf("negative buckets = %+v", payload.Negative)
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

// TestMapMetricsData_NilResourceGetsStableID guards the fix for resource-less
// senders: OTLP allows ResourceMetrics with Resource unset, and the storage
// writer inserts the log_resource row keyed by the resource ID. An empty ID
// would make scope rows reference a resource that does not exist, silently
// dropping every data point from the read-side views (which join
// scope -> log_resource). The nil-resource batch must therefore carry a
// deterministic, non-empty ID so writes and reads stay consistent.
func TestMapMetricsData_NilResourceGetsStableID(t *testing.T) {
	m := NewMapper()
	rm := &metricsV1.ResourceMetrics{
		Resource:  nil, // spec-legal: "no resource info is known"
		SchemaUrl: "https://opentelemetry.io/schemas/1.21.0",
		ScopeMetrics: []*metricsV1.ScopeMetrics{{
			Scope:   &commonV1.InstrumentationScope{Name: "nil-res-scope", Version: "1.0.0"},
			Metrics: []*metricsV1.Metric{{Name: "nil.res.metric", Data: &metricsV1.Metric_Gauge{Gauge: &metricsV1.Gauge{DataPoints: []*metricsV1.NumberDataPoint{{Value: &metricsV1.NumberDataPoint_AsDouble{AsDouble: 1}}}}}}},
		}},
	}

	b1 := m.MapMetricsData([]*metricsV1.ResourceMetrics{rm})
	if len(b1) != 1 {
		t.Fatalf("got %d batches, want 1", len(b1))
	}
	batch := b1[0]
	if batch.Resource == nil || batch.Resource.ID == "" {
		t.Fatalf("nil OTLP resource must map to a resource with a non-empty ID, got %q", batch.Resource.ID)
	}
	if len(batch.Metrics) != 1 || batch.Metrics[0].ResourceID != batch.Resource.ID {
		t.Fatalf("metric ResourceID=%q must equal batch resource ID %q", batch.Metrics[0].ResourceID, batch.Resource.ID)
	}
	// Scope identity must be derived from the same (non-empty) resource ID.
	if batch.Metrics[0].ScopeID == "" {
		t.Fatalf("scope ID must be derivable from the stable resource ID")
	}

	// Deterministic: a second mapping of the same input yields the same IDs.
	b2 := m.MapMetricsData([]*metricsV1.ResourceMetrics{rm})
	if b2[0].Resource.ID != batch.Resource.ID {
		t.Errorf("resource ID not deterministic: %q vs %q", b2[0].Resource.ID, batch.Resource.ID)
	}
	if b2[0].Metrics[0].ScopeID != batch.Metrics[0].ScopeID {
		t.Errorf("scope ID not deterministic: %q vs %q", b2[0].Metrics[0].ScopeID, batch.Metrics[0].ScopeID)
	}

	// Different schema URLs are distinct identities (same rule as non-nil resources).
	rmOther := &metricsV1.ResourceMetrics{ScopeMetrics: rm.ScopeMetrics}
	b3 := m.MapMetricsData([]*metricsV1.ResourceMetrics{rmOther})
	if b3[0].Resource.ID == batch.Resource.ID {
		t.Errorf("resource ID must differ when schema URL differs")
	}
}
