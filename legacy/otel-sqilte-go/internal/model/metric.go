package model

import (
	"sort"
	"strconv"
)

// MetricType mirrors the OTLP Metric.Data oneof. It determines how the
// value columns of metric_data_point are interpreted.
type MetricType uint8

// MetricType constants.
const (
	MetricTypeGauge MetricType = iota
	MetricTypeSum
	MetricTypeHistogram
	MetricTypeExponentialHistogram
	MetricTypeSummary
)

var metricTypeStrings = map[MetricType]string{
	MetricTypeGauge:                "gauge",
	MetricTypeSum:                  "sum",
	MetricTypeHistogram:            "histogram",
	MetricTypeExponentialHistogram: "exponential_histogram",
	MetricTypeSummary:              "summary",
}

// String returns the string representation of the metric type.
func (t MetricType) String() string {
	if s, ok := metricTypeStrings[t]; ok {
		return s
	}
	return "unknown"
}

// AggregationTemporality describes whether a metric aggregator reports delta
// changes since the last report, or cumulative changes since a fixed start.
// It matters for interpreting Sum/Histogram/ExponentialHistogram values.
type AggregationTemporality uint8

// AggregationTemporality constants (order matches OTLP enum values).
const (
	TemporalityUnspecified AggregationTemporality = iota
	TemporalityDelta
	TemporalityCumulative
)

var temporalityStrings = map[AggregationTemporality]string{
	TemporalityUnspecified: "unspecified",
	TemporalityDelta:       "delta",
	TemporalityCumulative:  "cumulative",
}

// String returns the string representation of the temporality.
func (t AggregationTemporality) String() string {
	if s, ok := temporalityStrings[t]; ok {
		return s
	}
	return "unspecified"
}

// Metric is the definition of a metric: name + metadata + type. It does NOT
// carry values. Multiple series (each with many data points) share one Metric.
//
// Scope identity is carried per metric (not per batch) so that batches
// containing multiple ScopeMetrics remain mergeable in the batcher.
type Metric struct {
	// ID is a stable hash of (resource_id, scope, name, unit, type).
	// Set by the mapper; used for INSERT OR IGNORE dedup in storage.
	ID string

	// ResourceID references the log_resource row this metric belongs to.
	ResourceID string

	// ScopeID references the scope row this metric belongs to.
	ScopeID string

	// ScopeName/ScopeVersion are the instrumentation scope (denormalized so
	// the mapper can compute ScopeID without a storage round trip).
	ScopeName    string
	ScopeVersion string

	// SchemaURL is the schema URL of the enclosing ScopeMetrics.
	SchemaURL string

	Name        string
	Description string
	Unit        string
	Type        MetricType

	// IsMonotonic applies to Sum only.
	IsMonotonic bool

	// Temporality applies to Sum/Histogram/ExponentialHistogram only.
	Temporality AggregationTemporality

	// Series holds the time series of this metric.
	Series []*MetricSeries
}

// MetricSeries is the time-series identity: (metric, attribute set).
// The canonical attribute set is what makes series distinct.
type MetricSeries struct {
	// ID is a stable hash of (Metric.ID + canonical attribute set).
	ID string

	// Attributes is the series identity attribute set (may be empty).
	Attributes []Attribute

	// DataPoints holds the samples of this series.
	DataPoints []*DataPoint
}

// DataPoint is a single sample of a series. Exactly one value shape is
// populated depending on the metric type:
//
//	Gauge/Sum                → DoubleValue or IntValue
//	Histogram/ExpHistogram   → Count, Sum, Min, Max + JSON payload
//	Summary                  → Count, Sum + SummaryJSON
//
// NaN double values are preserved in-memory; the SQLite writer persists
// them as NULL plus a nan_mask bit (SQLite REAL cannot store NaN).
type DataPoint struct {
	// Timestamp is when the sample was taken (unix ns).
	Timestamp int64

	// StartTimestamp is when the aggregation interval started (unix ns);
	// 0 when absent (e.g. gauges).
	StartTimestamp int64

	// Flags holds OTLP DataPointFlags (e.g. NoRecordedValue).
	Flags uint32

	// Scalar values (Gauge/Sum).
	DoubleValue *float64
	IntValue    *int64

	// Aggregation statistics (Histogram/Summary).
	Count *uint64
	Sum   *float64
	Min   *float64
	Max   *float64

	// Rich payloads, serialized compact JSON. Not normalized yet — the
	// columns exist so nothing is dropped during ingestion.
	HistogramJSON            []byte
	ExponentialHistogramJSON []byte
	SummaryJSON              []byte

	// Exemplars link measurements to traces/logs. Preserved, not yet queryable.
	Exemplars []Exemplar
}

// Exemplar is a measurement exemplar linking a data point to a trace.
type Exemplar struct {
	// Timestamp is when the measurement was recorded (unix ns).
	Timestamp int64

	// Value is the exemplar value (as_double or as_int).
	DoubleValue *float64
	IntValue    *int64

	// TraceID/SpanID are the trace context; valid when HasTrace is true.
	TraceID  [16]byte
	SpanID   [8]byte
	HasTrace bool

	// Attributes are the filtered attributes of the exemplar.
	Attributes []Attribute
}

// MetricBatch is the unit that flows through the metrics ingress queue —
// the analogue of model.LogBatch. It corresponds to one OTLP ResourceMetrics.
type MetricBatch struct {
	// Resource is the resource for this batch.
	Resource *Resource

	// SchemaURL is the schema URL of the enclosing ResourceMetrics.
	SchemaURL string

	// Metrics contains the metrics (each carrying its own scope + series).
	Metrics []*Metric
}

// NewMetricBatch creates a new MetricBatch with the given capacity.
func NewMetricBatch(capacity int) *MetricBatch {
	return &MetricBatch{
		Metrics: make([]*Metric, 0, capacity),
	}
}

// AddMetric appends a metric to the batch.
func (b *MetricBatch) AddMetric(metric *Metric) {
	b.Metrics = append(b.Metrics, metric)
}

// Size returns the total number of data points across all metrics.
// This is the unit of work used for batch sizing and transaction splitting.
func (b *MetricBatch) Size() int {
	total := 0
	for _, m := range b.Metrics {
		for _, s := range m.Series {
			total += len(s.DataPoints)
		}
	}
	return total
}

// IsEmpty returns true if the batch contains no data points.
func (b *MetricBatch) IsEmpty() bool {
	return b.Size() == 0
}

// Clear removes all metrics from the batch.
func (b *MetricBatch) Clear() {
	b.Metrics = b.Metrics[:0]
}

// CanonicalAttributesKey returns a stable, sorted, deduplicated canonical
// JSON encoding of the attribute set. Series identity is derived from this
// key (via a hash in the mapper), and storage persists the same bytes in
// metric_series.attributes_json. Attribute sets that differ only in order
// collapse to the same key.
//
// Duplicate keys are last-write-wins, matching the log event attribute path.
func CanonicalAttributesKey(attrs []Attribute) []byte {
	if len(attrs) == 0 {
		return []byte("{}")
	}

	// Last-write-wins index (no map[string]any allocation on the hot path).
	lastIdx := make(map[string]int, len(attrs))
	for i, attr := range attrs {
		lastIdx[attr.Key] = i
	}

	keys := make([]string, 0, len(lastIdx))
	for k := range lastIdx {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Estimate: 2 bytes per key/value pair plus attribute value overhead.
	buf := make([]byte, 0, len(attrs)*16)
	buf = append(buf, '{')
	for i, key := range keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendJSONString(buf, key)
		buf = append(buf, ':')
		buf = appendCanonicalAttrValue(buf, &attrs[lastIdx[key]])
	}
	buf = append(buf, '}')
	return buf
}

// appendCanonicalAttrValue appends the canonical JSON encoding of an
// attribute value (single string, no type wrapper — the series key only
// needs a deterministic textual identity, not OTLP type fidelity).
func appendCanonicalAttrValue(buf []byte, attr *Attribute) []byte {
	switch attr.Kind {
	case ValueString:
		return appendJSONString(buf, attr.Str)
	case ValueInt:
		return strconv.AppendInt(buf, attr.Num, 10)
	case ValueDouble:
		return strconv.AppendFloat(buf, attr.Dbl, 'f', -1, 64)
	case ValueBool:
		if attr.Flag {
			return append(buf, "true"...)
		}
		return append(buf, "false"...)
	case ValueBytes:
		return appendJSONString(buf, string(attr.Raw))
	case ValueNull:
		return append(buf, "null"...)
	default:
		return append(buf, "null"...)
	}
}

// appendJSONString appends s as a JSON string (with quotes and escaping).
func appendJSONString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	start := 0
	for i := range len(s) {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		if start < i {
			buf = append(buf, s[start:i]...)
		}
		switch c {
		case '"', '\\':
			buf = append(buf, '\\', c)
		case '\b':
			buf = append(buf, `\b`...)
		case '\n':
			buf = append(buf, `\n`...)
		case '\r':
			buf = append(buf, `\r`...)
		case '\t':
			buf = append(buf, `\t`...)
		default:
			const hex = "0123456789abcdef"
			buf = append(buf, `\u00`...)
			buf = append(buf, hex[c>>4], hex[c&0xf])
		}
		start = i + 1
	}
	if start < len(s) {
		buf = append(buf, s[start:]...)
	}
	return append(buf, '"')
}
