package otlp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"codeberg.org/nicknad/otel-sqlite/internal/model"

	// OTLP protobuf types — must not leak beyond this package.
	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	metricsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/metrics/v1"
)

// MapMetricsData converts OTLP ResourceMetrics to internal MetricBatch
// slices. Returns a slice of MetricBatch, one for each ResourceMetrics in
// the input (the analogue of MapLogsData).
func (m *Mapper) MapMetricsData(resourceMetrics []*metricsV1.ResourceMetrics) []*model.MetricBatch {
	if len(resourceMetrics) == 0 {
		return nil
	}

	batches := make([]*model.MetricBatch, 0, len(resourceMetrics))

	for _, rm := range resourceMetrics {
		batch := m.mapResourceMetrics(rm)
		if batch != nil && !batch.IsEmpty() {
			batches = append(batches, batch)
		}
	}

	return batches
}

// mapResourceMetrics converts a single ResourceMetrics to a MetricBatch.
func (m *Mapper) mapResourceMetrics(rm *metricsV1.ResourceMetrics) *model.MetricBatch {
	if rm == nil {
		return nil
	}

	batch := model.NewMetricBatch(len(rm.ScopeMetrics))

	// Map resource (same dedup scheme as logs — resources are shared).
	resource := m.mapResource(rm.Resource, rm.SchemaUrl)
	batch.Resource = resource
	batch.SchemaURL = rm.SchemaUrl

	for _, sm := range rm.ScopeMetrics {
		m.mapScopeMetrics(sm, batch, resource)
	}

	return batch
}

// mapScopeMetrics converts ScopeMetrics and adds metrics to the batch.
// Scope identity is carried on each metric so batches containing multiple
// scopes remain mergeable.
func (m *Mapper) mapScopeMetrics(sm *metricsV1.ScopeMetrics, batch *model.MetricBatch, resource *model.Resource) {
	if sm == nil {
		return
	}

	scopeName := ""
	scopeVersion := ""
	if sm.Scope != nil {
		scopeName = sm.Scope.Name
		scopeVersion = sm.Scope.Version
	}

	for _, protoMetric := range sm.Metrics {
		metric := m.mapMetric(protoMetric, resource, scopeName, scopeVersion, sm.SchemaUrl)
		if metric != nil {
			batch.AddMetric(metric)
		}
	}
}

// mapMetric converts an OTLP Metric to an internal Metric, grouping its data
// points into series by canonical attribute set. Metrics with no data points
// are dropped.
func (m *Mapper) mapMetric(protoMetric *metricsV1.Metric, resource *model.Resource, scopeName, scopeVersion, schemaURL string) *model.Metric { //nolint:lll
	if protoMetric == nil {
		return nil
	}

	metric := &model.Metric{
		ResourceID:   resource.ID,
		ScopeName:    scopeName,
		ScopeVersion: scopeVersion,
		SchemaURL:    schemaURL,
		Name:         protoMetric.Name,
		Description:  protoMetric.Description,
		Unit:         protoMetric.Unit,
	}

	// Determine type metadata first: Metric.ID hashes the type, and series
	// IDs hash Metric.ID, so identity must be fixed before mapping points.
	switch data := protoMetric.Data.(type) {
	case *metricsV1.Metric_Gauge:
		metric.Type = model.MetricTypeGauge
	case *metricsV1.Metric_Sum:
		metric.Type = model.MetricTypeSum
		metric.IsMonotonic = data.Sum.IsMonotonic
		metric.Temporality = mapTemporality(data.Sum.AggregationTemporality)
	case *metricsV1.Metric_Histogram:
		metric.Type = model.MetricTypeHistogram
		metric.Temporality = mapTemporality(data.Histogram.AggregationTemporality)
	case *metricsV1.Metric_ExponentialHistogram:
		metric.Type = model.MetricTypeExponentialHistogram
		metric.Temporality = mapTemporality(data.ExponentialHistogram.AggregationTemporality)
	case *metricsV1.Metric_Summary:
		metric.Type = model.MetricTypeSummary
	default:
		// Unknown/empty data — nothing to store.
		return nil
	}

	metric.ScopeID = computeScopeID(resource.ID, scopeName, scopeVersion, schemaURL)
	metric.ID = computeMetricID(metric)

	switch data := protoMetric.Data.(type) {
	case *metricsV1.Metric_Gauge:
		m.mapNumberDataPoints(metric, data.Gauge.DataPoints)
	case *metricsV1.Metric_Sum:
		m.mapNumberDataPoints(metric, data.Sum.DataPoints)
	case *metricsV1.Metric_Histogram:
		m.mapHistogramDataPoints(metric, data.Histogram.DataPoints)
	case *metricsV1.Metric_ExponentialHistogram:
		m.mapExponentialHistogramDataPoints(metric, data.ExponentialHistogram.DataPoints)
	case *metricsV1.Metric_Summary:
		m.mapSummaryDataPoints(metric, data.Summary.DataPoints)
	}

	if len(metric.Series) == 0 {
		return nil
	}
	return metric
}

// mapNumberDataPoints maps Gauge/Sum NumberDataPoints, grouping them into
// series by canonical attribute set.
func (m *Mapper) mapNumberDataPoints(metric *model.Metric, points []*metricsV1.NumberDataPoint) {
	seriesByKey := make(map[string]*model.MetricSeries)
	for _, p := range points {
		if p == nil {
			continue
		}
		series := m.seriesForAttrs(metric, seriesByKey, p.Attributes)

		dp := &model.DataPoint{
			Timestamp:      int64(p.TimeUnixNano),      //nolint:gosec
			StartTimestamp: int64(p.StartTimeUnixNano), //nolint:gosec
			Flags:          p.Flags,
			Exemplars:      mapExemplars(p.Exemplars),
		}
		switch v := p.Value.(type) {
		case *metricsV1.NumberDataPoint_AsDouble:
			dp.DoubleValue = &v.AsDouble
		case *metricsV1.NumberDataPoint_AsInt:
			val := v.AsInt
			dp.IntValue = &val
		default:
			// Point without a value is invalid per spec — skip it.
			continue
		}
		series.DataPoints = append(series.DataPoints, dp)
	}
}

// mapHistogramDataPoints maps HistogramDataPoints. Count/sum/min/max are
// promoted to typed columns; bucket distribution is preserved as a compact
// JSON payload (not yet normalized — Phase 2 optimization).
func (m *Mapper) mapHistogramDataPoints(metric *model.Metric, points []*metricsV1.HistogramDataPoint) {
	seriesByKey := make(map[string]*model.MetricSeries)
	for _, p := range points {
		if p == nil {
			continue
		}
		series := m.seriesForAttrs(metric, seriesByKey, p.Attributes)

		dp := &model.DataPoint{
			Timestamp:      int64(p.TimeUnixNano),      //nolint:gosec
			StartTimestamp: int64(p.StartTimeUnixNano), //nolint:gosec
			Flags:          p.Flags,
			Count:          &p.Count,
			Sum:            p.Sum,
			Min:            p.Min,
			Max:            p.Max,
			Exemplars:      mapExemplars(p.Exemplars),
		}
		if len(p.BucketCounts) > 0 || len(p.ExplicitBounds) > 0 {
			if payload, err := json.Marshal(histogramJSON{
				Bounds: p.ExplicitBounds,
				Counts: p.BucketCounts,
			}); err == nil {
				dp.HistogramJSON = payload
			}
		}
		series.DataPoints = append(series.DataPoints, dp)
	}
}

// mapExponentialHistogramDataPoints maps ExponentialHistogramDataPoints,
// preserving the full bucket layout as a JSON payload.
func (m *Mapper) mapExponentialHistogramDataPoints(metric *model.Metric, points []*metricsV1.ExponentialHistogramDataPoint) { //nolint:lll
	seriesByKey := make(map[string]*model.MetricSeries)
	for _, p := range points {
		if p == nil {
			continue
		}
		series := m.seriesForAttrs(metric, seriesByKey, p.Attributes)

		dp := &model.DataPoint{
			Timestamp:      int64(p.TimeUnixNano),      //nolint:gosec
			StartTimestamp: int64(p.StartTimeUnixNano), //nolint:gosec
			Flags:          p.Flags,
			Count:          &p.Count,
			Sum:            p.Sum,
			Min:            p.Min,
			Max:            p.Max,
			Exemplars:      mapExemplars(p.Exemplars),
		}
		payload, err := json.Marshal(expHistogramJSON{
			Scale:         p.Scale,
			ZeroCount:     p.ZeroCount,
			ZeroThreshold: p.ZeroThreshold,
			Positive:      mapBucketsJSON(p.Positive),
			Negative:      mapBucketsJSON(p.Negative),
		})
		if err == nil {
			dp.ExponentialHistogramJSON = payload
		}
		series.DataPoints = append(series.DataPoints, dp)
	}
}

// mapSummaryDataPoints maps SummaryDataPoints. Count/sum are typed columns;
// quantile values are preserved as a JSON payload.
func (m *Mapper) mapSummaryDataPoints(metric *model.Metric, points []*metricsV1.SummaryDataPoint) {
	seriesByKey := make(map[string]*model.MetricSeries)
	for _, p := range points {
		if p == nil {
			continue
		}
		series := m.seriesForAttrs(metric, seriesByKey, p.Attributes)

		dp := &model.DataPoint{
			Timestamp:      int64(p.TimeUnixNano),      //nolint:gosec
			StartTimestamp: int64(p.StartTimeUnixNano), //nolint:gosec
			Flags:          p.Flags,
			Count:          &p.Count,
			Sum:            &p.Sum,
		}
		if len(p.QuantileValues) > 0 {
			quantiles := make([]quantileJSON, 0, len(p.QuantileValues))
			for _, qv := range p.QuantileValues {
				if qv == nil {
					continue
				}
				quantiles = append(quantiles, quantileJSON{Quantile: qv.Quantile, Value: qv.Value})
			}
			if payload, err := json.Marshal(summaryJSON{Quantiles: quantiles}); err == nil {
				dp.SummaryJSON = payload
			}
		}
		series.DataPoints = append(series.DataPoints, dp)
	}
}

// seriesForAttrs returns the series for the given OTLP attributes within the
// metric, creating it (keyed by canonical attribute set) if needed.
func (m *Mapper) seriesForAttrs(metric *model.Metric, seriesByKey map[string]*model.MetricSeries, attrs []*commonV1.KeyValue) *model.MetricSeries { //nolint:lll
	var seriesAttrs []model.Attribute
	if len(attrs) > 0 {
		seriesAttrs = make([]model.Attribute, 0, len(attrs))
		for _, kv := range attrs {
			seriesAttrs = append(seriesAttrs, mapAttribute(kv))
		}
	}

	key := string(model.CanonicalAttributesKey(seriesAttrs))
	series := seriesByKey[key]
	if series == nil {
		series = &model.MetricSeries{
			ID:         computeSeriesID(metric.ID, []byte(key)),
			Attributes: seriesAttrs,
		}
		seriesByKey[key] = series
		metric.Series = append(metric.Series, series)
	}
	return series
}

// mapExemplars converts OTLP Exemplars, preserving trace linkage.
func mapExemplars(protoExemplars []*metricsV1.Exemplar) []model.Exemplar {
	if len(protoExemplars) == 0 {
		return nil
	}
	out := make([]model.Exemplar, 0, len(protoExemplars))
	for _, pe := range protoExemplars {
		if pe == nil {
			continue
		}
		e := model.Exemplar{Timestamp: int64(pe.TimeUnixNano)} //nolint:gosec
		switch v := pe.Value.(type) {
		case *metricsV1.Exemplar_AsDouble:
			e.DoubleValue = &v.AsDouble
		case *metricsV1.Exemplar_AsInt:
			val := v.AsInt
			e.IntValue = &val
		}
		if len(pe.TraceId) == 16 {
			copy(e.TraceID[:], pe.TraceId)
			e.HasTrace = true
		}
		if len(pe.SpanId) == 8 {
			copy(e.SpanID[:], pe.SpanId)
		}
		if len(pe.FilteredAttributes) > 0 {
			e.Attributes = make([]model.Attribute, 0, len(pe.FilteredAttributes))
			for _, kv := range pe.FilteredAttributes {
				e.Attributes = append(e.Attributes, mapAttribute(kv))
			}
		}
		out = append(out, e)
	}
	return out
}

// mapTemporality converts the OTLP aggregation temporality enum.
func mapTemporality(t metricsV1.AggregationTemporality) model.AggregationTemporality {
	switch t {
	case metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA:
		return model.TemporalityDelta
	case metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE:
		return model.TemporalityCumulative
	default:
		return model.TemporalityUnspecified
	}
}

// computeScopeID returns a stable identifier for a scope within a resource.
func computeScopeID(resourceID, name, version, schemaURL string) string {
	h := sha256.New()
	h.Write([]byte(resourceID))
	h.Write([]byte{0})
	h.Write([]byte(name))
	h.Write([]byte{0})
	h.Write([]byte(version))
	h.Write([]byte{0})
	h.Write([]byte(schemaURL))
	return "scope-" + hex.EncodeToString(h.Sum(nil)[:16])
}

// computeMetricID returns a stable identifier for a metric definition.
// Unit and type are part of identity: a change in either creates a new
// metric (and therefore new series), matching OTLP semantics.
func computeMetricID(m *model.Metric) string {
	h := sha256.New()
	h.Write([]byte(m.ResourceID))
	h.Write([]byte{0})
	h.Write([]byte(m.ScopeName))
	h.Write([]byte{0})
	h.Write([]byte(m.ScopeVersion))
	h.Write([]byte{0})
	h.Write([]byte(m.SchemaURL))
	h.Write([]byte{0})
	h.Write([]byte(m.Name))
	h.Write([]byte{0})
	h.Write([]byte(m.Unit))
	h.Write([]byte{0})
	h.Write([]byte(strconv.Itoa(int(m.Type))))
	return "metric-" + hex.EncodeToString(h.Sum(nil)[:16])
}

// computeSeriesID returns a stable identifier for a series: the metric ID
// plus the canonical attribute key.
func computeSeriesID(metricID string, attrsKey []byte) string {
	h := sha256.New()
	h.Write([]byte(metricID))
	h.Write([]byte{0})
	h.Write(attrsKey)
	return "series-" + hex.EncodeToString(h.Sum(nil)[:16])
}

// JSON payload shapes for rich metric data (Phase 2/3 storage).

type histogramJSON struct {
	Bounds []float64 `json:"bounds,omitempty"`
	Counts []uint64  `json:"counts,omitempty"`
}

type expHistogramJSON struct {
	Scale         int32        `json:"scale"`
	ZeroCount     uint64       `json:"zero_count"`
	ZeroThreshold float64      `json:"zero_threshold"`
	Positive      *bucketsJSON `json:"positive,omitempty"`
	Negative      *bucketsJSON `json:"negative,omitempty"`
}

type bucketsJSON struct {
	Offset       int32    `json:"offset"`
	BucketCounts []uint64 `json:"bucket_counts,omitempty"`
}

func mapBucketsJSON(b *metricsV1.ExponentialHistogramDataPoint_Buckets) *bucketsJSON {
	if b == nil {
		return nil
	}
	return &bucketsJSON{Offset: b.Offset, BucketCounts: b.BucketCounts}
}

type summaryJSON struct {
	Quantiles []quantileJSON `json:"quantiles,omitempty"`
}

type quantileJSON struct {
	Quantile float64 `json:"quantile"`
	Value    float64 `json:"value"`
}
