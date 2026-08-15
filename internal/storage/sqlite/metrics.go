package sqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// SQL statements used by WriteMetricsCommand. Prepared once by the Writer
// and bound to each transaction via tx.Stmt() to avoid re-parsing overhead.
const (
	sqlInsertScope = `INSERT OR IGNORE INTO scope
		(id, resource_id, name, version, schema_url)
		VALUES (?, ?, ?, ?, ?)`

	sqlInsertMetric = `INSERT OR IGNORE INTO metric
		(id, scope_id, name, description, unit, type,
		is_monotonic, aggregation_temporality)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	sqlInsertSeries = `INSERT OR IGNORE INTO metric_series
		(id, metric_id, attributes_json)
		VALUES (?, ?, ?)`

	sqlInsertDataPoint = `INSERT INTO metric_data_point
		(series_id, timestamp_ns, start_timestamp_ns, flags,
		double_value, int_value, count, sum, min, max,
		histogram_json, exponential_histogram_json, summary_json, exemplars_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

// MetricsSeenCaches holds the process-local dedup caches for metric writes.
// All maps are owned by the Writer and must only be touched from the single
// writer goroutine. Resources are shared with the log write path.
type MetricsSeenCaches struct {
	Resources map[string]struct{}
	Scopes    map[string]struct{}
	Metrics   map[string]struct{}
	Series    map[string]struct{}
}

// WriteMetricsCommand is a Command that writes a single MetricBatch to
// SQLite. It mirrors WriteBatchCommand: immutable after construction, never
// begins/commits/rolls back a transaction, and uses injected prepared
// statements plus process-local dedup caches to skip INSERT OR IGNORE probes.
//
// Execute order inside one transaction follows parent-before-child:
// resource → scope → metric → series → data point.
type WriteMetricsCommand struct {
	batch  *model.MetricBatch
	points int // number of data points in the batch (cached at construction)

	preparedStmts *PreparedStatements
	seen          *MetricsSeenCaches
}

// NewWriteMetricsCommand creates a new WriteMetricsCommand from a
// MetricBatch. The caller must not modify the batch after submission.
func NewWriteMetricsCommand(batch *model.MetricBatch) *WriteMetricsCommand {
	points := 0
	if batch != nil {
		points = batch.Size()
	}
	return &WriteMetricsCommand{
		batch:  batch,
		points: points,
	}
}

// SetPreparedStatements injects pre-prepared SQL statements for use during
// Execute.
func (c *WriteMetricsCommand) SetPreparedStatements(stmts *PreparedStatements) {
	c.preparedStmts = stmts
}

// SetSeenCaches injects the process-local dedup caches. The maps are owned
// by the Writer and must only be used from the single writer goroutine.
func (c *WriteMetricsCommand) SetSeenCaches(seen *MetricsSeenCaches) {
	c.seen = seen
}

// Size returns the number of data points in this command. It satisfies the
// RecordCounter interface used by the writer for transaction splitting.
func (c *WriteMetricsCommand) Size() int {
	return c.points
}

// Batch returns the underlying MetricBatch (for metrics and diagnostics).
func (c *WriteMetricsCommand) Batch() *model.MetricBatch {
	return c.batch
}

// Execute writes the metric batch inside the supplied transaction.
func (c *WriteMetricsCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	if c.batch == nil || c.batch.IsEmpty() {
		return nil
	}

	// Acquire insert statements, preferring pre-prepared ones.
	var (
		insertResource  *sql.Stmt
		insertScope     *sql.Stmt
		insertMetric    *sql.Stmt
		insertSeries    *sql.Stmt
		insertDataPoint *sql.Stmt
	)

	if c.preparedStmts != nil {
		insertResource = tx.Stmt(c.preparedStmts.InsertResource)
		insertScope = tx.Stmt(c.preparedStmts.InsertScope)
		insertMetric = tx.Stmt(c.preparedStmts.InsertMetric)
		insertSeries = tx.Stmt(c.preparedStmts.InsertSeries)
		insertDataPoint = tx.Stmt(c.preparedStmts.InsertDataPoint)
	} else {
		// Fallback: prepare inline (used when the command runs without the Writer).
		var err error
		if insertResource, err = tx.PrepareContext(ctx, sqlInsertResource); err != nil {
			return fmt.Errorf("prepare log_resource: %w", err)
		}
		if insertScope, err = tx.PrepareContext(ctx, sqlInsertScope); err != nil {
			_ = insertResource.Close()
			return fmt.Errorf("prepare scope: %w", err)
		}
		if insertMetric, err = tx.PrepareContext(ctx, sqlInsertMetric); err != nil {
			_ = insertResource.Close()
			_ = insertScope.Close()
			return fmt.Errorf("prepare metric: %w", err)
		}
		if insertSeries, err = tx.PrepareContext(ctx, sqlInsertSeries); err != nil {
			_ = insertResource.Close()
			_ = insertScope.Close()
			_ = insertMetric.Close()
			return fmt.Errorf("prepare metric_series: %w", err)
		}
		if insertDataPoint, err = tx.PrepareContext(ctx, sqlInsertDataPoint); err != nil {
			_ = insertResource.Close()
			_ = insertScope.Close()
			_ = insertMetric.Close()
			_ = insertSeries.Close()
			return fmt.Errorf("prepare metric_data_point: %w", err)
		}
	}
	defer func() { _ = insertResource.Close() }()
	defer func() { _ = insertScope.Close() }()
	defer func() { _ = insertMetric.Close() }()
	defer func() { _ = insertSeries.Close() }()
	defer func() { _ = insertDataPoint.Close() }()

	// Insert the resource row (INSERT OR IGNORE for dedup), same scheme as
	// the log write path.
	if c.batch.Resource != nil {
		resourceID := ensureResourceID(c.batch.Resource)
		if !c.resourceSeen(resourceID) {
			attrsJSON := marshalResourceAttrs(c.batch.Resource.Attributes)
			if _, err := insertResource.ExecContext(
				ctx,
				resourceID,
				c.batch.Resource.GetServiceName(),
				c.batch.Resource.GetHostName(),
				c.batch.Resource.SchemaURL,
				attrsJSON,
			); err != nil {
				return fmt.Errorf("insert resource %q: %w", resourceID, err)
			}
			c.markResourceSeen(resourceID)
		}
	}

	for _, metric := range c.batch.Metrics {
		if metric == nil {
			continue
		}

		// Scope row (dedup).
		if !c.scopeSeen(metric.ScopeID) {
			if _, err := insertScope.ExecContext(
				ctx,
				metric.ScopeID,
				metric.ResourceID,
				metric.ScopeName,
				metric.ScopeVersion,
				metric.SchemaURL,
			); err != nil {
				return fmt.Errorf("insert scope %q: %w", metric.ScopeID, err)
			}
			c.markScopeSeen(metric.ScopeID)
		}

		// Metric definition row (dedup).
		if !c.metricSeen(metric.ID) {
			if _, err := insertMetric.ExecContext(
				ctx,
				metric.ID,
				metric.ScopeID,
				metric.Name,
				metric.Description,
				metric.Unit,
				int(metric.Type),
				boolToInt(metric.IsMonotonic),
				int(metric.Temporality),
			); err != nil {
				return fmt.Errorf("insert metric %q: %w", metric.ID, err)
			}
			c.markMetricSeen(metric.ID)
		}

		for _, series := range metric.Series {
			if series == nil {
				continue
			}

			// Series row (dedup by identity hash).
			attrsKey := model.CanonicalAttributesKey(series.Attributes)
			if !c.seriesSeen(series.ID) {
				if _, err := insertSeries.ExecContext(
					ctx,
					series.ID,
					metric.ID,
					// Bind as string, not []byte: SQLite stores []byte as BLOB,
					// and BLOB never equals a TEXT literal — series identity
					// queries compare attributes_json with =.
					string(attrsKey),
				); err != nil {
					return fmt.Errorf("insert metric_series %q: %w", series.ID, err)
				}
				c.markSeriesSeen(series.ID)
			}

			// Data points (append-only).
			for _, dp := range series.DataPoints {
				if dp == nil {
					continue
				}
				if err := insertDataPointRow(ctx, insertDataPoint, series.ID, dp); err != nil {
					return fmt.Errorf("insert metric_data_point: %w", err)
				}
			}
		}
	}

	return nil
}

// insertDataPointRow inserts a single metric data point row.
func insertDataPointRow(ctx context.Context, stmt *sql.Stmt, seriesID string, dp *model.DataPoint) error {
	exemplarsJSON, err := marshalExemplars(dp.Exemplars)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(
		ctx,
		seriesID,
		dp.Timestamp,
		nullableInt64(dp.StartTimestamp),
		dp.Flags,
		dp.DoubleValue,
		dp.IntValue,
		nullableUint64(dp.Count),
		dp.Sum,
		dp.Min,
		dp.Max,
		nullableText(dp.HistogramJSON),
		nullableText(dp.ExponentialHistogramJSON),
		nullableText(dp.SummaryJSON),
		nullableText(exemplarsJSON),
	)
	return err
}

// Dedup-cache helpers. All methods are safe on nil c.seen (caches disabled).

func (c *WriteMetricsCommand) resourceSeen(id string) bool {
	if c.seen == nil || c.seen.Resources == nil {
		return false
	}
	_, ok := c.seen.Resources[id]
	return ok
}

func (c *WriteMetricsCommand) markResourceSeen(id string) {
	if c.seen != nil && c.seen.Resources != nil && id != "" {
		c.seen.Resources[id] = struct{}{}
	}
}

func (c *WriteMetricsCommand) scopeSeen(id string) bool {
	if c.seen == nil || c.seen.Scopes == nil {
		return false
	}
	_, ok := c.seen.Scopes[id]
	return ok
}

func (c *WriteMetricsCommand) markScopeSeen(id string) {
	if c.seen != nil && c.seen.Scopes != nil && id != "" {
		c.seen.Scopes[id] = struct{}{}
	}
}

func (c *WriteMetricsCommand) metricSeen(id string) bool {
	if c.seen == nil || c.seen.Metrics == nil {
		return false
	}
	_, ok := c.seen.Metrics[id]
	return ok
}

func (c *WriteMetricsCommand) markMetricSeen(id string) {
	if c.seen != nil && c.seen.Metrics != nil && id != "" {
		c.seen.Metrics[id] = struct{}{}
	}
}

func (c *WriteMetricsCommand) seriesSeen(id string) bool {
	if c.seen == nil || c.seen.Series == nil {
		return false
	}
	_, ok := c.seen.Series[id]
	return ok
}

func (c *WriteMetricsCommand) markSeriesSeen(id string) {
	if c.seen != nil && c.seen.Series != nil && id != "" {
		c.seen.Series[id] = struct{}{}
	}
}

// marshalExemplars serializes exemplars as a compact JSON array. Returns nil
// when there are no exemplars (stored as SQL NULL).
func marshalExemplars(exemplars []model.Exemplar) ([]byte, error) {
	if len(exemplars) == 0 {
		return nil, nil
	}
	out := make([]exemplarJSON, 0, len(exemplars))
	for _, e := range exemplars {
		item := exemplarJSON{
			TimestampNs: e.Timestamp,
			DoubleValue: e.DoubleValue,
			IntValue:    e.IntValue,
		}
		if e.HasTrace {
			item.TraceID = hex.EncodeToString(e.TraceID[:])
			item.SpanID = hex.EncodeToString(e.SpanID[:])
		}
		if len(e.Attributes) > 0 {
			item.Attributes = model.CanonicalAttributesKey(e.Attributes)
		}
		out = append(out, item)
	}
	return json.Marshal(out)
}

// exemplarJSON is the wire shape for a single exemplar.
type exemplarJSON struct {
	TimestampNs int64    `json:"timestamp_ns"`
	DoubleValue *float64 `json:"double_value,omitempty"`
	IntValue    *int64   `json:"int_value,omitempty"`
	TraceID     string   `json:"trace_id,omitempty"`
	SpanID      string   `json:"span_id,omitempty"`
	Attributes  []byte   `json:"attributes,omitempty"`
}

// Small SQL-binding helpers.

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// nullableInt64 returns nil for zero values so SQLite stores NULL (the
// column is nullable; 0 is a valid timestamp only for absent start times,
// where NULL is the honest encoding).
func nullableInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullableUint64(v *uint64) any {
	if v == nil {
		return nil
	}
	return *v
}

// nullableText returns nil for empty byte slices (stored as SQL NULL) and the
// slice as a string otherwise. Strings, not []byte: SQLite stores []byte as
// BLOB, which never equals a TEXT literal in comparisons.
func nullableText(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}
