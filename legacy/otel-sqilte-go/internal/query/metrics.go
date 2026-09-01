// Package query provides read-side access to stored OTLP metrics.
//
// It is deliberately separate from the storage package: the writer owns the
// single write connection, while query opens its own read-only connection
// (SQLite WAL mode permits concurrent readers). All queries go through the
// `metrics` / `metric_buckets` views created by migration 007 so the join
// shape lives in one place.
package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"

	"codeberg.org/nicknad/otel-sqlite/internal/model"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// Store is a read-only handle to a collector SQLite database.
type Store struct {
	db *sql.DB
}

// Open opens the database at path for reading. WAL mode allows the query
// handle to run concurrently with the writer; the connection is opened
// read-write so PRAGMAs (query_only) apply cleanly, but no writes are issued.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("open query db %q: %w", path, err)
	}
	// Multiple query goroutines are fine; readers never contend with the
	// single writer in WAL mode.
	db.SetMaxOpenConns(4)

	if _, err := db.Exec("PRAGMA query_only = ON"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set query_only: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error {
	return s.db.Close()
}

// ---------------------------------------------------------------------------
// Series
// ---------------------------------------------------------------------------

// SeriesFilter selects time series. All non-zero fields are ANDed together.
type SeriesFilter struct {
	// MetricName filters by metric name (exact).
	MetricName string
	// ServiceName filters by resource service.name (exact).
	ServiceName string
	// ScopeName filters by instrumentation scope name (exact).
	ScopeName string
	// Limit caps the number of series returned (0 = no limit).
	Limit int
}

// Series is a time-series row plus its metric/resource context.
type Series struct {
	ID             string
	AttributesJSON string
	MetricID       string
	MetricName     string
	Description    string
	Unit           string
	Type           model.MetricType
	IsMonotonic    bool
	Temporality    model.AggregationTemporality
	ScopeName      string
	ScopeVersion   string
	ServiceName    string
	HostName       string
	DataPointCount int64
	FirstTimestamp int64
	LastTimestamp  int64
}

// Series returns the series matching the filter, newest first by
// last data point timestamp.
func (s *Store) Series(ctx context.Context, f SeriesFilter) ([]Series, error) {
	where := []string{"1=1"}
	args := []any{}
	if f.MetricName != "" {
		where = append(where, "m.name = ?")
		args = append(args, f.MetricName)
	}
	if f.ServiceName != "" {
		where = append(where, "r.service_name = ?")
		args = append(args, f.ServiceName)
	}
	if f.ScopeName != "" {
		where = append(where, "s.name = ?")
		args = append(args, f.ScopeName)
	}

	q := `SELECT
			ms.id, ms.attributes_json,
			m.id, m.name, m.description, m.unit, m.type,
			m.is_monotonic, m.aggregation_temporality,
			s.name, s.version,
			r.service_name, r.host_name,
			COUNT(dp.id) AS point_count,
			COALESCE(MIN(dp.timestamp_ns), 0) AS first_ts,
			COALESCE(MAX(dp.timestamp_ns), 0) AS last_ts
		FROM metric_series ms
		JOIN metric m ON m.id = ms.metric_id
		JOIN scope s ON s.id = m.scope_id
		JOIN log_resource r ON r.id = s.resource_id
		LEFT JOIN metric_data_point dp ON dp.series_id = ms.id
		WHERE ` + strings.Join(where, " AND ") + `
		GROUP BY ms.id
		ORDER BY last_ts DESC`
	if f.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", f.Limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query series: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Series
	for rows.Next() {
		var (
			sr Series
			//nolint:revive // bool/int scans need intermediates
			mono, temp int
		)
		if err := rows.Scan(
			&sr.ID, &sr.AttributesJSON,
			&sr.MetricID, &sr.MetricName, &sr.Description, &sr.Unit, &sr.Type,
			&mono, &temp,
			&sr.ScopeName, &sr.ScopeVersion,
			&sr.ServiceName, &sr.HostName,
			&sr.DataPointCount, &sr.FirstTimestamp, &sr.LastTimestamp,
		); err != nil {
			return nil, fmt.Errorf("scan series: %w", err)
		}
		sr.IsMonotonic = mono != 0
		sr.Temporality = model.AggregationTemporality(temp) //nolint:gosec // G115: DB value is a stored OTLP enum
		out = append(out, sr)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Data points
// ---------------------------------------------------------------------------

// DataPointFilter selects data points of one series in a time window.
type DataPointFilter struct {
	SeriesID string
	// FromNs is the inclusive lower timestamp bound (0 = none).
	FromNs int64
	// ToNs is the inclusive upper timestamp bound (0 = none).
	ToNs int64
	// Limit caps the number of points returned (0 = no limit).
	Limit int
	// Ascending orders by timestamp ascending (default is descending).
	Ascending bool
}

// DataPoint is one row of the metrics view.
type DataPoint struct {
	ID               int64
	SeriesID         string
	TimestampNs      int64
	StartTimestampNs int64
	Flags            uint32
	DoubleValue      *float64 // NaN when the nan_mask bit is set
	IntValue         *int64
	Count            *uint64
	Sum              *float64
	Min              *float64
	Max              *float64
	HistogramJSON    []byte
	ExpHistogramJSON []byte
	SummaryJSON      []byte
	ExemplarsJSON    []byte
}

// DataPoints returns the data points of a series within the time window.
// The existing idx_metric_dp_series_time(series_id, timestamp_ns) index
// serves this query.
func (s *Store) DataPoints(ctx context.Context, f DataPointFilter) ([]DataPoint, error) {
	if f.SeriesID == "" {
		return nil, errors.New("DataPoints: series_id is required")
	}
	where := []string{"series_id = ?"}
	args := []any{f.SeriesID}
	if f.FromNs > 0 {
		where = append(where, "timestamp_ns >= ?")
		args = append(args, f.FromNs)
	}
	if f.ToNs > 0 {
		where = append(where, "timestamp_ns <= ?")
		args = append(args, f.ToNs)
	}
	order := "DESC"
	if f.Ascending {
		order = "ASC"
	}

	q := `SELECT id, series_id, timestamp_ns, start_timestamp_ns, flags,
			double_value, int_value, count, sum, min, max, nan_mask,
			histogram_json, exponential_histogram_json, summary_json, exemplars_json
		FROM metrics
		WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY timestamp_ns ` + order
	if f.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", f.Limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query data points: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []DataPoint
	for rows.Next() {
		var (
			dp                    DataPoint
			nanMask               int
			dbl, sum, mn, mx      sql.NullFloat64
			iv, count, startTs    sql.NullInt64
			hist, ehist, smry, ex sql.NullString
		)
		if err := rows.Scan(
			&dp.ID, &dp.SeriesID, &dp.TimestampNs, &startTs, &dp.Flags,
			&dbl, &iv, &count, &sum, &mn, &mx, &nanMask,
			&hist, &ehist, &smry, &ex,
		); err != nil {
			return nil, fmt.Errorf("scan data point: %w", err)
		}
		dp.StartTimestampNs = startTs.Int64
		dp.DoubleValue = scanFloat(dbl, nanMask&nanMaskDoubleValue != 0)
		dp.IntValue = scanInt64(iv)
		dp.Count = scanUint64(count)
		dp.Sum = scanFloat(sum, nanMask&nanMaskSum != 0)
		dp.Min = scanFloat(mn, nanMask&nanMaskMin != 0)
		dp.Max = scanFloat(mx, nanMask&nanMaskMax != 0)
		dp.HistogramJSON = scanBytes(hist)
		dp.ExpHistogramJSON = scanBytes(ehist)
		dp.SummaryJSON = scanBytes(smry)
		dp.ExemplarsJSON = scanBytes(ex)
		out = append(out, dp)
	}
	return out, rows.Err()
}

// nan_mask bits (mirror of the storage constants).
const (
	nanMaskDoubleValue = 1 << iota
	nanMaskSum
	nanMaskMin
	nanMaskMax
)

// scanFloat reconstructs a float column: NULL + NaN bit -> NaN, NULL without
// the bit -> nil (absent), otherwise the value.
func scanFloat(v sql.NullFloat64, nan bool) *float64 {
	if !v.Valid {
		if nan {
			f := math.NaN()
			return &f
		}
		return nil
	}
	f := v.Float64
	return &f
}

func scanInt64(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	out := v.Int64
	return &out
}

func scanUint64(v sql.NullInt64) *uint64 {
	if !v.Valid {
		return nil
	}
	out := uint64(v.Int64) //nolint:gosec // G115: counts are non-negative
	return &out
}

func scanBytes(v sql.NullString) []byte {
	if !v.Valid {
		return nil
	}
	return []byte(v.String)
}

// ---------------------------------------------------------------------------
// Histogram buckets (query-time normalization of histogram_json)
// ---------------------------------------------------------------------------

// Bucket is one row of the metric_buckets view.
type Bucket struct {
	DataPointID int64
	SeriesID    string
	TimestampNs int64
	Index       int
	// BoundJSON is the raw JSON value: a number or a string marker
	// ("NaN", "+Inf", "-Inf") for non-finite bounds.
	BoundJSON string
	// Bound is the numeric bound, nil when the bound is non-finite.
	Bound *float64
	Count uint64
}

// Buckets returns the normalized histogram buckets for a series in a time
// window, ordered by data point timestamp then bucket index.
func (s *Store) Buckets(ctx context.Context, seriesID string, fromNs, toNs int64, limit int) ([]Bucket, error) {
	where := []string{"series_id = ?"}
	args := []any{seriesID}
	if fromNs > 0 {
		where = append(where, "timestamp_ns >= ?")
		args = append(args, fromNs)
	}
	if toNs > 0 {
		where = append(where, "timestamp_ns <= ?")
		args = append(args, toNs)
	}
	q := `SELECT data_point_id, series_id, timestamp_ns, bucket_index, bound_json, bound, bucket_count
		FROM metric_buckets
		WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY timestamp_ns ASC, bucket_index ASC`
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query buckets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Bucket
	for rows.Next() {
		var (
			b     Bucket
			bound sql.NullFloat64
			count sql.NullInt64
		)
		if err := rows.Scan(
			&b.DataPointID, &b.SeriesID, &b.TimestampNs, &b.Index, &b.BoundJSON, &bound, &count,
		); err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}
		if bound.Valid {
			f := bound.Float64
			b.Bound = &f
		}
		if count.Valid {
			b.Count = uint64(count.Int64) //nolint:gosec // G115: counts are non-negative
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Exemplars (trace ↔ metric correlation)
// ---------------------------------------------------------------------------

// ExemplarHit is a data point carrying an exemplar that references a trace.
type ExemplarHit struct {
	DataPointID       int64
	SeriesID          string
	TimestampNs       int64
	MetricName        string
	ServiceName       string
	SeriesAttributes  string
	TraceID           string
	SpanID            string
	ExemplarTimestamp int64
	ExemplarDouble    *float64
	ExemplarInt       *int64
}

// ByExemplarTrace returns data points whose exemplars_json contains an
// exemplar with the given trace id (lowercase hex, as stored). The trace id
// is matched structurally via json_each so the correlation is exact, not a
// substring scan of the whole payload.
func (s *Store) ByExemplarTrace(ctx context.Context, traceID string, limit int) ([]ExemplarHit, error) {
	q := `SELECT
			dp.id, dp.series_id, dp.timestamp_ns,
			m.name, r.service_name, ms.attributes_json,
			e.value ->> 'trace_id',
			e.value ->> 'span_id',
			CAST(e.value ->> 'timestamp_ns' AS INTEGER),
			e.value ->> 'double_value',
			e.value ->> 'int_value'
		FROM metric_data_point dp
		JOIN json_each(dp.exemplars_json) e
		JOIN metric_series ms ON ms.id = dp.series_id
		JOIN metric m ON m.id = ms.metric_id
		JOIN scope s ON s.id = m.scope_id
		JOIN log_resource r ON r.id = s.resource_id
		WHERE dp.exemplars_json IS NOT NULL
		  AND e.value ->> 'trace_id' = ?`
	args := []any{traceID}
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query exemplars: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ExemplarHit
	for rows.Next() {
		var (
			h        ExemplarHit
			spanID   sql.NullString
			exTs     sql.NullInt64
			exDouble sql.NullString
			exInt    sql.NullInt64
		)
		if err := rows.Scan(
			&h.DataPointID, &h.SeriesID, &h.TimestampNs,
			&h.MetricName, &h.ServiceName, &h.SeriesAttributes,
			&h.TraceID, &spanID,
			&exTs, &exDouble, &exInt,
		); err != nil {
			return nil, fmt.Errorf("scan exemplar hit: %w", err)
		}
		h.SpanID = spanID.String
		h.ExemplarTimestamp = exTs.Int64
		if exDouble.Valid && exDouble.String != "" {
			if f, err := parseJSONFloat(exDouble.String); err == nil {
				h.ExemplarDouble = &f
			}
		}
		if exInt.Valid {
			v := exInt.Int64
			h.ExemplarInt = &v
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// parseJSONFloat parses a JSON float value that may be a number or a string
// marker ("NaN", "+Inf", "-Inf") as written by the mapper.
func parseJSONFloat(s string) (float64, error) {
	switch s {
	case `"NaN"`:
		return math.NaN(), nil
	case `"+Inf"`:
		return math.Inf(1), nil
	case `"-Inf"`:
		return math.Inf(-1), nil
	}
	var f float64
	if _, err := fmt.Sscanf(s, "%g", &f); err != nil {
		return 0, err
	}
	return f, nil
}
