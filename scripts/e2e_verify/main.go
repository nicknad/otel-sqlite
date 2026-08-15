// Command e2e_verify is the assertion half of scripts/e2e-mixed.sh.
//
// It opens the collector's SQLite database read-only, compares the stored
// log/metric row counts against the mockapp summary, checks structural
// invariants (no orphaned rows), and proves trace↔metric correlation via
// the read-side query package. Exits non-zero on any failed check.
package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"codeberg.org/nicknad/otel-sqlite/internal/query"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// summary mirrors cmd/mockapp's summary type.
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
	SampleTraceID    string   `json:"sample_trace_id"`
	LastTraceID      string   `json:"last_trace_id"`
}

type verifier struct {
	db      *sql.DB
	store   *query.Store
	ctx     context.Context
	summary summary
	failed  int
	checked int
}

func (v *verifier) check(name string, ok bool, detail string) {
	v.checked++
	if ok {
		fmt.Printf("  PASS  %s\n", name)
		return
	}
	v.failed++
	fmt.Printf("  FAIL  %s: %s\n", name, detail)
}

func (v *verifier) checkErr(name string, err error) {
	if err != nil {
		v.check(name, false, err.Error())
		return
	}
	v.check(name, true, "")
}

func (v *verifier) count(table string) (int64, error) {
	var n int64
	err := v.db.QueryRowContext(v.ctx, "SELECT COUNT(*) FROM "+table).Scan(&n)
	return n, err //nolint:wrapcheck // script tool
}

func main() {
	dbPath := flag.String("db", "", "collector SQLite database path (required)")
	summaryPath := flag.String("summary", "", "mockapp summary JSON path (required)")
	flag.Parse()

	if *dbPath == "" || *summaryPath == "" {
		log.Fatal("-db and -summary are required")
	}

	data, err := os.ReadFile(*summaryPath)
	if err != nil {
		log.Fatalf("read summary: %v", err)
	}
	var s summary
	if err := json.Unmarshal(data, &s); err != nil {
		log.Fatalf("parse summary: %v", err)
	}

	db, err := sql.Open("sqlite3", *dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	store, err := query.Open(*dbPath)
	if err != nil {
		_ = db.Close()
		log.Fatalf("open query store: %v", err) //nolint:gocritic // process exits anyway
	}
	defer func() { _ = store.Close() }()

	v := &verifier{
		db:      db,
		store:   store,
		ctx:     context.Background(),
		summary: s,
	}

	fmt.Printf("verifying %s against %s\n", *dbPath, *summaryPath)
	fmt.Printf("mockapp summary: %d requests, %d log records, %d metric points, %d errors\n",
		s.RequestsSent, s.LogRecordsSent, s.MetricPointsSent, s.ExportErrors)

	v.check("mockapp reported zero export errors", s.ExportErrors == 0,
		fmt.Sprintf("export_errors=%d", s.ExportErrors))

	// ---- exact row counts ----
	logEvents, err := v.count("log_event")
	v.checkErr("log_event count matches mockapp", err)
	if err == nil {
		v.check("log_event count == log records sent", logEvents == int64(s.LogRecordsSent),
			fmt.Sprintf("stored=%d sent=%d", logEvents, s.LogRecordsSent))
	}
	points, err := v.count("metric_data_point")
	v.checkErr("metric_data_point count matches mockapp", err)
	if err == nil {
		v.check("metric_data_point count == points sent", points == int64(s.MetricPointsSent),
			fmt.Sprintf("stored=%d sent=%d", points, s.MetricPointsSent))
	}

	// ---- schema-level structure ----
	nResources := len(s.Resources)
	resCount, err := v.count("log_resource")
	v.checkErr("log_resource count", err)
	if err == nil {
		v.check("log_resource count == resources", resCount == int64(nResources),
			fmt.Sprintf("stored=%d expected=%d", resCount, nResources))
	}
	scopeCount, err := v.count("scope")
	v.checkErr("scope count", err)
	if err == nil {
		// One scope per resource (shared by both signals).
		v.check("scope count == resources", scopeCount == int64(nResources),
			fmt.Sprintf("stored=%d expected=%d", scopeCount, nResources))
	}
	metricCount, err := v.count("metric")
	v.checkErr("metric count", err)
	if err == nil {
		// 3 metric definitions per resource (gauge, sum, histogram).
		v.check("metric count == 3*resources", metricCount == int64(3*nResources),
			fmt.Sprintf("stored=%d expected=%d", metricCount, 3*nResources))
	}
	seriesCount, err := v.count("metric_series")
	v.checkErr("metric_series count", err)
	if err == nil {
		// One series per metric (all points carry identical attributes).
		v.check("metric_series count == 3*resources", seriesCount == int64(3*nResources),
			fmt.Sprintf("stored=%d expected=%d", seriesCount, 3*nResources))
	}

	// ---- orphan invariants (foreign_keys=OFF on the writer makes these
	// possible failure modes that raw counts would miss) ----
	for _, q := range []struct {
		name string
		sql  string
	}{
		{
			name: "no data points with missing series",
			sql: `SELECT COUNT(*) FROM metric_data_point dp
				LEFT JOIN metric_series ms ON ms.id = dp.series_id WHERE ms.id IS NULL`,
		},
		{
			name: "no series with missing metric",
			sql:  "SELECT COUNT(*) FROM metric_series ms LEFT JOIN metric m ON m.id = ms.metric_id WHERE m.id IS NULL",
		},
		{
			name: "no metric with missing scope",
			sql:  "SELECT COUNT(*) FROM metric m LEFT JOIN scope s ON s.id = m.scope_id WHERE s.id IS NULL",
		},
		{
			name: "no scope with missing resource",
			sql:  "SELECT COUNT(*) FROM scope s LEFT JOIN log_resource r ON r.id = s.resource_id WHERE r.id IS NULL",
		},
		{
			name: "no data point with NULL timestamp",
			sql:  "SELECT COUNT(*) FROM metric_data_point WHERE timestamp_ns IS NULL",
		},
	} {
		var n int64
		if err := v.db.QueryRowContext(v.ctx, q.sql).Scan(&n); err != nil {
			v.checkErr(q.name, err)
			continue
		}
		v.check(q.name, n == 0, fmt.Sprintf("found %d rows", n))
	}

	// ---- read-side queries through the query package ----
	v.verifyReadSide()

	// ---- trace ↔ metric correlation ----
	v.verifyCorrelation()

	fmt.Println()
	if v.failed > 0 {
		fmt.Printf("RESULT: FAIL (%d/%d checks failed)\n", v.failed, v.checked)
		os.Exit(1)
	}
	fmt.Printf("RESULT: PASS (%d checks)\n", v.checked)
}

// verifyReadSide asserts the metrics read view answers the three expected
// series shapes and their data points/buckets.
func (v *verifier) verifyReadSide() {
	nResources := len(v.summary.Resources)
	ctx := v.ctx

	// Series lookup by metric name: 1 series per resource.
	series, err := v.store.Series(ctx, query.SeriesFilter{MetricName: v.summary.HistogramMetric})
	v.checkErr("query.Series finds histogram series", err)
	if err == nil {
		v.check("histogram series per resource", len(series) == nResources,
			fmt.Sprintf("found=%d expected=%d", len(series), nResources))
	}

	// Gauge series: one point per request for each resource (round-robin), so
	// the total across all gauge series equals the request count exactly.
	requestsPerResource := v.summary.RequestsSent / nResources
	gaugeSeries, err := v.store.Series(ctx, query.SeriesFilter{MetricName: v.summary.LogMetricName})
	v.checkErr("query.Series finds gauge series", err)
	if err == nil {
		total := int64(0)
		for i := range gaugeSeries {
			total += gaugeSeries[i].DataPointCount
		}
		v.check("gauge points across series == requests sent", total == int64(v.summary.RequestsSent),
			fmt.Sprintf("total=%d requests=%d series=%d", total, v.summary.RequestsSent, len(gaugeSeries)))
		v.check("gauge series per resource", len(gaugeSeries) == nResources,
			fmt.Sprintf("series=%d expected=%d", len(gaugeSeries), nResources))
		if len(gaugeSeries) > 0 {
			pts, err := v.store.DataPoints(ctx, query.DataPointFilter{SeriesID: gaugeSeries[0].ID})
			v.checkErr("query.DataPoints on gauge series", err)
			if err == nil {
				lo, hi := requestsPerResource, requestsPerResource+1
				v.check("gauge data points queryable", len(pts) >= lo && len(pts) <= hi,
					fmt.Sprintf("points=%d expected %d..%d per resource", len(pts), lo, hi))
			}
		}
	}

	// Histogram buckets: every point normalizes to len(bounds)+1 buckets.
	histSeries, err := v.store.Series(ctx, query.SeriesFilter{MetricName: v.summary.HistogramMetric})
	v.checkErr("query.Series finds histogram series (buckets)", err)
	if err == nil {
		total := int64(0)
		for i := range histSeries {
			total += histSeries[i].DataPointCount
		}
		wantTotal := int64(v.summary.RequestsSent) * int64(v.summary.PointsPerRequest-2)
		v.check("histogram points across series match", total == wantTotal,
			fmt.Sprintf("total=%d expected=%d", total, wantTotal))
		if len(histSeries) > 0 {
			buckets, err := v.store.Buckets(ctx, histSeries[0].ID, 0, 0, 0)
			v.checkErr("query.Buckets on histogram series", err)
			if err == nil {
				want := 7 * int(histSeries[0].DataPointCount)
				v.check("histogram buckets queryable", len(buckets) == want,
					fmt.Sprintf("buckets=%d expected=%d (7 per point, %d points)",
						len(buckets), want, histSeries[0].DataPointCount))
				if len(buckets) > 0 {
					v.check("first bucket count positive", buckets[0].Count > 0,
						fmt.Sprintf("count=%d", buckets[0].Count))
				}
			}
		}
	}
}

// verifyCorrelation proves trace↔metric linkage: an exemplar trace id from
// the mockapp must appear both as a histogram exemplar (via json_each) and
// as a log_event.trace_id blob (the log path stores raw bytes).
func (v *verifier) verifyCorrelation() {
	traceHex := v.summary.SampleTraceID
	traceBytes, err := hex.DecodeString(traceHex)
	v.checkErr("decode sample trace id", err)
	if err != nil {
		return
	}

	// 1. Read side: data points whose exemplars reference the trace.
	hits, err := v.store.ByExemplarTrace(v.ctx, traceHex, 10)
	v.checkErr("query.ByExemplarTrace(sample trace)", err)
	if err == nil {
		v.check("exemplar hit found for sample trace", len(hits) > 0,
			fmt.Sprintf("hits=%d", len(hits)))
		if len(hits) > 0 {
			v.check("exemplar hit metric is the histogram", hits[0].MetricName == v.summary.HistogramMetric,
				fmt.Sprintf("metric=%q", hits[0].MetricName))
		}
	}

	// 2. Log side: a log_event row with the same trace id (stored as BLOB).
	var n int64
	if err := v.db.QueryRowContext(v.ctx,
		"SELECT COUNT(*) FROM log_event WHERE trace_id = ?", traceBytes,
	).Scan(&n); err != nil {
		v.checkErr("log_event trace_id lookup", err)
		return
	}
	v.check("log_event row matches exemplar trace id", n > 0,
		fmt.Sprintf("rows=%d", n))
}
