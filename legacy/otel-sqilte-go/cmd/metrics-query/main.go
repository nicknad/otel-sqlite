// Command metrics-query is a read-only CLI for stored OTLP metrics.
//
// It queries the `metrics` and `metric_buckets` views (migration 007)
// through the internal/query package: list time series, fetch data points
// in a time window, view normalized histogram buckets, and correlate data
// points to traces via exemplars.
//
// Examples:
//
//	metrics-query -db otel.db -series                       # list all series
//	metrics-query -db otel.db -metric http.server.requests  # series for a metric
//	metrics-query -db otel.db -service payments-api -points # points of that service
//	metrics-query -db otel.db -metric request.duration -buckets
//	metrics-query -db otel.db -trace aabb0100...ffff        # exemplar correlation
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/query"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dbPath := flag.String("db", "otel-logs.db", "path to the collector SQLite database")
	metricName := flag.String("metric", "", "filter series by metric name (exact)")
	serviceName := flag.String("service", "", "filter series by resource service.name (exact)")
	scopeName := flag.String("scope", "", "filter series by instrumentation scope name (exact)")
	showSeries := flag.Bool("series", true, "list matching time series")
	showPoints := flag.Bool("points", false, "also print data points for the matched series")
	showBuckets := flag.Bool("buckets", false, "print normalized histogram buckets for the matched series")
	traceID := flag.String("trace", "", "hex trace id: print data points whose exemplars reference it")
	from := flag.String("from", "", "data point window start: RFC3339 (e.g. 2025-08-15T10:00:00Z) or unix ns")
	to := flag.String("to", "", "data point window end: RFC3339 or unix ns")
	limit := flag.Int("limit", 100, "maximum number of rows per section")
	asc := flag.Bool("asc", false, "order data points ascending by timestamp (default descending)")
	asJSON := flag.Bool("json", false, "emit JSON instead of a table")
	flag.Parse()

	ctx := context.Background()

	// Parse the time window before opening the DB so argument errors fail
	// fast without leaking the store handle.
	fromNs, err := parseTime(*from)
	if err != nil {
		return fmt.Errorf("bad -from: %w", err)
	}
	toNs, err := parseTime(*to)
	if err != nil {
		return fmt.Errorf("bad -to: %w", err)
	}

	store, err := query.Open(*dbPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", *dbPath, err)
	}
	defer func() { _ = store.Close() }()

	// Exemplar correlation mode.
	if *traceID != "" {
		hits, err := store.ByExemplarTrace(ctx, strings.ToLower(*traceID), *limit)
		if err != nil {
			return fmt.Errorf("query exemplars: %w", err)
		}
		if *asJSON {
			emitJSON(hits)
			return nil
		}
		printExemplarHits(hits)
		return nil
	}

	filter := query.SeriesFilter{
		MetricName:  *metricName,
		ServiceName: *serviceName,
		ScopeName:   *scopeName,
		Limit:       *limit,
	}
	series, err := store.Series(ctx, filter)
	if err != nil {
		return fmt.Errorf("query series: %w", err)
	}

	if *asJSON {
		if *showPoints || *showBuckets {
			return errors.New("JSON mode supports one section at a time; use -series, -points or -buckets separately")
		}
		emitJSON(series)
		return nil
	}

	if *showSeries {
		printSeries(series)
	}

	if *showPoints {
		for i := range series {
			pts, err := store.DataPoints(ctx, query.DataPointFilter{
				SeriesID:  series[i].ID,
				FromNs:    fromNs,
				ToNs:      toNs,
				Limit:     *limit,
				Ascending: *asc,
			})
			if err != nil {
				return fmt.Errorf("query points for series %s: %w", series[i].ID, err)
			}
			printPoints(&series[i], pts)
		}
	}

	if *showBuckets {
		for i := range series {
			if series[i].Type != model.MetricTypeHistogram {
				continue
			}
			buckets, err := store.Buckets(ctx, series[i].ID, fromNs, toNs, *limit)
			if err != nil {
				return fmt.Errorf("query buckets for series %s: %w", series[i].ID, err)
			}
			printBuckets(&series[i], buckets)
		}
	}
	return nil
}

func parseTime(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	// Unix nanoseconds.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	// RFC3339.
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, fmt.Errorf("expected RFC3339 or unix ns, got %q", s)
	}
	return t.UnixNano(), nil
}

func printSeries(series []query.Series) {
	fmt.Printf("%-4s %-38s %-28s %-10s %-8s %-16s %-8s %s\n",
		"#", "series_id", "metric", "type", "unit", "service", "points", "attributes")
	for i := range series {
		attrs := series[i].AttributesJSON
		if attrs == "{}" {
			attrs = "(none)"
		}
		fmt.Printf("%-4d %-38s %-28s %-10s %-8s %-16s %-8d %s\n",
			i+1, shortID(series[i].ID), series[i].MetricName, series[i].Type,
			series[i].Unit, series[i].ServiceName, series[i].DataPointCount, attrs)
	}
	if len(series) == 0 {
		fmt.Println("(no matching series)")
	}
	fmt.Println()
}

func printPoints(s *query.Series, pts []query.DataPoint) {
	fmt.Printf("data points for series %s (metric=%s):\n", shortID(s.ID), s.MetricName)
	if len(pts) == 0 {
		fmt.Println("  (none)")
		return
	}
	for i := range pts {
		p := &pts[i]
		val := "—"
		switch {
		case p.DoubleValue != nil:
			val = fmt.Sprintf("%v", *p.DoubleValue)
		case p.IntValue != nil:
			val = strconv.FormatInt(*p.IntValue, 10)
		case p.Count != nil:
			val = fmt.Sprintf("count=%d sum=%s", *p.Count, fstr(p.Sum))
		}
		ex := ""
		if len(p.ExemplarsJSON) > 0 {
			ex = " exemplars"
		}
		fmt.Printf("  t=%-22d %-20s flags=%d%s\n", p.TimestampNs, val, p.Flags, ex)
	}
	fmt.Println()
}

func printBuckets(s *query.Series, buckets []query.Bucket) {
	fmt.Printf("histogram buckets for series %s (metric=%s):\n", shortID(s.ID), s.MetricName)
	if len(buckets) == 0 {
		fmt.Println("  (none)")
		return
	}
	for i := range buckets {
		b := &buckets[i]
		bound := b.BoundJSON
		if b.Bound != nil {
			bound = fmt.Sprintf("%v", *b.Bound)
		}
		fmt.Printf("  t=%-22d bucket[%d] bound=%-12s count=%d\n",
			b.TimestampNs, b.Index, bound, b.Count)
	}
	fmt.Println()
}

func printExemplarHits(hits []query.ExemplarHit) {
	fmt.Printf("data points with exemplars referencing the trace:\n")
	if len(hits) == 0 {
		fmt.Println("  (none)")
		return
	}
	for i := range hits {
		h := &hits[i]
		val := "—"
		switch {
		case h.ExemplarDouble != nil:
			val = fmt.Sprintf("%v", *h.ExemplarDouble)
		case h.ExemplarInt != nil:
			val = strconv.FormatInt(*h.ExemplarInt, 10)
		}
		fmt.Printf("  point=%d metric=%-35s service=%-20s span=%s value=%s\n",
			h.DataPointID, h.MetricName, h.ServiceName, h.SpanID, val)
	}
	fmt.Println()
}

func emitJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		log.Printf("encode: %v", err)
	}
}

func shortID(id string) string {
	if len(id) > 20 {
		return id[:20] + "…"
	}
	return id
}

func fstr(v *float64) string {
	if v == nil {
		return "—"
	}
	return fmt.Sprintf("%v", *v)
}
