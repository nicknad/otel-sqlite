// Command loadtest is a load generator for the OTLP SQLite collector.
//
// It simulates several "mock APIs" (concurrent gRPC clients) that push OTLP
// log or metric export requests to a running collector and reports the
// sustained ingestion throughput (records/second and requests/second
// acknowledged by the collector) plus error rate. It can also scrape the
// collector's Prometheus /metrics endpoint to report received and written
// counters so an ingest-vs-process baseline can be established.
//
// Use -signal metrics to exercise the OTLP metrics ingestion path (data
// points per request, gauge/sum/histogram mix, exemplars with trace ids) and
// -signal mixed to drive both signals concurrently against the shared
// command queue/writer (even client ids send logs, odd send metrics).
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metricsCollectorV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/metrics/v1"
	metricsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/metrics/v1"

	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1"
	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	logsPB "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1"
	resourceV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1"
)

func main() {
	addr := flag.String("addr", "localhost:4317", "collector gRPC address")
	signalName := flag.String("signal", "logs", "signal to generate: \"logs\", \"metrics\" or \"mixed\"")
	metrics := flag.String(
		"metrics", "",
		"collector Prometheus /metrics URL (e.g. http://localhost:9090/metrics); empty disables scraping",
	)
	clients := flag.Int("clients", 16, "number of mock-API clients (concurrent gRPC senders)")
	records := flag.Int("records", 1000, "number of log records (or metric data points) per export request")
	duration := flag.Duration("duration", 30*time.Second, "load test duration")
	rpsPerClient := flag.Int("rps-per-client", 0, "max requests/second per client (0 = uncapped)")
	attrsPerRecord := flag.Int("attrs", 4, "attributes per log record (or per metric series)")
	resourceCount := flag.Int("resources", 8, "number of distinct resources (services) simulated across all clients")
	drainTimeout := flag.Duration(
		"drain-timeout", 2*time.Minute,
		"how long to wait after clients stop for written counters to catch received counters (0 disables)",
	)
	flag.Parse()

	if *clients <= 0 || *records <= 0 || *duration <= 0 {
		log.Fatal("clients, records and duration must be positive")
	}
	if *drainTimeout < 0 {
		log.Fatal("drain-timeout must be >= 0")
	}
	switch *signalName {
	case "logs", "metrics", "mixed":
	default:
		log.Fatalf("signal must be \"logs\", \"metrics\" or \"mixed\", got %q", *signalName)
	}

	// Shared gRPC connection (HTTP/2 multiplexes concurrent streams).
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial %s: %v", *addr, err)
	}
	defer func() { _ = conn.Close() }()

	// Pre-build a pool of distinct resources/services so resources are reused
	// (exercises the dedup path in the writer).
	resources := makeResourcePool(*resourceCount)

	// Per-client counters.
	var (
		totalRequests   atomic.Uint64
		totalRecords    atomic.Uint64
		totalErrors     atomic.Uint64
		totalErrorBytes atomic.Uint64
	)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	// Capture Ctrl-C / SIGTERM early to print partial results.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		select {
		case <-sig:
			log.Println("interrupt received, finishing early...")
			cancel()
		case <-ctx.Done():
		}
	}()

	// Baseline metrics snapshot before load starts.
	var baseline metricsSnapshot
	if *metrics != "" {
		baseline = scrapeMetrics(*metrics)
	}

	log.Printf("load test: signal=%s addr=%s clients=%d records/req=%d attrs=%d resources=%d duration=%s rps/client=%d",
		*signalName, *addr, *clients, *records, *attrsPerRecord, *resourceCount, *duration, *rpsPerClient)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		svc := resources[i%len(resources)]
		// In mixed mode clients alternate signals (even id = logs, odd id =
		// metrics) so both paths feed the shared command queue concurrently.
		isMetric := *signalName == "metrics" || (*signalName == "mixed" && i%2 == 1)
		if isMetric {
			mclient := metricsCollectorV1.NewMetricsServiceClient(conn)
			go func(id int) {
				defer wg.Done()
				runMetricsClient(ctx, mclient, id, *records, *attrsPerRecord, svc,
					*rpsPerClient, &totalRequests, &totalRecords, &totalErrors, &totalErrorBytes)
			}(i)
		} else {
			lclient := logsV1.NewLogsServiceClient(conn)
			go func(id int) {
				defer wg.Done()
				runClient(ctx, lclient, id, *records, *attrsPerRecord, svc,
					*rpsPerClient, &totalRequests, &totalRecords, &totalErrors, &totalErrorBytes)
			}(i)
		}
	}
	wg.Wait()
	clientElapsed := time.Since(start)

	fmt.Println()
	fmt.Println("================ LOAD TEST RESULTS ================")
	fmt.Printf("Signal:                          %s\n", *signalName)
	fmt.Printf("Duration (client active):       %s\n", clientElapsed.Round(time.Millisecond))
	fmt.Printf("Mock API clients:              %d\n", *clients)
	fmt.Printf("Records per request:          %d\n", *records)
	totalReq := totalRequests.Load()
	totalRec := totalRecords.Load()
	totalErr := totalErrors.Load()
	fmt.Printf("Export requests sent:          %d\n", totalReq)
	fmt.Printf("Records sent:                  %d\n", totalRec)
	fmt.Printf("Export errors:                 %d (%.2f%% of requests)\n",
		totalErr, percent(totalErr, totalReq))
	fmt.Printf("Ingest rate (records):         %.2f rec/s\n", float64(totalRec)/clientElapsed.Seconds())
	fmt.Printf("Ingest rate (requests):        %.2f req/s\n", float64(totalReq)/clientElapsed.Seconds())
	if totalErrorBytes.Load() > 0 {
		fmt.Printf("Error bytes (gRPC msg):        %d\n", totalErrorBytes.Load())
	}

	if *metrics != "" {
		// Snapshot immediately after clients stop: process rate during the
		// active window uses this delta / clientElapsed (not drain time).
		activeEnd := scrapeMetrics(*metrics)

		var (
			after     = activeEnd
			drainTook time.Duration
			drained   bool
		)
		// Signals present in this run; the drain check must cover all of them.
		signals := []sigCounter{{name: "logs", recvField: "logsReceived", writField: "logsWritten"}}
		if *signalName == "metrics" || *signalName == "mixed" {
			signals = append(signals, sigCounter{name: "metrics", recvField: "metricsReceived", writField: "metricPointsWritten"})
		}
		if *drainTimeout > 0 {
			drainStart := time.Now()
			deadline := drainStart.Add(*drainTimeout)
			for {
				after = scrapeMetrics(*metrics)
				allDrained := true
				for _, sig := range signals {
					recv, _ := diffCounter(&after, &baseline, sig.recvField)
					writ, _ := diffCounter(&after, &baseline, sig.writField)
					if recv > 0 && writ < recv {
						allDrained = false
						break
					}
				}
				if allDrained {
					drained = true
					drainTook = time.Since(drainStart)
					break
				}
				if time.Now().After(deadline) {
					drainTook = time.Since(drainStart)
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
		}

		fmt.Println()
		fmt.Println("----------- Prometheus counters -----------")
		if *signalName == "logs" || *signalName == "mixed" {
			fmt.Printf("logs_received_total:           %s -> %s (delta %s)\n",
				baseline.logsReceived, after.logsReceived, subCounters(after.logsReceived, baseline.logsReceived))
			fmt.Printf("logs_written_total:            %s -> %s (delta %s)\n",
				baseline.logsWritten, after.logsWritten, subCounters(after.logsWritten, baseline.logsWritten))
		}
		if *signalName == "metrics" || *signalName == "mixed" {
			fmt.Printf("metrics_received_total:         %s -> %s (delta %s)\n",
				baseline.metricsReceived, after.metricsReceived, subCounters(after.metricsReceived, baseline.metricsReceived))
			fmt.Printf("metric_data_points_written_total: %s -> %s (delta %s)\n",
				baseline.metricPointsWritten, after.metricPointsWritten, subCounters(after.metricPointsWritten, baseline.metricPointsWritten))
		}
		fmt.Printf("batches_written_total:         %s -> %s (delta %s)\n",
			baseline.batchesWritten, after.batchesWritten, subCounters(after.batchesWritten, baseline.batchesWritten))
		fmt.Printf("write_errors_total:            %s -> %s (delta %s)\n",
			baseline.writeErrors, after.writeErrors, subCounters(after.writeErrors, baseline.writeErrors))

		// Per-signal process rate during the client-active window (the ceiling)
		// and confirmed final write totals, with a combined rate for mixed runs.
		for _, sig := range signals {
			writFinal, errW := diffCounter(&after, &baseline, sig.writField)
			recvFinal, errR := diffCounter(&after, &baseline, sig.recvField)
			writActive, _ := diffCounter(&activeEnd, &baseline, sig.writField)
			if errW == nil && writFinal > 0 {
				activeRate := float64(writActive) / clientElapsed.Seconds()
				fmt.Printf("\nProcess rate (%s, client window): %d written / %s = %.2f rec/s\n",
					sig.name, writActive, clientElapsed.Round(time.Millisecond), activeRate)
				fmt.Printf("Confirmed written (%s, final):     %d records\n", sig.name, writFinal)
			}
			if errR == nil && errW == nil && recvFinal > 0 && writFinal < recvFinal {
				gap := recvFinal - writFinal
				pct := float64(gap) / float64(recvFinal) * 100
				if pct > 5.0 {
					log.Printf("\n*** WARNING: possible data loss (%s): received=%d written=%d gap=%d (%.1f%%)\n",
						sig.name, recvFinal, writFinal, gap, pct)
				}
			}
		}
		if *signalName == "mixed" {
			writLogs, _ := diffCounter(&activeEnd, &baseline, "logsWritten")
			writMetrics, _ := diffCounter(&activeEnd, &baseline, "metricPointsWritten")
			if combined := writLogs + writMetrics; combined > 0 {
				fmt.Printf("\nProcess rate (combined, client window): %d written / %s = %.2f rec/s\n",
					combined, clientElapsed.Round(time.Millisecond), float64(combined)/clientElapsed.Seconds())
			}
		}
		if *drainTimeout > 0 {
			status := "incomplete"
			if drained {
				status = "complete"
			}
			fmt.Printf("\nDrain after clients stopped:  %s (%s, timeout %s)\n",
				drainTook.Round(time.Millisecond), status, *drainTimeout)
		}
	}
	fmt.Println("===================================================")

	if totalErr > 0 {
		log.Println("load test completed with errors")
		return
	}
}

func runClient(
	ctx context.Context,
	client logsV1.LogsServiceClient,
	id int,
	records, attrs int,
	svc *resourceV1.Resource,
	rpsLimit int,
	totalReq, totalRec, totalErr, totalErrBytes *atomic.Uint64,
) {
	// Per-client rate limiter.
	var interval time.Duration
	if rpsLimit > 0 {
		interval = time.Second / time.Duration(rpsLimit)
	}
	ticker := &time.Ticker{}
	if interval > 0 {
		t := time.NewTicker(interval)
		defer t.Stop()
		ticker = t
	}

	scope := &commonV1.InstrumentationScope{Name: fmt.Sprintf("mock-api-%d", id), Version: "1.0.0"}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if interval > 0 {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}

		req := buildExportRequest(svc, scope, id, records, attrs)
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := client.Export(callCtx, req)
		cancel()

		if err != nil {
			totalErr.Add(1)
			totalErrBytes.Add(uint64(len(err.Error()))) //nolint:gosec // G115: bounded by err msg length
			continue
		}
		totalReq.Add(1)
		totalRec.Add(uint64(records)) //nolint:gosec // G115: bounded flag value
	}
}

func buildExportRequest(
	resource *resourceV1.Resource,
	scope *commonV1.InstrumentationScope,
	clientID, records, attrs int,
) *logsV1.ExportLogsServiceRequest {
	now := uint64(time.Now().UnixNano()) //nolint:gosec // G115: timestamp fits
	logRecords := make([]*logsPB.LogRecord, records)
	for i := 0; i < records; i++ {
		msg := fmt.Sprintf("client=%d msg=%d", clientID, i)
		body := &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: msg}}
		ui := uint64(i) //nolint:gosec // G115: loop counter
		logRecords[i] = &logsPB.LogRecord{
			TimeUnixNano:           now + ui,
			ObservedTimeUnixNano:   now,
			SeverityNumber:         logsPB.SeverityNumber_SEVERITY_NUMBER_INFO,
			SeverityText:           "INFO",
			Body:                   body,
			TraceId:                randomTraceID(clientID, i),
			SpanId:                 randomSpanID(clientID, i),
			Attributes:             buildAttrs(clientID, i, attrs),
			DroppedAttributesCount: 0,
			Flags:                  1,
			EventName:              "loadtest.event",
		}
	}

	return &logsV1.ExportLogsServiceRequest{
		ResourceLogs: []*logsPB.ResourceLogs{
			{
				Resource: resource,
				ScopeLogs: []*logsPB.ScopeLogs{
					{
						Scope:      scope,
						LogRecords: logRecords,
					},
				},
			},
		},
	}
}

// runMetricsClient is the metrics analogue of runClient: it exports OTLP
// metric requests carrying `points` data points each.
func runMetricsClient(
	ctx context.Context,
	client metricsCollectorV1.MetricsServiceClient,
	id int,
	points, attrs int,
	svc *resourceV1.Resource,
	rpsLimit int,
	totalReq, totalRec, totalErr, totalErrBytes *atomic.Uint64,
) {
	// Per-client rate limiter.
	var interval time.Duration
	if rpsLimit > 0 {
		interval = time.Second / time.Duration(rpsLimit)
	}
	ticker := &time.Ticker{}
	if interval > 0 {
		t := time.NewTicker(interval)
		defer t.Stop()
		ticker = t
	}

	scope := &commonV1.InstrumentationScope{Name: fmt.Sprintf("mock-api-%d", id), Version: "1.0.0"}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if interval > 0 {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}

		req := buildMetricsExportRequest(svc, scope, id, points, attrs)
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := client.Export(callCtx, req)
		cancel()

		if err != nil {
			totalErr.Add(1)
			totalErrBytes.Add(uint64(len(err.Error()))) //nolint:gosec // G115: bounded by err msg length
			continue
		}
		totalReq.Add(1)
		totalRec.Add(uint64(points)) //nolint:gosec // G115: bounded flag value
	}
}

// buildMetricsExportRequest builds one ExportMetricsServiceRequest with
// `points` data points split across three metric types (gauge, sum,
// histogram) so the whole storage path is exercised. Histogram points carry
// an exemplar with a trace id for trace↔metric correlation.
func buildMetricsExportRequest(
	resource *resourceV1.Resource,
	scope *commonV1.InstrumentationScope,
	clientID, points, attrs int,
) *metricsCollectorV1.ExportMetricsServiceRequest {
	now := uint64(time.Now().UnixNano()) //nolint:gosec // G115: timestamp fits

	// Split points across three metrics: gauge (40%), sum (40%), histogram (20%).
	nGauge := points * 40 / 100
	nSum := points * 40 / 100
	nHist := points - nGauge - nSum

	gaugePoints := make([]*metricsV1.NumberDataPoint, 0, nGauge)
	sumPoints := make([]*metricsV1.NumberDataPoint, 0, nSum)
	histPoints := make([]*metricsV1.HistogramDataPoint, 0, nHist)

	for i := 0; i < nGauge; i++ {
		ui := uint64(i) //nolint:gosec // G115: loop counter
		gaugePoints = append(gaugePoints, &metricsV1.NumberDataPoint{
			TimeUnixNano: now + ui,
			Attributes:   buildAttrs(clientID, i, attrs),
			Value: &metricsV1.NumberDataPoint_AsDouble{
				AsDouble: float64(i%1000) / 10, //nolint:gosec // G115: bounded
			},
		})
	}
	for i := 0; i < nSum; i++ {
		ui := uint64(i) //nolint:gosec // G115: loop counter
		sumPoints = append(sumPoints, &metricsV1.NumberDataPoint{
			TimeUnixNano: now + ui,
			Attributes:   buildAttrs(clientID, i, attrs),
			Value: &metricsV1.NumberDataPoint_AsInt{
				AsInt: int64(i), //nolint:gosec // G115: bounded
			},
		})
	}
	for i := 0; i < nHist; i++ {
		ui := uint64(i) //nolint:gosec // G115: loop counter
		traceID := randomTraceID(clientID, i)
		spanID := randomSpanID(clientID, i)
		sum := float64(i % 1000) //nolint:gosec // G115: bounded
		histPoints = append(histPoints, &metricsV1.HistogramDataPoint{
			TimeUnixNano:   now + ui,
			Attributes:     buildAttrs(clientID, i, attrs),
			Count:          uint64(10 + i%50), //nolint:gosec // G115: bounded
			Sum:            &sum,
			ExplicitBounds: []float64{0.1, 0.5, 1, 2.5},
			BucketCounts:   []uint64{4, 3, 2, 1, 1},
			Exemplars: []*metricsV1.Exemplar{
				{
					TimeUnixNano: now + ui,
					TraceId:      traceID,
					SpanId:       spanID,
					Value: &metricsV1.Exemplar_AsDouble{
						AsDouble: 0.75,
					},
					FilteredAttributes: buildAttrs(clientID, i, 2),
				},
			},
		})
	}

	metrics := make([]*metricsV1.Metric, 0, 3)
	if nGauge > 0 {
		metrics = append(metrics, &metricsV1.Metric{
			Name: fmt.Sprintf("loadtest.gauge.%d", clientID),
			Data: &metricsV1.Metric_Gauge{Gauge: &metricsV1.Gauge{DataPoints: gaugePoints}},
		})
	}
	if nSum > 0 {
		metrics = append(metrics, &metricsV1.Metric{
			Name: fmt.Sprintf("loadtest.sum.%d", clientID),
			Data: &metricsV1.Metric_Sum{Sum: &metricsV1.Sum{
				IsMonotonic:            true,
				AggregationTemporality: metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				DataPoints:             sumPoints,
			}},
		})
	}
	if nHist > 0 {
		metrics = append(metrics, &metricsV1.Metric{
			Name: fmt.Sprintf("loadtest.hist.%d", clientID),
			Data: &metricsV1.Metric_Histogram{Histogram: &metricsV1.Histogram{
				AggregationTemporality: metricsV1.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
				DataPoints:             histPoints,
			}},
		})
	}

	return &metricsCollectorV1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsV1.ResourceMetrics{
			{
				Resource: resource,
				ScopeMetrics: []*metricsV1.ScopeMetrics{
					{
						Scope:   scope,
						Metrics: metrics,
					},
				},
			},
		},
	}
}

func makeResourcePool(n int) []*resourceV1.Resource {
	out := make([]*resourceV1.Resource, n)
	for i := 0; i < n; i++ {
		svcName := fmt.Sprintf("mock-api-%d", i)
		hostName := fmt.Sprintf("host-%d", i%4)
		strVal := func(s string) *commonV1.AnyValue {
			return &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: s}}
		}
		out[i] = &resourceV1.Resource{
			Attributes: []*commonV1.KeyValue{
				{Key: "service.name", Value: strVal(svcName)},
				{Key: "host.name", Value: strVal(hostName)},
				{Key: "deployment.environment", Value: strVal("loadtest")},
			},
		}
	}
	return out
}

func buildAttrs(clientID, idx, attrs int) []*commonV1.KeyValue {
	if attrs <= 0 {
		return nil
	}
	out := make([]*commonV1.KeyValue, 0, attrs)
	for j := 0; j < attrs; j++ {
		out = append(out, &commonV1.KeyValue{
			Key: fmt.Sprintf("attr_%d", j),
			Value: &commonV1.AnyValue{
				Value: &commonV1.AnyValue_StringValue{
					StringValue: fmt.Sprintf("v-%d-%d-%d", clientID, idx, j),
				},
			},
		})
	}
	return out
}

// deterministic-ish pseudo-random IDs so each record has trace/span context.
func randomTraceID(clientID, idx int) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[0:8], uint64(clientID)) //nolint:gosec // G115: bounded
	binary.LittleEndian.PutUint64(b[8:16], uint64(idx))     //nolint:gosec // G115: bounded
	return b
}

func randomSpanID(clientID, idx int) []byte {
	b := make([]byte, 8)
	span := uint64(clientID)*1_000_003 + uint64(idx) //nolint:gosec // G115: bounded
	binary.LittleEndian.PutUint64(b, span)
	return b
}

// ---------- metrics scraping ----------

// sigCounter names one signal's received/written counter pair for drain
// checking and per-signal rate reporting (mixed runs track both).
type sigCounter struct {
	name      string
	recvField string
	writField string
}

type metricsSnapshot struct {
	logsReceived        string
	logsWritten         string
	batchesWritten      string
	writeErrors         string
	metricsReceived     string
	metricPointsWritten string
}

func scrapeMetrics(url string) metricsSnapshot {
	var s metricsSnapshot
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return s
	}
	cli := &http.Client{Timeout: 5 * time.Second}
	resp, err := cli.Do(req)
	if err != nil {
		log.Printf("metrics scrape failed: %v", err)
		return s
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return s
	}
	s.logsReceived = findCounter(body, "otel_collector_ingest_logs_received_total")
	s.logsWritten = findCounter(body, "otel_collector_storage_logs_written_total")
	s.batchesWritten = findCounter(body, "otel_collector_storage_batches_written_total")
	s.writeErrors = findCounter(body, "otel_collector_storage_write_errors_total")
	s.metricsReceived = findCounter(body, "otel_collector_ingest_metrics_received_total")
	s.metricPointsWritten = findCounter(body, "otel_collector_storage_metric_data_points_written_total")
	return s
}

// diffCounter returns the delta of a named counter field between two
// snapshots. field is one of: logsReceived, logsWritten, metricsReceived,
// metricPointsWritten, batchesWritten, writeErrors.
func diffCounter(a, b *metricsSnapshot, field string) (int64, error) {
	switch field {
	case "logsReceived":
		return diff(a.logsReceived, b.logsReceived)
	case "logsWritten":
		return diff(a.logsWritten, b.logsWritten)
	case "metricsReceived":
		return diff(a.metricsReceived, b.metricsReceived)
	case "metricPointsWritten":
		return diff(a.metricPointsWritten, b.metricPointsWritten)
	case "batchesWritten":
		return diff(a.batchesWritten, b.batchesWritten)
	case "writeErrors":
		return diff(a.writeErrors, b.writeErrors)
	default:
		return 0, fmt.Errorf("unknown counter field %q", field)
	}
}

func findCounter(body []byte, name string) string {
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		// Lines look like: otel_collector_storage_logs_written_total 1234
		// or with labels:  otel_collector_foo_total{...} 1234
		if !strings.HasPrefix(line, name) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		return fields[len(fields)-1]
	}
	return ""
}

func percent(n, d uint64) float64 {
	if d == 0 {
		return 0
	}
	return float64(n) / float64(d) * 100
}

func diff(a, b string) (int64, error) {
	// Prometheus may format large counters with scientific notation
	// (e.g. "2.283e+06"), so parse as float64 then convert.
	af, errA := strconv.ParseFloat(a, 64)
	bf, errB := strconv.ParseFloat(b, 64)
	if errA != nil || errB != nil {
		return 0, fmt.Errorf("parse")
	}
	return int64(af - bf), nil
}

func subCounters(a, b string) string {
	d, err := diff(a, b)
	if err != nil {
		return "n/a"
	}
	return strconv.FormatInt(d, 10)
}
