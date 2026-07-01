// Command loadtest is a load generator for the OTLP SQLite collector.
//
// It simulates several "mock APIs" (concurrent gRPC clients) that push OTLP
// log export requests to a running collector and reports the sustained
// ingestion throughput (records/second and requests/second acknowledged by
// the collector) plus error rate. It can also scrape the collector's
// Prometheus /metrics endpoint to report logs-received and logs-written
// counters so an ingest-vs-process baseline can be established.
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

	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1"
	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	logsPB "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1"
	resourceV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1"
)

func main() {
	addr := flag.String("addr", "localhost:4317", "collector gRPC address")
	metrics := flag.String(
		"metrics", "",
		"collector Prometheus /metrics URL (e.g. http://localhost:9090/metrics); empty disables scraping")
	clients := flag.Int("clients", 16, "number of mock-API clients (concurrent gRPC senders)")
	records := flag.Int("records", 1000, "number of log records per export request")
	duration := flag.Duration("duration", 30*time.Second, "load test duration")
	rpsPerClient := flag.Int("rps-per-client", 0, "max requests/second per client (0 = uncapped)")
	attrsPerRecord := flag.Int("attrs", 4, "attributes per log record")
	resourceCount := flag.Int("resources", 8, "number of distinct resources (services) simulated across all clients")
	flag.Parse()

	if *clients <= 0 || *records <= 0 || *duration <= 0 {
		log.Fatal("clients, records and duration must be positive")
	}

	// Shared gRPC connection (HTTP/2 multiplexes concurrent streams).
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial %s: %v", *addr, err)
	}
	defer func() { _ = conn.Close() }()
	client := logsV1.NewLogsServiceClient(conn)

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

	log.Printf("load test: addr=%s clients=%d records/req=%d attrs/record=%d resources=%d duration=%s rps/client=%d",
		*addr, *clients, *records, *attrsPerRecord, *resourceCount, *duration, *rpsPerClient)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		svc := resources[i%len(resources)]
		go func(id int) {
			defer wg.Done()
			runClient(ctx, client, id, *records, *attrsPerRecord, svc,
				*rpsPerClient, &totalRequests, &totalRecords, &totalErrors, &totalErrorBytes)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Give the pipeline a moment to drain before the final scrape.
	time.Sleep(3 * time.Second)

	fmt.Println()
	fmt.Println("================ LOAD TEST RESULTS ================")
	fmt.Printf("Duration (client active):       %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Mock API clients:              %d\n", *clients)
	fmt.Printf("Records per request:          %d\n", *records)
	totalReq := totalRequests.Load()
	totalRec := totalRecords.Load()
	totalErr := totalErrors.Load()
	fmt.Printf("Export requests sent:          %d\n", totalReq)
	fmt.Printf("Records sent:                  %d\n", totalRec)
	fmt.Printf("Export errors:                 %d (%.2f%% of requests)\n",
		totalErr, percent(totalErr, totalReq))
	fmt.Printf("Ingest rate (records):         %.2f rec/s\n", float64(totalRec)/elapsed.Seconds())
	fmt.Printf("Ingest rate (requests):        %.2f req/s\n", float64(totalReq)/elapsed.Seconds())
	if totalErrorBytes.Load() > 0 {
		fmt.Printf("Error bytes (gRPC msg):        %d\n", totalErrorBytes.Load())
	}

	if *metrics != "" {
		after := scrapeMetrics(*metrics)
		fmt.Println()
		fmt.Println("----------- Prometheus counters -----------")
		fmt.Printf("logs_received_total:           %s -> %s (delta %s)\n",
			baseline.logsReceived, after.logsReceived, subCounters(after.logsReceived, baseline.logsReceived))
		fmt.Printf("logs_written_total:            %s -> %s (delta %s)\n",
			baseline.logsWritten, after.logsWritten, subCounters(after.logsWritten, baseline.logsWritten))
		fmt.Printf("batches_written_total:         %s -> %s (delta %s)\n",
			baseline.batchesWritten, after.batchesWritten, subCounters(after.batchesWritten, baseline.batchesWritten))
		fmt.Printf("write_errors_total:            %s -> %s (delta %s)\n",
			baseline.writeErrors, after.writeErrors, subCounters(after.writeErrors, baseline.writeErrors))
		if after.logsReceived != "" && baseline.logsReceived != "" {
			if d, err := diff(after.logsReceived, baseline.logsReceived); err == nil {
				fmt.Printf("Confirmed received (metrics):  %d (%.2f/s)\n",
					d, float64(d)/elapsed.Seconds())
			}
		}
		if after.logsWritten != "" && baseline.logsWritten != "" {
			if d, err := diff(after.logsWritten, baseline.logsWritten); err == nil {
				fmt.Printf("Confirmed written (metrics):   %d (%.2f/s)  <-- PROCESS rate\n",
					d, float64(d)/elapsed.Seconds())
			}
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

type metricsSnapshot struct {
	logsReceived   string
	logsWritten    string
	batchesWritten string
	writeErrors    string
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
	return s
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
	ai, errA := strconv.ParseInt(a, 10, 64)
	bi, errB := strconv.ParseInt(b, 10, 64)
	if errA != nil || errB != nil {
		return 0, fmt.Errorf("parse")
	}
	return ai - bi, nil
}

func subCounters(a, b string) string {
	d, err := diff(a, b)
	if err != nil {
		return "n/a"
	}
	return strconv.FormatInt(d, 10)
}
