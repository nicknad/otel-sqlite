package otlp

import (
	"context"
	"testing"

	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/metrics"
	"codeberg.org/nicknad/otel-sqlite/internal/model"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1"
	metricsCollectorV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/metrics/v1"
	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	logsPB "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1"
	metricsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/metrics/v1"
)

// serverTestMetrics builds a Metrics with a private registry so tests do not
// conflict on the promauto default registerer. Only the counters the servers
// touch are populated.
func serverTestMetrics() *metrics.Metrics {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)
	return &metrics.Metrics{
		LogsReceived:           factory.NewCounter(prometheus.CounterOpts{Name: "logs_received_total"}),
		MetricsReceived:        factory.NewCounter(prometheus.CounterOpts{Name: "metrics_received_total"}),
		BackpressureRejections: factory.NewCounter(prometheus.CounterOpts{Name: "backpressure_rejections_total"}),
	}
}

func TestBackpressureRejected(t *testing.T) {
	tests := []struct {
		name      string
		threshold float64
		depth     int
		capacity  int
		want      bool
	}{
		{"disabled threshold", 0, 10, 10, false},
		{"disabled capacity", 0.8, 10, 0, false},
		{"below threshold", 0.8, 7, 10, false},
		{"exactly at threshold", 0.8, 8, 10, true},
		{"above threshold", 0.8, 9, 10, true},
		{"full queue", 0.5, 10, 10, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := backpressureRejected(tt.threshold, tt.depth, tt.capacity); got != tt.want {
				t.Errorf("backpressureRejected(%v,%d,%d) = %v, want %v",
					tt.threshold, tt.depth, tt.capacity, got, tt.want)
			}
		})
	}
}

func TestServer_ExportNilRequest(t *testing.T) {
	s := NewServer(nil, nil)
	resp, err := s.Export(context.Background(), nil)
	if err != nil {
		t.Fatalf("Export(nil): %v", err)
	}
	if resp == nil {
		t.Fatal("Export(nil) returned nil response")
	}
}

func TestServer_ExportNilIngressDisabled(t *testing.T) {
	s := NewServer(nil, nil)
	_, err := s.Export(context.Background(), &logsV1.ExportLogsServiceRequest{
		ResourceLogs: []*logsPB.ResourceLogs{{}},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Export with nil ingress = %v, want Unavailable", err)
	}
}

func TestMetricsServer_ExportNilIngressDisabled(t *testing.T) {
	s := NewMetricsServer(nil, nil)
	_, err := s.Export(context.Background(), &metricsCollectorV1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsV1.ResourceMetrics{{}},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Export with nil ingress = %v, want Unavailable", err)
	}
}

func TestServer_ExportBackpressureRejects(t *testing.T) {
	q := ingest.NewIngressQueue(1)
	// Fill the queue so depth == capacity.
	if err := q.Send(context.Background(), model.NewLogBatch(1)); err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	m := serverTestMetrics()

	s := NewServer(q, m)
	s.SetBackpressureThreshold(1.0)
	_, err := s.Export(context.Background(), &logsV1.ExportLogsServiceRequest{
		ResourceLogs: []*logsPB.ResourceLogs{{}},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Export with full queue = %v, want Unavailable", err)
	}
}

func TestMetricsServer_ExportBackpressureRejects(t *testing.T) {
	q := ingest.NewMetricIngressQueue(1)
	if err := q.Send(context.Background(), model.NewMetricBatch(1)); err != nil {
		t.Fatalf("seed queue: %v", err)
	}

	m := serverTestMetrics()
	s := NewMetricsServer(q, m)
	s.SetBackpressureThreshold(1.0)
	_, err := s.Export(context.Background(), &metricsCollectorV1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsV1.ResourceMetrics{{}},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Export with full queue = %v, want Unavailable", err)
	}
}

func TestServer_ExportSuccessIncrementsCounters(t *testing.T) {
	q := ingest.NewIngressQueue(10)
	m := serverTestMetrics()
	s := NewServer(q, m)

	req := &logsV1.ExportLogsServiceRequest{
		ResourceLogs: []*logsPB.ResourceLogs{
			{
				ScopeLogs: []*logsPB.ScopeLogs{
					{
						LogRecords: []*logsPB.LogRecord{
							{Body: &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: "a"}}},
							{Body: &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: "b"}}},
						},
					},
				},
			},
		},
	}
	if _, err := s.Export(context.Background(), req); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Queue now holds the mapped batch; drain to prove delivery.
	ctx := context.Background()
	batch, err := q.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if batch == nil || batch.Size() != 2 {
		t.Errorf("mapped batch size = %v, want 2", batch)
	}
}

func TestMetricsServer_ExportSuccessIncrementsCounters(t *testing.T) {
	q := ingest.NewMetricIngressQueue(10)
	m := serverTestMetrics()
	s := NewMetricsServer(q, m)

	req := &metricsCollectorV1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsV1.ResourceMetrics{
			{
				ScopeMetrics: []*metricsV1.ScopeMetrics{
					{
						Metrics: []*metricsV1.Metric{
							{
								Name: "server.test.gauge",
								Data: &metricsV1.Metric_Gauge{Gauge: &metricsV1.Gauge{
									DataPoints: []*metricsV1.NumberDataPoint{
										{Value: &metricsV1.NumberDataPoint_AsDouble{AsDouble: 1}},
										{Value: &metricsV1.NumberDataPoint_AsInt{AsInt: 2}},
									},
								}},
							},
						},
					},
				},
			},
		},
	}
	if _, err := s.Export(context.Background(), req); err != nil {
		t.Fatalf("Export: %v", err)
	}

	ctx := context.Background()
	batch, err := q.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if batch == nil || batch.Size() != 2 {
		t.Errorf("mapped batch size = %v, want 2", batch)
	}
}

func TestRegisterServer_DoesNotPanic(t *testing.T) {
	gs := grpc.NewServer()
	defer gs.Stop()

	RegisterServer(gs, NewServer(nil, nil))
	RegisterMetricsServer(gs, NewMetricsServer(nil, nil))

	if got := gs.GetServiceInfo(); len(got) != 2 {
		t.Errorf("registered services = %d, want 2 (logs + metrics)", len(got))
	}
}
