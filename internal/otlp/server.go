// Package otlp provides the gRPC servers for receiving OTLP logs and metrics.
// This package handles the transport layer and delegates to the mapper.
package otlp

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/metrics"

	// OTLP protobuf imports
	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1"
	metricsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/metrics/v1"
)

// backpressureRejected reports whether early rejection should trigger: the
// queue depth exceeds the given fraction of capacity.
func backpressureRejected(threshold float64, depth, capacity int) bool {
	if threshold <= 0 || capacity <= 0 {
		return false
	}
	return float64(depth)/float64(capacity) >= threshold
}

// Server implements the OTLP gRPC logs service.
type Server struct {
	logsV1.UnimplementedLogsServiceServer

	mapper       *Mapper
	ingressQueue ingest.IngressQueue
	metrics      *metrics.Metrics

	// backpressureThreshold is the fraction of ingress queue capacity at which
	// the server rejects new requests (0 = disabled).
	backpressureThreshold float64
}

// NewServer creates a new OTLP gRPC logs server.
//
// If backpressureThreshold > 0, the server rejects Export requests when the
// ingress queue depth exceeds that fraction of its capacity, returning
// Unavailable to clients instead of blocking. This trades throughput for
// memory safety by preventing queue buildup under sustained load.
func NewServer(ingressQueue ingest.IngressQueue, m *metrics.Metrics) *Server {
	return &Server{
		mapper:       NewMapper(),
		ingressQueue: ingressQueue,
		metrics:      m,
	}
}

// SetBackpressureThreshold enables early rejection when the ingress queue
// fullness exceeds the given fraction (0.0–1.0) of capacity.
func (s *Server) SetBackpressureThreshold(threshold float64) {
	s.backpressureThreshold = threshold
}

// Export implements the Export method of the LogsServiceServer interface.
func (s *Server) Export(ctx context.Context, request *logsV1.ExportLogsServiceRequest) (*logsV1.ExportLogsServiceResponse, error) { //nolint:lll
	if request == nil || request.ResourceLogs == nil {
		return &logsV1.ExportLogsServiceResponse{}, nil
	}

	if s.ingressQueue == nil {
		return nil, status.Error(codes.Unavailable, "log ingestion is disabled")
	}

	// Optional early backpressure: reject before mapping if the ingress queue
	// is already too full, protecting memory even under extreme client concurrency.
	if backpressureRejected(s.backpressureThreshold, s.ingressQueue.Len(), s.ingressQueue.Cap()) {
		s.metrics.IncrementBackpressureRejections()
		fullness := int(float64(s.ingressQueue.Len()) / float64(s.ingressQueue.Cap()) * 100)
		return nil, status.Errorf(codes.Unavailable, "server overloaded: ingress queue %d%% full", fullness)
	}

	// Map OTLP request to internal batches
	batches := s.mapper.MapLogsData(request.ResourceLogs)

	// Count received logs
	totalLogs := 0
	for _, batch := range batches {
		totalLogs += batch.Size()
	}

	if totalLogs > 0 {
		s.metrics.IncrementLogsReceived(totalLogs)
	}

	// Send each batch to the ingress queue (1 channel op per batch, not per record)
	for _, batch := range batches {
		if err := s.ingressQueue.Send(ctx, batch); err != nil {
			return nil, status.Errorf(codes.Unavailable, "ingress queue full: %v", err)
		}
	}

	return &logsV1.ExportLogsServiceResponse{}, nil
}

// RegisterServer registers the OTLP logs service with a gRPC server.
func RegisterServer(grpcServer *grpc.Server, server *Server) {
	logsV1.RegisterLogsServiceServer(grpcServer, server)
}

// MetricsServer implements the OTLP gRPC metrics service. OTLP metrics are
// application telemetry being ingested and stored in SQLite — they are never
// exposed on the collector's Prometheus /metrics endpoint.
type MetricsServer struct {
	metricsV1.UnimplementedMetricsServiceServer

	mapper       *Mapper
	ingressQueue ingest.MetricIngressQueue
	metrics      *metrics.Metrics

	// backpressureThreshold is the fraction of ingress queue capacity at which
	// the server rejects new requests (0 = disabled).
	backpressureThreshold float64
}

// NewMetricsServer creates a new OTLP gRPC metrics server. ingressQueue may
// be nil when metric ingestion is disabled; Export then returns Unavailable.
func NewMetricsServer(ingressQueue ingest.MetricIngressQueue, m *metrics.Metrics) *MetricsServer {
	return &MetricsServer{
		mapper:       NewMapper(),
		ingressQueue: ingressQueue,
		metrics:      m,
	}
}

// SetBackpressureThreshold enables early rejection when the ingress queue
// fullness exceeds the given fraction (0.0–1.0) of capacity.
func (s *MetricsServer) SetBackpressureThreshold(threshold float64) {
	s.backpressureThreshold = threshold
}

// Export implements the Export method of the MetricsServiceServer interface.
func (s *MetricsServer) Export(ctx context.Context, request *metricsV1.ExportMetricsServiceRequest) (*metricsV1.ExportMetricsServiceResponse, error) { //nolint:lll
	if request == nil || request.ResourceMetrics == nil {
		return &metricsV1.ExportMetricsServiceResponse{}, nil
	}
	if s.ingressQueue == nil {
		return nil, status.Error(codes.Unavailable, "metric ingestion is disabled")
	}

	// Optional early backpressure, same policy as the logs path.
	if backpressureRejected(s.backpressureThreshold, s.ingressQueue.Len(), s.ingressQueue.Cap()) {
		s.metrics.IncrementBackpressureRejections()
		fullness := int(float64(s.ingressQueue.Len()) / float64(s.ingressQueue.Cap()) * 100)
		return nil, status.Errorf(codes.Unavailable, "server overloaded: metrics ingress queue %d%% full", fullness)
	}

	// Map OTLP request to internal batches
	batches := s.mapper.MapMetricsData(request.ResourceMetrics)

	// Count received data points.
	totalPoints := 0
	for _, batch := range batches {
		totalPoints += batch.Size()
	}
	if totalPoints > 0 {
		s.metrics.IncrementMetricsReceived(totalPoints)
	}

	for _, batch := range batches {
		if err := s.ingressQueue.Send(ctx, batch); err != nil {
			return nil, status.Errorf(codes.Unavailable, "metrics ingress queue full: %v", err)
		}
	}

	return &metricsV1.ExportMetricsServiceResponse{}, nil
}

// RegisterMetricsServer registers the OTLP metrics service with a gRPC server.
func RegisterMetricsServer(grpcServer *grpc.Server, server *MetricsServer) {
	metricsV1.RegisterMetricsServiceServer(grpcServer, server)
}
