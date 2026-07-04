// Package otlp provides the gRPC server for receiving OTLP logs.
// This package handles the transport layer and delegates to the mapper.
package otlp

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/metrics"

	// OTLP protobuf imports
	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1"
)

// Server implements the OTLP gRPC service for logs.
type Server struct {
	logsV1.UnimplementedLogsServiceServer

	mapper       *Mapper
	ingressQueue ingest.IngressQueue
	metrics      *metrics.Metrics

	// backpressureThreshold is the fraction of ingress queue capacity at which
	// the server rejects new requests (0 = disabled).
	backpressureThreshold float64
}

// NewServer creates a new OTLP gRPC server.
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

	// Map OTLP request to internal batches
	batches := s.mapper.MapLogsData(request.ResourceLogs)

	// Optional early backpressure: reject before mapping if the ingress queue
	// is already too full, protecting memory even under extreme client concurrency.
	if s.backpressureThreshold > 0 {
		queueCap := s.ingressQueue.Cap()
		if queueCap > 0 {
			fullness := float64(s.ingressQueue.Len()) / float64(queueCap)
			if fullness >= s.backpressureThreshold {
				s.metrics.IncrementBackpressureRejections()
				return nil, status.Errorf(codes.Unavailable,
					"server overloaded: ingress queue %d%% full", int(fullness*100))
			}
		}
	}

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

// RegisterServer registers the OTLP server with a gRPC server.
func RegisterServer(grpcServer *grpc.Server, server *Server) {
	logsV1.RegisterLogsServiceServer(grpcServer, server)
}

// StartGRPCServer starts a gRPC server with the OTLP service.
// Optional gRPC server options (e.g. grpc.MaxRecvMsgSize) can be passed.
func StartGRPCServer(address string, server *Server, opts ...grpc.ServerOption) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	grpcServer := grpc.NewServer(opts...)
	RegisterServer(grpcServer, server)

	go func() {
		log.Printf("Starting gRPC server on %s", address)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}()

	return grpcServer, nil
}
