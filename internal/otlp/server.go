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
}

// NewServer creates a new OTLP gRPC server.
func NewServer(ingressQueue ingest.IngressQueue, m *metrics.Metrics) *Server {
	return &Server{
		mapper:       NewMapper(),
		ingressQueue: ingressQueue,
		metrics:      m,
	}
}

// Export implements the Export method of the LogsServiceServer interface.
func (s *Server) Export(ctx context.Context, request *logsV1.ExportLogsServiceRequest) (*logsV1.ExportLogsServiceResponse, error) { //nolint:lll
	if request == nil || request.ResourceLogs == nil {
		return &logsV1.ExportLogsServiceResponse{}, nil
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
			return nil, status.Errorf(codes.ResourceExhausted, "ingress queue full: %v", err)
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
