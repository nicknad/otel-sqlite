// Package main is the entry point for the OTLP collector.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"codeberg.org/nicknad/otel-sqlite/internal/batcher"
	"codeberg.org/nicknad/otel-sqlite/internal/config"
	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/metrics"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/otlp"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
)

// Application holds the main application components.
type Application struct {
	config     *config.Config
	metrics    *metrics.Metrics
	grpcServer *grpc.Server
	httpServer *http.Server

	// Pipeline components
	ingressQueue ingest.IngressQueue
	cmdQueue     storage.CommandQueue
	batcher      *batcher.Batcher
	writer       *sqlite.Writer

	// OTLP server
	otlpServer *otlp.Server
}

func main() {
	// Load configuration
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// Create application
	app := &Application{
		config: cfg,
	}

	// Initialize components
	if err := app.initialize(); err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}

	// Start components
	if err := app.start(); err != nil {
		log.Fatalf("Failed to start application: %v", err)
	}

	// Wait for shutdown
	app.waitForShutdown()

	// Cleanup
	app.cleanup()
}

// loadConfig loads configuration with defaults overridden by environment variables.
func loadConfig() (*config.Config, error) {
	cfg := config.DefaultConfig()
	if _, err := cfg.LoadFromEnv(); err != nil {
		return nil, fmt.Errorf("failed to load config from environment: %w", err)
	}
	return cfg, nil
}

// initialize initializes all application components.
func (a *Application) initialize() error {
	// Create metrics
	a.metrics = metrics.NewMetrics()

	// Create queues
	a.ingressQueue = ingest.NewIngressQueue(a.config.IngressQueueCapacity)
	a.cmdQueue = storage.NewCommandQueue(a.config.BatchQueueCapacity)

	// Create OTLP server
	a.otlpServer = otlp.NewServer(a.ingressQueue, a.metrics)

	// Create batcher (wraps batches as WriteBatchCommand, sends to cmd queue)
	a.batcher = batcher.NewBatcher(a.ingressQueue, a.cmdQueue, &batcher.BatcherConfig{
		BatchSize:     a.config.BatchSize,
		FlushInterval: a.config.FlushInterval,
		Metrics:       a.metrics,
	})
	a.batcher.WithCommandFactory(func(batch *model.LogBatch) storage.Command {
		return sqlite.NewWriteBatchCommand(batch)
	})

	// Create SQLite writer (implements CommandExecutor)
	writerConfig := &sqlite.WriterConfig{
		Path:          a.config.SQLitePath,
		BatchSize:     a.config.BatchSize,
		FlushInterval: a.config.FlushInterval,
		WALMode:       true,
		Metrics:       a.metrics,
	}

	var err error
	a.writer, err = sqlite.NewWriter(a.cmdQueue, writerConfig)
	if err != nil {
		return fmt.Errorf("failed to create SQLite writer: %w", err)
	}

	return nil
}

// start starts all application components.
func (a *Application) start() error {
	// Start metrics server
	if err := a.startMetricsServer(); err != nil {
		return fmt.Errorf("failed to start metrics server: %w", err)
	}

	// Start gRPC server
	if err := a.startGRPCServer(); err != nil {
		return fmt.Errorf("failed to start gRPC server: %w", err)
	}

	// Start pipeline components
	a.batcher.Start(context.Background())

	// Start SQLite writer
	a.writer.Start(context.Background())

	log.Println("OTLP collector started successfully")
	log.Printf("gRPC server listening on %s", a.config.ListenAddress)
	log.Printf("Metrics server listening on %s", a.config.MetricsAddress)

	return nil
}

// startMetricsServer starts the Prometheus metrics HTTP server.
func (a *Application) startMetricsServer() error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	a.httpServer = &http.Server{
		Addr:              a.config.MetricsAddress,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Starting metrics server on %s", a.config.MetricsAddress)
		if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	return nil
}

// startGRPCServer starts the gRPC server.
func (a *Application) startGRPCServer() error {
	lis, err := net.Listen("tcp", a.config.ListenAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", a.config.ListenAddress, err)
	}

	a.grpcServer = grpc.NewServer()
	otlp.RegisterServer(a.grpcServer, a.otlpServer)

	go func() {
		log.Printf("Starting gRPC server on %s", a.config.ListenAddress)
		if err := a.grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	return nil
}

// waitForShutdown waits for a shutdown signal.
func (a *Application) waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Shutdown signal received")
}

// cleanup performs cleanup on shutdown.
func (a *Application) cleanup() {
	log.Println("Shutting down...")

	// Create shutdown context
	ctx, cancel := context.WithTimeout(context.Background(), a.config.ShutdownTimeout)
	defer cancel()

	// Stop gRPC server
	if a.grpcServer != nil {
		a.grpcServer.GracefulStop()
	}

	// Stop HTTP server
	if a.httpServer != nil {
		_ = a.httpServer.Shutdown(ctx)
	}

	// Stop pipeline components
	a.batcher.Stop()

	// Stop SQLite writer
	if a.writer != nil {
		a.writer.Stop()
		a.writer.Wait()
	}

	// Close queues
	if a.ingressQueue != nil {
		a.ingressQueue.Close()
	}
	if a.cmdQueue != nil {
		a.cmdQueue.Close()
	}

	log.Println("Shutdown complete")
}
