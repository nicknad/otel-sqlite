// Package main is the entry point for the OTLP collector.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"

	"codeberg.org/nicknad/otel-sqlite/internal/alerts"
	"codeberg.org/nicknad/otel-sqlite/internal/batcher"
	"codeberg.org/nicknad/otel-sqlite/internal/config"
	"codeberg.org/nicknad/otel-sqlite/internal/ingest"
	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/maintenance/tasks"
	"codeberg.org/nicknad/otel-sqlite/internal/metrics"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/notifications"
	"codeberg.org/nicknad/otel-sqlite/internal/otlp"
	"codeberg.org/nicknad/otel-sqlite/internal/rules"
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

	// Maintenance
	maintenanceWorker  *maintenance.Worker
	maintenanceMetrics *maintenance.Metrics

	// Notification
	notifyWorker *notifications.Worker
	notifStore   notifications.Store
	alertStore   alerts.AlertStore
	notifiers    []notifications.Notifier
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

	// Apply Go runtime memory limit if configured.
	// This tells the GC to be more aggressive before the process RSS exceeds
	// the limit, reducing OOM risk in containerized deployments.
	if cfg.GoMemoryLimitMB > 0 {
		limit := int64(cfg.GoMemoryLimitMB) * 1024 * 1024
		debug.SetMemoryLimit(limit)
		log.Printf("Go memory limit set to %d MB", cfg.GoMemoryLimitMB)
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

// loadConfig loads configuration with defaults overridden by environment variables
// and an optional YAML config file. The config file is read once; both the main
// config and the maintenance sub-config are extracted from the same parse.
func loadConfig() (*config.Config, error) {
	cfg := config.DefaultConfig()

	// Load optional YAML config file.
	configPath := os.Getenv("CONFIG_FILE")
	if configPath != "" {
		log.Printf("Loading config file from CONFIG_FILE")
		if err := cfg.LoadFile(configPath); err != nil {
			return nil, fmt.Errorf("failed to load config file: %w", err)
		}
	}

	// Environment variables override file values.
	if _, err := cfg.LoadFromEnv(); err != nil {
		return nil, fmt.Errorf("failed to load config from environment: %w", err)
	}

	// Load maintenance config. Use defaults first, then read from the same
	// config file (already read above) if available, then apply env overrides.
	maintCfg := maintenance.DefaultConfig()
	if configPath != "" {
		if err := maintCfg.LoadFile(configPath); err != nil {
			return nil, fmt.Errorf("failed to load maintenance config from file: %w", err)
		}
	}
	if _, err := maintCfg.LoadFromEnv(); err != nil {
		return nil, fmt.Errorf("failed to load maintenance config from environment: %w", err)
	}
	cfg.Maintenance = maintCfg

	// Legacy BATCH_SIZE / FLUSH_INTERVAL are applied inside cfg.LoadFromEnv
	// (config.ApplyLegacyEnv) with correct "specific env unset" precedence.

	return cfg, nil
}

// initialize initializes all application components.
func (a *Application) initialize() error {
	// Create metrics
	a.metrics = metrics.NewMetrics()

	// Create queues
	a.ingressQueue = ingest.NewIngressQueue(a.config.IngressQueueCapacity)
	a.cmdQueue = storage.NewCommandQueue(a.config.BatchQueueCapacity)

	// Create OTLP server with optional backpressure threshold
	a.otlpServer = otlp.NewServer(a.ingressQueue, a.metrics)
	if a.config.IngressQueueBackpressureThreshold > 0 {
		a.otlpServer.SetBackpressureThreshold(a.config.IngressQueueBackpressureThreshold)
		log.Printf("Backpressure threshold set to %.0f%% of ingress queue capacity",
			a.config.IngressQueueBackpressureThreshold*100)
	}

	// Create batcher (wraps batches as WriteBatchCommand, sends to cmd queue)
	a.batcher = batcher.NewBatcher(a.ingressQueue, a.cmdQueue, &batcher.BatcherConfig{
		BatchSize:              a.config.BatcherBatchSize,
		FlushInterval:          a.config.BatcherFlushInterval,
		Metrics:                a.metrics,
		ErrorSeverityThreshold: parseSeverity(a.config.BatcherErrorSeverityThreshold),
	})
	a.batcher.WithCommandFactory(func(batch *model.LogBatch) storage.Command {
		return sqlite.NewWriteBatchCommand(batch)
	})

	// Create SQLite writer (implements CommandExecutor)
	writerConfig := &sqlite.WriterConfig{
		Path:          a.config.SQLitePath,
		BatchSize:     a.config.WriterBatchSize,
		FlushInterval: a.config.WriterFlushInterval,
		WALMode:       true,
		Metrics:       a.metrics,
	}

	// Apply writer tuning configuration
	if a.config.WriterMaxTransactionRecords > 0 {
		sqlite.MaxTransactionRecords = a.config.WriterMaxTransactionRecords
	}

	var err error
	a.writer, err = sqlite.NewWriter(a.cmdQueue, writerConfig)
	if err != nil {
		return fmt.Errorf("failed to create SQLite writer: %w", err)
	}

	// Initialize notification pipeline if enabled.
	if a.config.Notification != nil && a.config.Notification.Enabled {
		if err := a.initializeNotifications(); err != nil {
			return fmt.Errorf("failed to initialize notifications: %w", err)
		}
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

	// Start maintenance worker
	a.startMaintenance()

	// Start notification worker
	if a.notifyWorker != nil {
		a.notifyWorker.Start(context.Background())
	}

	log.Println("OTLP collector started successfully")
	log.Printf("gRPC server listening on %s", a.config.ListenAddress)
	log.Printf("Metrics server listening on %s", a.config.MetricsAddress)
	log.Printf(
		"pipeline config: ingress_queue=%d batch_queue=%d "+
			"batcher_batch_size=%d batcher_flush=%s "+
			"writer_batch_size=%d writer_flush=%s max_tx_records=%d",
		a.config.IngressQueueCapacity,
		a.config.BatchQueueCapacity,
		a.config.BatcherBatchSize,
		a.config.BatcherFlushInterval,
		a.config.WriterBatchSize,
		a.config.WriterFlushInterval,
		a.config.WriterMaxTransactionRecords,
	)

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

// startGRPCServer starts the gRPC server with decompression and size limits.
func (a *Application) startGRPCServer() error {
	lis, err := net.Listen("tcp", a.config.ListenAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", a.config.ListenAddress, err)
	}

	// gRPC server options with decompression support, connection limits,
	// and keepalive enforcement.
	//
	// gRPC supports gzip decompression by default; we explicitly set
	// message size limits to accommodate compressed payloads, cap
	// concurrent streams to prevent resource exhaustion, and enforce
	// keepalive to clean up stale connections.
	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(a.config.GrpcMaxRecvMsgSize),
		grpc.MaxSendMsgSize(a.config.GrpcMaxSendMsgSize),
		grpc.MaxConcurrentStreams(uint32(a.config.GrpcMaxConcurrentStreams)), //nolint:gosec
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     5 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  2 * time.Minute,
			Timeout:               20 * time.Second,
		}),
	}

	a.grpcServer = grpc.NewServer(grpcOpts...)
	otlp.RegisterServer(a.grpcServer, a.otlpServer)

	go func() {
		log.Printf("Starting gRPC server on %s", a.config.ListenAddress)
		if err := a.grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	return nil
}

// startMaintenance initializes and starts the maintenance worker.
func (a *Application) startMaintenance() {
	maintCfg := a.config.Maintenance
	if maintCfg == nil {
		maintCfg = maintenance.DefaultConfig()
	}

	a.maintenanceMetrics = maintenance.NewMetrics()

	a.maintenanceWorker = maintenance.NewWorker(maintCfg, a.writer, a.maintenanceMetrics)

	// Register tasks. Disabled tasks are filtered by the worker at runtime.
	a.maintenanceWorker.Register(tasks.NewRetentionTask(
		maintCfg.RetentionEnabled,
		maintCfg.RetentionKeepLogs,
		maintCfg.RetentionCleanupInterval,
		maintCfg.RetentionDeleteBatchSize,
	))

	a.maintenanceWorker.Register(tasks.NewCheckpointTask(
		maintCfg.CheckpointEnabled,
		sqlite.CheckpointMode(maintCfg.CheckpointMode),
		maintCfg.CheckpointInterval,
	))

	a.maintenanceWorker.Register(tasks.NewVacuumTask(
		maintCfg.VacuumEnabled,
		maintCfg.VacuumInterval,
	))

	a.maintenanceWorker.Register(tasks.NewOptimizeTask(
		maintCfg.OptimizeEnabled,
		maintCfg.OptimizeInterval,
	))

	a.maintenanceWorker.Register(tasks.NewFtsRebuildTask(
		maintCfg.FTSEnabled,
		maintCfg.FTSRebuildInterval,
	))

	a.maintenanceWorker.Start(context.Background())
	log.Printf("maintenance worker: started with %d registered tasks", a.maintenanceWorker.TaskCount())
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
	ctx, cancel := context.WithTimeoutCause(context.Background(), a.config.ShutdownTimeout,
		errors.New("shutdown timeout"))
	defer cancel()

	var errs []error

	// Stop gRPC server
	if a.grpcServer != nil {
		a.grpcServer.GracefulStop()
	}

	// Stop HTTP server
	if a.httpServer != nil {
		if err := a.httpServer.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("http shutdown: %w", err))
		}
	}

	// Stop pipeline components
	a.batcher.Stop()

	// Stop maintenance worker
	if a.maintenanceWorker != nil {
		a.maintenanceWorker.Stop()
	}

	// Stop notification worker
	if a.notifyWorker != nil {
		a.notifyWorker.Stop()
	}
	for _, n := range a.notifiers {
		_ = n.Close()
	}
	if a.notifStore != nil {
		_ = a.notifStore.Close()
	}
	if a.alertStore != nil {
		_ = a.alertStore.Close()
	}

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

	if len(errs) > 0 {
		log.Printf("shutdown errors: %v", errors.Join(errs...))
	}
	log.Println("Shutdown complete")
}

// initializeNotifications sets up the notification and alerting pipeline.
func (a *Application) initializeNotifications() error {
	nc := a.config.Notification

	// Open the notification delivery state store (retry/DLQ).
	notifStore, err := notifications.NewBboltStore(nc.StorePath)
	if err != nil {
		return fmt.Errorf("open notification store: %w", err)
	}
	a.notifStore = notifStore

	// Open the alert state store.
	alertStore, err := alerts.NewBboltAlertStore(nc.AlertStorePath)
	if err != nil {
		return fmt.Errorf("open alert store: %w", err)
	}
	a.alertStore = alertStore

	// Build rules from config.
	ruleList := make([]rules.Rule, 0, len(nc.Rules))
	for i := range nc.Rules {
		rule, err := ruleConfigToRule(&nc.Rules[i])
		if err != nil {
			return fmt.Errorf("rule %q: %w", nc.Rules[i].Name, err)
		}
		ruleList = append(ruleList, *rule)
	}

	// Create rule engine.
	engine, err := rules.NewRuleEngine(ruleList)
	if err != nil {
		return fmt.Errorf("create rule engine: %w", err)
	}

	// Create notifiers from config.
	notifierMap := make(map[string]notifications.Notifier)
	for name, ncfg := range nc.Notifiers {
		var cfg any
		switch ncfg.Type {
		case "log":
			// no config needed
		case "http":
			url := ncfg.URL
			if url == "" {
				url = "http://localhost:8080/webhook"
			}
			cfg = &notifications.HTTPNotifierConfig{
				URL:        url,
				Timeout:    ncfg.Timeout,
				AuthHeader: ncfg.AuthHeader,
			}
		default:
			return fmt.Errorf("unknown notifier type %q for %q", ncfg.Type, name)
		}
		n, err := notifications.NewNotifier(ncfg.Type, cfg)
		if err != nil {
			return fmt.Errorf("create notifier %q: %w", name, err)
		}
		notifierMap[name] = n
		a.notifiers = append(a.notifiers, n)
	}

	// Create worker.
	a.notifyWorker = notifications.NewWorker(&notifications.WorkerConfig{
		AlertStore:              alertStore,
		NotifStore:              notifStore,
		Engine:                  engine,
		Notifiers:               notifierMap,
		EventQueueDepth:         nc.EventQueueDepth,
		RetryInterval:           nc.RetryInterval,
		GCInterval:              nc.GCInterval,
		AlertIdleTTL:            nc.AlertIdleTTL,
		ResolvedAlertRetention:  nc.ResolvedAlertRetention,
		DLQRetention:            nc.DLQRetention,
		BboltCompactionEnabled:  nc.BboltCompactionEnabled,
		BboltCompactionInterval: nc.BboltCompactionInterval,
		Metrics:                 a.metrics,
	})

	// Wire to batcher.
	a.batcher.WithErrorNotifier(a.notifyWorker)

	log.Printf("notification pipeline: initialized (%d rules, %d notifiers)", len(ruleList), len(notifierMap))
	return nil
}

// parseSeverity converts a severity string to a model.Severity.
// Shared between batcher config and rule config parsing.
func parseSeverity(s string) model.Severity {
	switch s {
	case "FATAL":
		return model.SeverityFatal
	case "ERROR", "":
		return model.SeverityError
	case "WARN":
		return model.SeverityWarn
	case "INFO":
		return model.SeverityInfo
	case "DEBUG":
		return model.SeverityDebug
	case "TRACE":
		return model.SeverityTrace
	default:
		return model.SeverityError
	}
}

// ruleConfigToRule converts a config.RuleConfig to a rules.Rule.
func ruleConfigToRule(rc *config.RuleConfig) (*rules.Rule, error) {
	rule := &rules.Rule{
		Name:             rc.Name,
		MatchSeverity:    parseSeverity(rc.MatchSeverity),
		ResourceFilter:   rc.ResourceFilter,
		BodyFilter:       rc.BodyFilter,
		AttributeFilters: rc.AttributeFilters,
		MaxRetries:       rc.MaxRetries,
		Destination:      rc.Destination,
		AlertThreshold:   rc.AlertThreshold,
	}

	// Parse durations.
	if rc.Cooldown != "" {
		d, err := time.ParseDuration(rc.Cooldown)
		if err != nil {
			return nil, fmt.Errorf("cooldown: %w", err)
		}
		rule.Cooldown = d
	}
	if rc.RetryBackoff != "" {
		d, err := time.ParseDuration(rc.RetryBackoff)
		if err != nil {
			return nil, fmt.Errorf("retry_backoff: %w", err)
		}
		rule.RetryBackoff = d
	}
	if rc.AlertWindow != "" {
		d, err := time.ParseDuration(rc.AlertWindow)
		if err != nil {
			return nil, fmt.Errorf("alert_window: %w", err)
		}
		rule.AlertWindow = d
	}
	if rc.AlertResolveWindow != "" {
		d, err := time.ParseDuration(rc.AlertResolveWindow)
		if err != nil {
			return nil, fmt.Errorf("alert_resolve_window: %w", err)
		}
		rule.AlertResolveWindow = d
	}

	return rule, nil
}
