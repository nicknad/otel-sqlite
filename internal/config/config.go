// Package config provides application configuration management.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
)

// Config holds the application configuration.
// All values can be set via environment variables.
type Config struct {
	// Server configuration
	ListenAddress string `mapstructure:"listen_address"`

	// SQLite configuration
	SQLitePath string `mapstructure:"sqlite_path"`

	// Ingestion pipeline configuration
	IngressQueueCapacity int `mapstructure:"ingress_queue_capacity"`
	BatchQueueCapacity   int `mapstructure:"batch_queue_capacity"`

	// Batcher configuration
	BatcherBatchSize     int           `mapstructure:"batcher_batch_size"`
	BatcherFlushInterval time.Duration `mapstructure:"batcher_flush_interval"`

	// Writer configuration
	WriterBatchSize     int           `mapstructure:"writer_batch_size"`
	WriterFlushInterval time.Duration `mapstructure:"writer_flush_interval"`

	// Writer tuning
	WriterMaxTransactionRecords int `mapstructure:"writer_max_transaction_records"`

	// Observability
	MetricsAddress string `mapstructure:"metrics_address"`

	// gRPC configuration
	GrpcMaxRecvMsgSize       int `mapstructure:"grpc_max_recv_msg_size"`
	GrpcMaxSendMsgSize       int `mapstructure:"grpc_max_send_msg_size"`
	GrpcMaxConcurrentStreams int `mapstructure:"grpc_max_concurrent_streams"`

	// Backpressure: when ingress queue depth exceeds this fraction (0.0–1.0)
	// of capacity, the gRPC server rejects new requests with Unavailable.
	// 0 disables early rejection (server blocks until queue accepts).
	IngressQueueBackpressureThreshold float64 `mapstructure:"ingress_queue_backpressure_threshold"`

	// Go runtime memory limit in MB. 0 disables (uses Go default).
	// When set, calls debug.SetMemoryLimit(bytes) at startup.
	GoMemoryLimitMB int `mapstructure:"go_memory_limit_mb"`

	// Timeouts
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`

	// Maintenance configuration
	Maintenance *maintenance.Config `mapstructure:"maintenance"`
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		ListenAddress:                     ":4317",
		SQLitePath:                        "otel-logs.db",
		IngressQueueCapacity:              10000,
		BatchQueueCapacity:                1000,
		BatcherBatchSize:                  250,
		BatcherFlushInterval:              5 * time.Second,
		WriterBatchSize:                   100,
		WriterFlushInterval:               5 * time.Second,
		WriterMaxTransactionRecords:       5000,
		MetricsAddress:                    ":9090",
		GrpcMaxRecvMsgSize:                16 * 1024 * 1024, // 16 MB
		GrpcMaxSendMsgSize:                16 * 1024 * 1024, // 16 MB
		GrpcMaxConcurrentStreams:          100,
		IngressQueueBackpressureThreshold: 0, // disabled by default; recommends 0.8
		GoMemoryLimitMB:                   0, // disabled by default; recommends 2048
		ShutdownTimeout:                   30 * time.Second,
	}
}

// envVar holds the mapping from a config field to its environment variable name.
type envVar struct {
	Key         string
	Env         string
	Description string
}

// envVars lists all configurable fields and their environment variable names.
var envVars = []envVar{
	{Key: "ListenAddress", Env: "LISTEN_ADDRESS", Description: "gRPC server listen address"},
	{Key: "SQLitePath", Env: "SQLITE_PATH", Description: "Path to SQLite database file"},
	{Key: "IngressQueueCapacity", Env: "INGRESS_QUEUE_CAPACITY", Description: "Maximum ingress queue size"},
	{Key: "BatchQueueCapacity", Env: "BATCH_QUEUE_CAPACITY", Description: "Maximum batch queue size"},
	{Key: "BatcherBatchSize", Env: "BATCHER_BATCH_SIZE", Description: "Number of log records per batch (batcher)"},
	{Key: "BatcherFlushInterval", Env: "BATCHER_FLUSH_INTERVAL", Description: "Maximum time between batch flushes"},
	{Key: "WriterBatchSize", Env: "WRITER_BATCH_SIZE", Description: "Number of commands per transaction (writer)"},
	{Key: "WriterFlushInterval", Env: "WRITER_FLUSH_INTERVAL", Description: "Maximum time between transaction flushes"},
	{Key: "WriterMaxTransactionRecords", Env: "WRITER_MAX_TRANSACTION_RECORDS",
		Description: "Maximum records per SQLite transaction"},
	{Key: "GrpcMaxRecvMsgSize", Env: "GRPC_MAX_RECV_MSG_SIZE", Description: "Max gRPC receive message size in bytes"},
	{Key: "GrpcMaxSendMsgSize", Env: "GRPC_MAX_SEND_MSG_SIZE", Description: "Max gRPC send message size in bytes"},
	{Key: "GrpcMaxConcurrentStreams", Env: "GRPC_MAX_CONCURRENT_STREAMS", Description: "Max concurrent gRPC streams"},
	{Key: "IngressQueueBackpressureThreshold", Env: "INGRESS_QUEUE_BACKPRESSURE_THRESHOLD",
		Description: "Ingress queue fullness fraction (0.0–1.0) to trigger early rejection"},
	{Key: "GoMemoryLimitMB", Env: "GO_MEMORY_LIMIT_MB", Description: "Go runtime memory limit in MB (0 = disabled)"},
	{Key: "MetricsAddress", Env: "METRICS_ADDRESS", Description: "Prometheus metrics server address"},
	{Key: "ShutdownTimeout", Env: "SHUTDOWN_TIMEOUT", Description: "Graceful shutdown timeout"},
}

// LoadFromEnv overrides config fields with values from environment variables.
// Returns the number of overrides applied and any parse errors encountered.
func (c *Config) LoadFromEnv() (int, error) {
	var count int
	for _, ev := range envVars {
		val, ok := os.LookupEnv(ev.Env)
		if !ok {
			continue
		}
		if err := c.setField(ev.Key, val); err != nil {
			return count, fmt.Errorf("env %s: %w", ev.Env, err)
		}
		count++
	}
	return count, nil
}

// setField sets a config field by name from a string value.
func (c *Config) setField(key, value string) error {
	switch key {
	case "ListenAddress":
		c.ListenAddress = value
	case "SQLitePath":
		c.SQLitePath = value
	case "IngressQueueCapacity":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int %q: %w", value, err)
		}
		c.IngressQueueCapacity = n
	case "BatchQueueCapacity":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int %q: %w", value, err)
		}
		c.BatchQueueCapacity = n
	case "BatcherBatchSize":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int %q: %w", value, err)
		}
		c.BatcherBatchSize = n
	case "BatcherFlushInterval":
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", value, err)
		}
		c.BatcherFlushInterval = d
	case "WriterBatchSize":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int %q: %w", value, err)
		}
		c.WriterBatchSize = n
	case "WriterFlushInterval":
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", value, err)
		}
		c.WriterFlushInterval = d
	case "WriterMaxTransactionRecords":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int %q: %w", value, err)
		}
		c.WriterMaxTransactionRecords = n
	case "GrpcMaxRecvMsgSize":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int %q: %w", value, err)
		}
		c.GrpcMaxRecvMsgSize = n
	case "GrpcMaxSendMsgSize":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int %q: %w", value, err)
		}
		c.GrpcMaxSendMsgSize = n
	case "GrpcMaxConcurrentStreams":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int %q: %w", value, err)
		}
		c.GrpcMaxConcurrentStreams = n
	case "IngressQueueBackpressureThreshold":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("invalid float %q: %w", value, err)
		}
		c.IngressQueueBackpressureThreshold = f
	case "GoMemoryLimitMB":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int %q: %w", value, err)
		}
		c.GoMemoryLimitMB = n
	case "MetricsAddress":
		c.MetricsAddress = value
	case "ShutdownTimeout":
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", value, err)
		}
		c.ShutdownTimeout = d
	}
	return nil
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	if c.ListenAddress == "" {
		return fmt.Errorf("listen_address must not be empty")
	}
	if c.SQLitePath == "" {
		return fmt.Errorf("sqlite_path must not be empty")
	}
	if c.IngressQueueCapacity <= 0 {
		return fmt.Errorf("ingress_queue_capacity must be positive, got %d", c.IngressQueueCapacity)
	}
	if c.BatchQueueCapacity <= 0 {
		return fmt.Errorf("batch_queue_capacity must be positive, got %d", c.BatchQueueCapacity)
	}
	if c.BatcherBatchSize <= 0 {
		return fmt.Errorf("batcher_batch_size must be positive, got %d", c.BatcherBatchSize)
	}
	if c.BatcherFlushInterval <= 0 {
		return fmt.Errorf("batcher_flush_interval must be positive, got %s", c.BatcherFlushInterval)
	}
	if c.WriterBatchSize <= 0 {
		return fmt.Errorf("writer_batch_size must be positive, got %d", c.WriterBatchSize)
	}
	if c.WriterFlushInterval <= 0 {
		return fmt.Errorf("writer_flush_interval must be positive, got %s", c.WriterFlushInterval)
	}
	if c.MetricsAddress == "" {
		return fmt.Errorf("metrics_address must not be empty")
	}
	if c.GrpcMaxRecvMsgSize <= 0 {
		return fmt.Errorf("grpc_max_recv_msg_size must be positive, got %d", c.GrpcMaxRecvMsgSize)
	}
	if c.GrpcMaxSendMsgSize <= 0 {
		return fmt.Errorf("grpc_max_send_msg_size must be positive, got %d", c.GrpcMaxSendMsgSize)
	}
	if c.GrpcMaxConcurrentStreams <= 0 {
		return fmt.Errorf("grpc_max_concurrent_streams must be positive, got %d", c.GrpcMaxConcurrentStreams)
	}
	if c.IngressQueueBackpressureThreshold < 0 || c.IngressQueueBackpressureThreshold > 1 {
		return fmt.Errorf("ingress_queue_backpressure_threshold must be between 0.0 and 1.0, got %f",
			c.IngressQueueBackpressureThreshold)
	}
	if c.GoMemoryLimitMB < 0 {
		return fmt.Errorf("go_memory_limit_mb must be non-negative, got %d", c.GoMemoryLimitMB)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown_timeout must be positive, got %s", c.ShutdownTimeout)
	}
	if c.Maintenance != nil {
		if err := c.Maintenance.Validate(); err != nil {
			return fmt.Errorf("maintenance: %w", err)
		}
	}
	return nil
}
