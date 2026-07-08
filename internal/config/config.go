// Package config provides application configuration management.
package config

import (
	"errors"
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
	BatcherBatchSize              int           `mapstructure:"batcher_batch_size"`
	BatcherFlushInterval          time.Duration `mapstructure:"batcher_flush_interval"`
	BatcherErrorSeverityThreshold string        `mapstructure:"batcher_error_severity_threshold"`

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

	// Notification configuration
	Notification *NotificationConfig `mapstructure:"notification"`

	// Maintenance configuration
	Maintenance *maintenance.Config `mapstructure:"maintenance"`
}

// NotificationConfig holds configuration for the notification pipeline.
type NotificationConfig struct {
	// Enabled is the master switch.
	Enabled bool `mapstructure:"enabled"`

	// EventQueueDepth is the buffered channel capacity between batcher and worker.
	EventQueueDepth int `mapstructure:"event_queue_depth"`

	// StorePath is the file path for the embedded KV store (bbolt) used for
	// notification delivery state and DLQ.
	StorePath string `mapstructure:"store_path"`

	// AlertStorePath is the file path for the alert state store (bbolt).
	// Defaults to "alert-state.db" if empty.
	AlertStorePath string `mapstructure:"alert_store_path"`

	// RetryInterval is how often the retry goroutine scans for retryable events.
	RetryInterval time.Duration `mapstructure:"retry_interval"`

	// GCInterval is how often the worker garbage-collects resolved alerts.
	GCInterval time.Duration `mapstructure:"gc_interval"`

	// Notifiers maps notifier name → config. Built-in names: "log", "http".
	Notifiers map[string]NotifierConfig `mapstructure:"notifiers"`

	// Rules defines the notification rules.
	Rules []RuleConfig `mapstructure:"rules"`
}

// NotifierConfig holds configuration for a single notifier instance.
type NotifierConfig struct {
	// Type is the notifier implementation: "log" or "http".
	Type string `mapstructure:"type"`

	// URL is the webhook URL (for "http" type).
	URL string `mapstructure:"url"`

	// Timeout is the HTTP client timeout (for "http" type).
	Timeout time.Duration `mapstructure:"timeout"`

	// AuthHeader is an optional Authorization header value.
	AuthHeader string `mapstructure:"auth_header"`
}

// RuleConfig is the YAML-parseable form of a notification rule.
type RuleConfig struct {
	Name             string            `mapstructure:"name"`
	MatchSeverity    string            `mapstructure:"match_severity"`
	ResourceFilter   string            `mapstructure:"resource_filter"`
	BodyFilter       string            `mapstructure:"body_filter"`
	AttributeFilters map[string]string `mapstructure:"attribute_filters"`
	Cooldown         string            `mapstructure:"cooldown"`
	RateLimit        int               `mapstructure:"rate_limit"`
	RateWindow       string            `mapstructure:"rate_window"`
	DedupWindow      string            `mapstructure:"dedup_window"`
	MaxRetries       int               `mapstructure:"max_retries"`
	RetryBackoff     string            `mapstructure:"retry_backoff"`
	Destination      string            `mapstructure:"destination"`

	// Alert-specific fields.
	AlertWindow        string `mapstructure:"alert_window"`
	AlertThreshold     int    `mapstructure:"alert_threshold"`
	AlertResolveWindow string `mapstructure:"alert_resolve_window"`
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		ListenAddress:                     ":4317",
		SQLitePath:                        "otel-logs.db",
		IngressQueueCapacity:              10000,
		BatchQueueCapacity:                5000,
		BatcherBatchSize:                  250,
		BatcherFlushInterval:              1 * time.Second,
		BatcherErrorSeverityThreshold:     "ERROR",
		WriterBatchSize:                   100,
		WriterFlushInterval:               1 * time.Second,
		WriterMaxTransactionRecords:       5000,
		MetricsAddress:                    ":9090",
		GrpcMaxRecvMsgSize:                16 * 1024 * 1024, // 16 MB
		GrpcMaxSendMsgSize:                16 * 1024 * 1024, // 16 MB
		GrpcMaxConcurrentStreams:          100,
		IngressQueueBackpressureThreshold: 0, // disabled by default; recommends 0.8
		GoMemoryLimitMB:                   0, // disabled by default; recommends 2048
		ShutdownTimeout:                   30 * time.Second,
		Notification: &NotificationConfig{
			Enabled:         false,
			EventQueueDepth: 1000,
			StorePath:       "notify-state.db",
			AlertStorePath:  "alert-state.db",
			RetryInterval:   30 * time.Second,
			GCInterval:      5 * time.Minute,
		},
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
	{
		Key: "BatcherErrorSeverityThreshold", Env: "BATCHER_ERROR_SEVERITY_THRESHOLD",
		Description: "Minimum severity to notify (ERROR, WARN, INFO, etc.)",
	},
	{Key: "WriterBatchSize", Env: "WRITER_BATCH_SIZE", Description: "Number of commands per transaction (writer)"},
	{Key: "WriterFlushInterval", Env: "WRITER_FLUSH_INTERVAL", Description: "Maximum time between transaction flushes"},
	{
		Key: "WriterMaxTransactionRecords", Env: "WRITER_MAX_TRANSACTION_RECORDS",
		Description: "Maximum records per SQLite transaction",
	},
	{Key: "GrpcMaxRecvMsgSize", Env: "GRPC_MAX_RECV_MSG_SIZE", Description: "Max gRPC receive message size in bytes"},
	{Key: "GrpcMaxSendMsgSize", Env: "GRPC_MAX_SEND_MSG_SIZE", Description: "Max gRPC send message size in bytes"},
	{Key: "GrpcMaxConcurrentStreams", Env: "GRPC_MAX_CONCURRENT_STREAMS", Description: "Max concurrent gRPC streams"},
	{
		Key: "IngressQueueBackpressureThreshold", Env: "INGRESS_QUEUE_BACKPRESSURE_THRESHOLD",
		Description: "Ingress queue fullness fraction (0.0–1.0) to trigger early rejection",
	},
	{Key: "GoMemoryLimitMB", Env: "GO_MEMORY_LIMIT_MB", Description: "Go runtime memory limit in MB (0 = disabled)"},
	{Key: "MetricsAddress", Env: "METRICS_ADDRESS", Description: "Prometheus metrics server address"},
	{Key: "ShutdownTimeout", Env: "SHUTDOWN_TIMEOUT", Description: "Graceful shutdown timeout"},
	{Key: "NotificationEnabled", Env: "NOTIFICATION_ENABLED", Description: "Enable notification pipeline (true/false)"},
	{
		Key: "NotificationEventQueueDepth", Env: "NOTIFICATION_EVENT_QUEUE_DEPTH",
		Description: "Notification event queue capacity",
	},
	{
		Key: "NotificationStorePath", Env: "NOTIFICATION_STORE_PATH",
		Description: "Path to notification state store (bbolt)",
	},
	{
		Key: "NotificationRetryInterval", Env: "NOTIFICATION_RETRY_INTERVAL",
		Description: "Notification retry scan interval",
	},
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
	case "BatcherErrorSeverityThreshold":
		c.BatcherErrorSeverityThreshold = value
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
	case "NotificationEnabled":
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid bool %q: %w", value, err)
		}
		c.ensureNotification().Enabled = b
	case "NotificationEventQueueDepth":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int %q: %w", value, err)
		}
		c.ensureNotification().EventQueueDepth = n
	case "NotificationStorePath":
		c.ensureNotification().StorePath = value
	case "NotificationRetryInterval":
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", value, err)
		}
		c.ensureNotification().RetryInterval = d
	}
	return nil
}

// ensureNotification returns the Notification config, creating it if nil.
func (c *Config) ensureNotification() *NotificationConfig {
	if c.Notification == nil {
		c.Notification = &NotificationConfig{}
	}
	return c.Notification
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	if c.ListenAddress == "" {
		return errors.New("listen_address must not be empty")
	}
	if c.SQLitePath == "" {
		return errors.New("sqlite_path must not be empty")
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
	if c.BatcherErrorSeverityThreshold != "" {
		switch c.BatcherErrorSeverityThreshold {
		case "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "FATAL", "UNSPECIFIED":
		default:
			return fmt.Errorf(
				"batcher_error_severity_threshold must be ERROR,WARN,INFO,DEBUG,TRACE,FATAL,UNSPECIFIED, got %q",
				c.BatcherErrorSeverityThreshold)
		}
	}
	if c.WriterBatchSize <= 0 {
		return fmt.Errorf("writer_batch_size must be positive, got %d", c.WriterBatchSize)
	}
	if c.WriterFlushInterval <= 0 {
		return fmt.Errorf("writer_flush_interval must be positive, got %s", c.WriterFlushInterval)
	}
	if c.MetricsAddress == "" {
		return errors.New("metrics_address must not be empty")
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
	if c.Notification != nil && c.Notification.Enabled {
		if c.Notification.StorePath == "" {
			return errors.New("notification.store_path must not be empty when notifications are enabled")
		}
		if c.Notification.AlertStorePath == "" {
			return errors.New("notification.alert_store_path must not be empty when notifications are enabled")
		}
		if c.Notification.EventQueueDepth <= 0 {
			return fmt.Errorf("notification.event_queue_depth must be positive, got %d", c.Notification.EventQueueDepth)
		}
		if c.Notification.RetryInterval <= 0 {
			return fmt.Errorf("notification.retry_interval must be positive, got %s", c.Notification.RetryInterval)
		}
		if c.Notification.GCInterval <= 0 {
			return fmt.Errorf("notification.gc_interval must be positive, got %s", c.Notification.GCInterval)
		}
		for i := range c.Notification.Rules {
			rule := &c.Notification.Rules[i]
			if rule.Name == "" {
				return fmt.Errorf("notification.rules[%d].name must not be empty", i)
			}
			if rule.Destination == "" {
				return fmt.Errorf("notification.rules[%d].destination must not be empty", i)
			}
		}
	}
	if c.Maintenance != nil {
		if err := c.Maintenance.Validate(); err != nil {
			return fmt.Errorf("maintenance: %w", err)
		}
	}
	return nil
}
