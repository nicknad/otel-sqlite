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

	// AlertIdleTTL is how long Pending/Firing alerts can remain idle (no
	// matching events) before being garbage-collected. Prevents unbounded
	// growth from ephemeral resources. Defaults to 24h.
	AlertIdleTTL time.Duration `mapstructure:"alert_idle_ttl"`

	// ResolvedAlertRetention is how long Resolved alerts are retained before
	// being garbage-collected. Defaults to 24h.
	ResolvedAlertRetention time.Duration `mapstructure:"resolved_alert_retention"`

	// DLQRetention is how long dead-letter queue entries are retained before
	// being purged. Defaults to 30 days.
	DLQRetention time.Duration `mapstructure:"dlq_retention"`

	// BboltCompactionEnabled enables periodic compaction of the bbolt stores.
	BboltCompactionEnabled bool `mapstructure:"bbolt_compaction_enabled"`

	// BboltCompactionInterval is how often to compact the bbolt stores.
	// Defaults to 24h if enabled.
	BboltCompactionInterval time.Duration `mapstructure:"bbolt_compaction_interval"`

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
		BatcherBatchSize:                  500,
		BatcherFlushInterval:              1 * time.Second,
		BatcherErrorSeverityThreshold:     "ERROR",
		WriterBatchSize:                   50, // commands per tx collection, not records
		WriterFlushInterval:               1 * time.Second,
		WriterMaxTransactionRecords:       10000,
		MetricsAddress:                    ":9090",
		GrpcMaxRecvMsgSize:                16 * 1024 * 1024, // 16 MB
		GrpcMaxSendMsgSize:                16 * 1024 * 1024, // 16 MB
		GrpcMaxConcurrentStreams:          100,
		IngressQueueBackpressureThreshold: 0, // disabled by default; recommends 0.8
		GoMemoryLimitMB:                   0, // disabled by default; recommends 2048
		ShutdownTimeout:                   30 * time.Second,
		Notification: &NotificationConfig{
			Enabled:                 false,
			EventQueueDepth:         1000,
			StorePath:               "notify-state.db",
			AlertStorePath:          "alert-state.db",
			RetryInterval:           30 * time.Second,
			GCInterval:              5 * time.Minute,
			AlertIdleTTL:            24 * time.Hour,
			ResolvedAlertRetention:  24 * time.Hour,
			DLQRetention:            30 * 24 * time.Hour,
			BboltCompactionEnabled:  false,
			BboltCompactionInterval: 24 * time.Hour,
		},
	}
}

// envBinding describes one environment-variable → config-field mapping.
type envBinding struct {
	env   string
	parse func(string) (any, error)
	apply func(*Config, any)
}

var envBindings = []envBinding{
	// Strings
	{env: "LISTEN_ADDRESS", parse: parseString, apply: func(c *Config, v any) { c.ListenAddress = v.(string) }},
	{env: "SQLITE_PATH", parse: parseString, apply: func(c *Config, v any) { c.SQLitePath = v.(string) }},
	{env: "METRICS_ADDRESS", parse: parseString, apply: func(c *Config, v any) { c.MetricsAddress = v.(string) }},
	{
		env: "BATCHER_ERROR_SEVERITY_THRESHOLD", parse: parseString,
		apply: func(c *Config, v any) { c.BatcherErrorSeverityThreshold = v.(string) },
	},

	// Ints
	{
		env: "INGRESS_QUEUE_CAPACITY", parse: parseInt,
		apply: func(c *Config, v any) { c.IngressQueueCapacity = v.(int) },
	},
	{
		env: "BATCH_QUEUE_CAPACITY", parse: parseInt,
		apply: func(c *Config, v any) { c.BatchQueueCapacity = v.(int) },
	},
	{
		env: "BATCHER_BATCH_SIZE", parse: parseInt,
		apply: func(c *Config, v any) { c.BatcherBatchSize = v.(int) },
	},
	{
		env: "WRITER_BATCH_SIZE", parse: parseInt,
		apply: func(c *Config, v any) { c.WriterBatchSize = v.(int) },
	},
	{
		env: "WRITER_MAX_TRANSACTION_RECORDS", parse: parseInt,
		apply: func(c *Config, v any) { c.WriterMaxTransactionRecords = v.(int) },
	},
	{
		env: "GRPC_MAX_RECV_MSG_SIZE", parse: parseInt,
		apply: func(c *Config, v any) { c.GrpcMaxRecvMsgSize = v.(int) },
	},
	{
		env: "GRPC_MAX_SEND_MSG_SIZE", parse: parseInt,
		apply: func(c *Config, v any) { c.GrpcMaxSendMsgSize = v.(int) },
	},
	{
		env: "GRPC_MAX_CONCURRENT_STREAMS", parse: parseInt,
		apply: func(c *Config, v any) { c.GrpcMaxConcurrentStreams = v.(int) },
	},
	{
		env: "GO_MEMORY_LIMIT_MB", parse: parseInt,
		apply: func(c *Config, v any) { c.GoMemoryLimitMB = v.(int) },
	},

	// Durations
	{
		env: "BATCHER_FLUSH_INTERVAL", parse: parseDuration,
		apply: func(c *Config, v any) { c.BatcherFlushInterval = v.(time.Duration) },
	},
	{
		env: "WRITER_FLUSH_INTERVAL", parse: parseDuration,
		apply: func(c *Config, v any) { c.WriterFlushInterval = v.(time.Duration) },
	},
	{
		env: "SHUTDOWN_TIMEOUT", parse: parseDuration,
		apply: func(c *Config, v any) { c.ShutdownTimeout = v.(time.Duration) },
	},

	// Float
	{
		env: "INGRESS_QUEUE_BACKPRESSURE_THRESHOLD", parse: parseFloat,
		apply: func(c *Config, v any) { c.IngressQueueBackpressureThreshold = v.(float64) },
	},

	// Notification
	{
		env: "NOTIFICATION_ENABLED", parse: parseBool,
		apply: func(c *Config, v any) { c.ensureNotification().Enabled = v.(bool) },
	},
	{
		env: "NOTIFICATION_EVENT_QUEUE_DEPTH", parse: parseInt,
		apply: func(c *Config, v any) { c.ensureNotification().EventQueueDepth = v.(int) },
	},
	{
		env: "NOTIFICATION_STORE_PATH", parse: parseString,
		apply: func(c *Config, v any) { c.ensureNotification().StorePath = v.(string) },
	},
	{
		env: "NOTIFICATION_RETRY_INTERVAL", parse: parseDuration,
		apply: func(c *Config, v any) { c.ensureNotification().RetryInterval = v.(time.Duration) },
	},
	{
		env: "NOTIFICATION_ALERT_STORE_PATH", parse: parseString,
		apply: func(c *Config, v any) { c.ensureNotification().AlertStorePath = v.(string) },
	},
	{
		env: "NOTIFICATION_GC_INTERVAL", parse: parseDuration,
		apply: func(c *Config, v any) { c.ensureNotification().GCInterval = v.(time.Duration) },
	},
	{
		env: "NOTIFICATION_ALERT_IDLE_TTL", parse: parseDuration,
		apply: func(c *Config, v any) { c.ensureNotification().AlertIdleTTL = v.(time.Duration) },
	},
	{
		env: "NOTIFICATION_RESOLVED_ALERT_RETENTION", parse: parseDuration,
		apply: func(c *Config, v any) { c.ensureNotification().ResolvedAlertRetention = v.(time.Duration) },
	},
	{
		env: "NOTIFICATION_DLQ_RETENTION", parse: parseDuration,
		apply: func(c *Config, v any) { c.ensureNotification().DLQRetention = v.(time.Duration) },
	},
	{
		env: "NOTIFICATION_BBOLT_COMPACTION_ENABLED", parse: parseBool,
		apply: func(c *Config, v any) { c.ensureNotification().BboltCompactionEnabled = v.(bool) },
	},
	{
		env: "NOTIFICATION_BBOLT_COMPACTION_INTERVAL", parse: parseDuration,
		apply: func(c *Config, v any) { c.ensureNotification().BboltCompactionInterval = v.(time.Duration) },
	},
}

// Parse helpers.
func parseString(s string) (any, error)   { return s, nil }
func parseInt(s string) (any, error)      { return strconv.Atoi(s) }
func parseFloat(s string) (any, error)    { return strconv.ParseFloat(s, 64) }
func parseBool(s string) (any, error)     { return strconv.ParseBool(s) }
func parseDuration(s string) (any, error) { return time.ParseDuration(s) }

// LoadFromEnv overrides config fields with values from environment variables.
// Returns the number of overrides applied and any parse errors encountered.
//
// After applying named bindings it also honors the deprecated BATCH_SIZE and
// FLUSH_INTERVAL variables when the corresponding specific variables were not
// set. See ApplyLegacyEnv for precedence rules.
func (c *Config) LoadFromEnv() (int, error) {
	var count int
	for _, b := range envBindings {
		val, ok := os.LookupEnv(b.env)
		if !ok {
			continue
		}
		parsed, err := b.parse(val)
		if err != nil {
			return count, fmt.Errorf("env %s: %w", b.env, err)
		}
		b.apply(c, parsed)
		count++
	}
	legacy, err := c.ApplyLegacyEnv()
	if err != nil {
		return count, err
	}
	return count + legacy, nil
}

// ApplyLegacyEnv applies the deprecated BATCH_SIZE and FLUSH_INTERVAL
// environment variables.
//
// Precedence:
//  1. BATCHER_BATCH_SIZE / WRITER_BATCH_SIZE win when set.
//  2. Else BATCH_SIZE, when set to a positive int, fills whichever of the
//     batcher/writer sizes did not have a specific env var.
//  3. BATCHER_FLUSH_INTERVAL / WRITER_FLUSH_INTERVAL win when set.
//  4. Else FLUSH_INTERVAL fills whichever flush interval lacked a specific
//     env var.
//
// The check is "was the specific env var present", not "is the field still
// zero". DefaultConfig always populates non-zero defaults, so a zero-value
// check would silently ignore legacy env vars (the historical loadtest bug).
func (c *Config) ApplyLegacyEnv() (int, error) {
	var count int

	if v := os.Getenv("BATCH_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return count, fmt.Errorf("env BATCH_SIZE: %w", err)
		}
		if n <= 0 {
			return count, fmt.Errorf("env BATCH_SIZE: must be positive, got %d", n)
		}
		applied := false
		if _, ok := os.LookupEnv("BATCHER_BATCH_SIZE"); !ok {
			c.BatcherBatchSize = n
			applied = true
		}
		if _, ok := os.LookupEnv("WRITER_BATCH_SIZE"); !ok {
			c.WriterBatchSize = n
			applied = true
		}
		if applied {
			count++
		}
	}

	if v := os.Getenv("FLUSH_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return count, fmt.Errorf("env FLUSH_INTERVAL: %w", err)
		}
		applied := false
		if _, ok := os.LookupEnv("BATCHER_FLUSH_INTERVAL"); !ok {
			c.BatcherFlushInterval = d
			applied = true
		}
		if _, ok := os.LookupEnv("WRITER_FLUSH_INTERVAL"); !ok {
			c.WriterFlushInterval = d
			applied = true
		}
		if applied {
			count++
		}
	}

	return count, nil
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
				c.BatcherErrorSeverityThreshold,
			)
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
		if c.Notification.AlertIdleTTL <= 0 {
			return fmt.Errorf("notification.alert_idle_ttl must be positive, got %s", c.Notification.AlertIdleTTL)
		}
		if c.Notification.ResolvedAlertRetention <= 0 {
			return fmt.Errorf(
				"notification.resolved_alert_retention must be positive, got %s",
				c.Notification.ResolvedAlertRetention,
			)
		}
		if c.Notification.DLQRetention <= 0 {
			return fmt.Errorf("notification.dlq_retention must be positive, got %s", c.Notification.DLQRetention)
		}
		if c.Notification.BboltCompactionEnabled && c.Notification.BboltCompactionInterval <= 0 {
			return fmt.Errorf(
				"notification.bbolt_compaction_interval must be positive when compaction is enabled, got %s",
				c.Notification.BboltCompactionInterval,
			)
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
