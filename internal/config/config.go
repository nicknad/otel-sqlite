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

	// Batching configuration
	BatchSize     int           `mapstructure:"batch_size"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`

	// Observability
	MetricsAddress string `mapstructure:"metrics_address"`

	// Timeouts
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`

	// Maintenance configuration
	Maintenance *maintenance.Config `mapstructure:"maintenance"`
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		ListenAddress:        ":4317",
		SQLitePath:           "otel-logs.db",
		IngressQueueCapacity: 10000,
		BatchQueueCapacity:   1000,
		BatchSize:            100,
		FlushInterval:        5 * time.Second,
		MetricsAddress:       ":9090",
		ShutdownTimeout:      30 * time.Second,
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
	{Key: "BatchSize", Env: "BATCH_SIZE", Description: "Number of log records per batch"},
	{Key: "FlushInterval", Env: "FLUSH_INTERVAL", Description: "Maximum time between batch flushes"},
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
	case "BatchSize":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int %q: %w", value, err)
		}
		c.BatchSize = n
	case "FlushInterval":
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", value, err)
		}
		c.FlushInterval = d
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
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be positive, got %d", c.BatchSize)
	}
	if c.FlushInterval <= 0 {
		return fmt.Errorf("flush_interval must be positive, got %s", c.FlushInterval)
	}
	if c.MetricsAddress == "" {
		return fmt.Errorf("metrics_address must not be empty")
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
