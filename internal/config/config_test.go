package config

import (
	"os"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	if c.ListenAddress != ":4317" {
		t.Errorf("expected :4317, got %q", c.ListenAddress)
	}
	if c.SQLitePath != "otel-logs.db" {
		t.Errorf("expected otel-logs.db, got %q", c.SQLitePath)
	}
	if c.IngressQueueCapacity != 10000 {
		t.Errorf("expected 10000, got %d", c.IngressQueueCapacity)
	}
	if c.BatchQueueCapacity != 1000 {
		t.Errorf("expected 1000, got %d", c.BatchQueueCapacity)
	}
	if c.BatchSize != 100 {
		t.Errorf("expected 100, got %d", c.BatchSize)
	}
	if c.FlushInterval != 5*time.Second {
		t.Errorf("expected 5s, got %s", c.FlushInterval)
	}
	if c.MetricsAddress != ":9090" {
		t.Errorf("expected :9090, got %q", c.MetricsAddress)
	}
	if c.ShutdownTimeout != 30*time.Second {
		t.Errorf("expected 30s, got %s", c.ShutdownTimeout)
	}
}

func TestLoadFromEnv(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		check func(*testing.T, *Config)
	}{
		{
			name: "no env vars",
			env:  map[string]string{},
			check: func(t *testing.T, c *Config) {
				if c.ListenAddress != ":4317" {
					t.Errorf("expected :4317, got %q", c.ListenAddress)
				}
			},
		},
		{
			name: "override listen address",
			env:  map[string]string{"LISTEN_ADDRESS": ":9999"},
			check: func(t *testing.T, c *Config) {
				if c.ListenAddress != ":9999" {
					t.Errorf("expected :9999, got %q", c.ListenAddress)
				}
			},
		},
		{
			name: "override all string fields",
			env: map[string]string{
				"LISTEN_ADDRESS":  ":443",
				"SQLITE_PATH":     "/data/logs.db",
				"METRICS_ADDRESS": ":8080",
			},
			check: func(t *testing.T, c *Config) {
				if c.ListenAddress != ":443" {
					t.Errorf("expected :443, got %q", c.ListenAddress)
				}
				if c.SQLitePath != "/data/logs.db" {
					t.Errorf("expected /data/logs.db, got %q", c.SQLitePath)
				}
				if c.MetricsAddress != ":8080" {
					t.Errorf("expected :8080, got %q", c.MetricsAddress)
				}
			},
		},
		{
			name: "override int fields",
			env: map[string]string{
				"INGRESS_QUEUE_CAPACITY": "5000",
				"BATCH_QUEUE_CAPACITY":   "200",
				"BATCH_SIZE":             "50",
			},
			check: func(t *testing.T, c *Config) {
				if c.IngressQueueCapacity != 5000 {
					t.Errorf("expected 5000, got %d", c.IngressQueueCapacity)
				}
				if c.BatchQueueCapacity != 200 {
					t.Errorf("expected 200, got %d", c.BatchQueueCapacity)
				}
				if c.BatchSize != 50 {
					t.Errorf("expected 50, got %d", c.BatchSize)
				}
			},
		},
		{
			name: "override duration fields",
			env: map[string]string{
				"FLUSH_INTERVAL":   "10s",
				"SHUTDOWN_TIMEOUT": "60s",
			},
			check: func(t *testing.T, c *Config) {
				if c.FlushInterval != 10*time.Second {
					t.Errorf("expected 10s, got %s", c.FlushInterval)
				}
				if c.ShutdownTimeout != 60*time.Second {
					t.Errorf("expected 60s, got %s", c.ShutdownTimeout)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set env vars
			for k, v := range tt.env {
				os.Setenv(k, v)
			}
			defer func() {
				for k := range tt.env {
					os.Unsetenv(k)
				}
			}()

			c := DefaultConfig()
			n, err := c.LoadFromEnv()
			if err != nil {
				t.Fatalf("LoadFromEnv() error: %v", err)
			}
			if n != len(tt.env) {
				t.Errorf("expected %d overrides, got %d", len(tt.env), n)
			}
			tt.check(t, c)
		})
	}
}

func TestLoadFromEnv_errors(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "invalid int",
			env:  map[string]string{"INGRESS_QUEUE_CAPACITY": "not-a-number"},
		},
		{
			name: "invalid duration",
			env:  map[string]string{"FLUSH_INTERVAL": "not-a-duration"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				os.Setenv(k, v)
			}
			defer func() {
				for k := range tt.env {
					os.Unsetenv(k)
				}
			}()

			c := DefaultConfig()
			_, err := c.LoadFromEnv()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name:    "valid defaults",
			cfg:     DefaultConfig(),
			wantErr: false,
		},
		{
			name:    "empty listen address",
			cfg:     &Config{ListenAddress: "", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatchSize: 1, FlushInterval: time.Second, MetricsAddress: ":9090", ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "empty sqlite path",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatchSize: 1, FlushInterval: time.Second, MetricsAddress: ":9090", ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "zero ingress capacity",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 0, BatchQueueCapacity: 1, BatchSize: 1, FlushInterval: time.Second, MetricsAddress: ":9090", ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "negative batch size",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatchSize: -1, FlushInterval: time.Second, MetricsAddress: ":9090", ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "zero flush interval",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatchSize: 1, FlushInterval: 0, MetricsAddress: ":9090", ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "empty metrics address",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatchSize: 1, FlushInterval: time.Second, MetricsAddress: "", ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "zero shutdown timeout",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatchSize: 1, FlushInterval: time.Second, MetricsAddress: ":9090", ShutdownTimeout: 0}, //nolint:lll
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
