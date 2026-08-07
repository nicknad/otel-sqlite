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
	if c.BatchQueueCapacity != 5000 {
		t.Errorf("expected 5000, got %d", c.BatchQueueCapacity)
	}
	if c.BatcherBatchSize != 250 {
		t.Errorf("expected 250, got %d", c.BatcherBatchSize)
	}
	if c.BatcherFlushInterval != 1*time.Second {
		t.Errorf("expected 1s, got %s", c.BatcherFlushInterval)
	}
	if c.WriterBatchSize != 100 {
		t.Errorf("expected 100, got %d", c.WriterBatchSize)
	}
	if c.WriterFlushInterval != 1*time.Second {
		t.Errorf("expected 1s, got %s", c.WriterFlushInterval)
	}
	if c.MetricsAddress != ":9090" {
		t.Errorf("expected :9090, got %q", c.MetricsAddress)
	}
	if c.ShutdownTimeout != 30*time.Second {
		t.Errorf("expected 30s, got %s", c.ShutdownTimeout)
	}
	if c.GrpcMaxRecvMsgSize != 16*1024*1024 {
		t.Errorf("expected %d, got %d", 16*1024*1024, c.GrpcMaxRecvMsgSize)
	}
	if c.GrpcMaxSendMsgSize != 16*1024*1024 {
		t.Errorf("expected %d, got %d", 16*1024*1024, c.GrpcMaxSendMsgSize)
	}
	if c.GrpcMaxConcurrentStreams != 100 {
		t.Errorf("expected 100, got %d", c.GrpcMaxConcurrentStreams)
	}
	if c.IngressQueueBackpressureThreshold != 0 {
		t.Errorf("expected 0, got %f", c.IngressQueueBackpressureThreshold)
	}
	if c.GoMemoryLimitMB != 0 {
		t.Errorf("expected 0, got %d", c.GoMemoryLimitMB)
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
				"BATCHER_BATCH_SIZE":     "50",
				"WRITER_BATCH_SIZE":      "75",
			},
			check: func(t *testing.T, c *Config) {
				if c.IngressQueueCapacity != 5000 {
					t.Errorf("expected 5000, got %d", c.IngressQueueCapacity)
				}
				if c.BatchQueueCapacity != 200 {
					t.Errorf("expected 200, got %d", c.BatchQueueCapacity)
				}
				if c.BatcherBatchSize != 50 {
					t.Errorf("expected 50, got %d", c.BatcherBatchSize)
				}
				if c.WriterBatchSize != 75 {
					t.Errorf("expected 75, got %d", c.WriterBatchSize)
				}
			},
		},
		{
			name: "override duration fields",
			env: map[string]string{
				"BATCHER_FLUSH_INTERVAL": "10s",
				"WRITER_FLUSH_INTERVAL":  "15s",
				"SHUTDOWN_TIMEOUT":       "60s",
			},
			check: func(t *testing.T, c *Config) {
				if c.BatcherFlushInterval != 10*time.Second {
					t.Errorf("expected 10s, got %s", c.BatcherFlushInterval)
				}
				if c.WriterFlushInterval != 15*time.Second {
					t.Errorf("expected 15s, got %s", c.WriterFlushInterval)
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

func TestLoadFromEnv_legacyBatchSizeAndFlush(t *testing.T) {
	tests := []struct {
		name             string
		env              map[string]string
		wantBatcherSize  int
		wantWriterSize   int
		wantBatcherFlush time.Duration
		wantWriterFlush  time.Duration
		wantOverridesMin int
	}{
		{
			name:             "BATCH_SIZE alone fills both sizes",
			env:              map[string]string{"BATCH_SIZE": "500"},
			wantBatcherSize:  500,
			wantWriterSize:   500,
			wantBatcherFlush: time.Second, // default
			wantWriterFlush:  time.Second,
			wantOverridesMin: 1,
		},
		{
			name:             "FLUSH_INTERVAL alone fills both flushes",
			env:              map[string]string{"FLUSH_INTERVAL": "250ms"},
			wantBatcherSize:  250,
			wantWriterSize:   100,
			wantBatcherFlush: 250 * time.Millisecond,
			wantWriterFlush:  250 * time.Millisecond,
			wantOverridesMin: 1,
		},
		{
			name: "specific batch sizes win over BATCH_SIZE",
			env: map[string]string{
				"BATCH_SIZE":         "500",
				"BATCHER_BATCH_SIZE": "10",
				"WRITER_BATCH_SIZE":  "20",
			},
			wantBatcherSize:  10,
			wantWriterSize:   20,
			wantBatcherFlush: time.Second,
			wantWriterFlush:  time.Second,
			wantOverridesMin: 2, // two specific bindings; legacy applies nothing
		},
		{
			name: "BATCH_SIZE fills only unset side",
			env: map[string]string{
				"BATCH_SIZE":        "500",
				"WRITER_BATCH_SIZE": "20",
			},
			wantBatcherSize:  500,
			wantWriterSize:   20,
			wantBatcherFlush: time.Second,
			wantWriterFlush:  time.Second,
			wantOverridesMin: 2,
		},
		{
			name: "specific flush wins over FLUSH_INTERVAL",
			env: map[string]string{
				"FLUSH_INTERVAL":         "250ms",
				"BATCHER_FLUSH_INTERVAL": "3s",
				"WRITER_FLUSH_INTERVAL":  "4s",
			},
			wantBatcherSize:  250,
			wantWriterSize:   100,
			wantBatcherFlush: 3 * time.Second,
			wantWriterFlush:  4 * time.Second,
			wantOverridesMin: 2,
		},
		{
			name: "loadtest-style legacy env is honored over defaults",
			env: map[string]string{
				"BATCH_SIZE":     "500",
				"FLUSH_INTERVAL": "1s",
			},
			wantBatcherSize:  500,
			wantWriterSize:   500,
			wantBatcherFlush: time.Second,
			wantWriterFlush:  time.Second,
			wantOverridesMin: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			c := DefaultConfig()
			n, err := c.LoadFromEnv()
			if err != nil {
				t.Fatalf("LoadFromEnv() error: %v", err)
			}
			if n < tt.wantOverridesMin {
				t.Errorf("overrides = %d, want at least %d", n, tt.wantOverridesMin)
			}
			if c.BatcherBatchSize != tt.wantBatcherSize {
				t.Errorf("BatcherBatchSize = %d, want %d", c.BatcherBatchSize, tt.wantBatcherSize)
			}
			if c.WriterBatchSize != tt.wantWriterSize {
				t.Errorf("WriterBatchSize = %d, want %d", c.WriterBatchSize, tt.wantWriterSize)
			}
			if c.BatcherFlushInterval != tt.wantBatcherFlush {
				t.Errorf("BatcherFlushInterval = %s, want %s", c.BatcherFlushInterval, tt.wantBatcherFlush)
			}
			if c.WriterFlushInterval != tt.wantWriterFlush {
				t.Errorf("WriterFlushInterval = %s, want %s", c.WriterFlushInterval, tt.wantWriterFlush)
			}
		})
	}
}

func TestApplyLegacyEnv_errors(t *testing.T) {
	t.Run("invalid BATCH_SIZE", func(t *testing.T) {
		t.Setenv("BATCH_SIZE", "nope")
		c := DefaultConfig()
		if _, err := c.ApplyLegacyEnv(); err == nil {
			t.Fatal("expected error for invalid BATCH_SIZE")
		}
	})
	t.Run("non-positive BATCH_SIZE", func(t *testing.T) {
		t.Setenv("BATCH_SIZE", "0")
		c := DefaultConfig()
		if _, err := c.ApplyLegacyEnv(); err == nil {
			t.Fatal("expected error for non-positive BATCH_SIZE")
		}
	})
	t.Run("invalid FLUSH_INTERVAL", func(t *testing.T) {
		t.Setenv("FLUSH_INTERVAL", "nope")
		c := DefaultConfig()
		if _, err := c.ApplyLegacyEnv(); err == nil {
			t.Fatal("expected error for invalid FLUSH_INTERVAL")
		}
	})
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
			env:  map[string]string{"BATCHER_FLUSH_INTERVAL": "not-a-duration"},
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
			cfg:     &Config{ListenAddress: "", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "empty sqlite path",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "zero ingress capacity",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 0, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "negative batcher batch size",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: -1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "negative writer batch size",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: -1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "zero batcher flush interval",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: 0, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "zero writer flush interval",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: 0, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "empty metrics address",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: "", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "zero shutdown timeout",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, ShutdownTimeout: 0}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "zero grpc max recv msg size",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 0, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "zero grpc max send msg size",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 0, GrpcMaxConcurrentStreams: 1, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "zero grpc max concurrent streams",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 0, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "backpressure threshold above 1",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, IngressQueueBackpressureThreshold: 1.5, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "backpressure threshold below 0",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, IngressQueueBackpressureThreshold: -0.1, ShutdownTimeout: time.Second}, //nolint:lll
			wantErr: true,
		},
		{
			name:    "negative go memory limit",
			cfg:     &Config{ListenAddress: ":4317", SQLitePath: "db", IngressQueueCapacity: 1, BatchQueueCapacity: 1, BatcherBatchSize: 1, BatcherFlushInterval: time.Second, WriterBatchSize: 1, WriterFlushInterval: time.Second, MetricsAddress: ":9090", GrpcMaxRecvMsgSize: 1, GrpcMaxSendMsgSize: 1, GrpcMaxConcurrentStreams: 1, GoMemoryLimitMB: -1, ShutdownTimeout: time.Second}, //nolint:lll
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
