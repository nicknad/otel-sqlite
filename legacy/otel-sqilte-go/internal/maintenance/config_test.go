package maintenance

import (
	"os"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()

	if !c.MaintenanceEnabled {
		t.Error("expected MaintenanceEnabled to be true")
	}
	if c.CheckInterval != 1*time.Hour {
		t.Errorf("expected CheckInterval=1h, got %s", c.CheckInterval)
	}
	if !c.RetentionEnabled {
		t.Error("expected RetentionEnabled to be true")
	}
	if c.RetentionKeepLogs != 30*24*time.Hour {
		t.Errorf("expected RetentionKeepLogs=30d, got %s", c.RetentionKeepLogs)
	}
	if !c.MetricRetentionEnabled {
		t.Error("expected MetricRetentionEnabled to be true")
	}
	if c.RetentionKeepMetrics != 30*24*time.Hour {
		t.Errorf("expected RetentionKeepMetrics=30d, got %s", c.RetentionKeepMetrics)
	}
	if c.RetentionCleanupInterval != 24*time.Hour {
		t.Errorf("expected RetentionCleanupInterval=24h, got %s", c.RetentionCleanupInterval)
	}
	if c.RetentionDeleteBatchSize != 10000 {
		t.Errorf("expected RetentionDeleteBatchSize=10000, got %d", c.RetentionDeleteBatchSize)
	}
	if !c.CheckpointEnabled {
		t.Error("expected CheckpointEnabled to be true")
	}
	if c.CheckpointInterval != 24*time.Hour {
		t.Errorf("expected CheckpointInterval=24h, got %s", c.CheckpointInterval)
	}
	if c.CheckpointMode != "PASSIVE" {
		t.Errorf("expected CheckpointMode=PASSIVE, got %q", c.CheckpointMode)
	}
	if !c.OptimizeEnabled {
		t.Error("expected OptimizeEnabled to be true")
	}
	if c.OptimizeInterval != 24*time.Hour {
		t.Errorf("expected OptimizeInterval=24h, got %s", c.OptimizeInterval)
	}
	if c.VacuumEnabled {
		t.Error("expected VacuumEnabled to be false")
	}
	if c.VacuumInterval != 7*24*time.Hour {
		t.Errorf("expected VacuumInterval=7d, got %s", c.VacuumInterval)
	}
}

func TestLoadFromEnv(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		check func(*testing.T, *Config)
	}{
		{
			name: "metric retention env vars",
			env: map[string]string{
				"METRIC_RETENTION_ENABLED": "false",
				"RETENTION_KEEP_METRICS":   "168h",
			},
			check: func(t *testing.T, c *Config) {
				t.Helper()
				if c.MetricRetentionEnabled {
					t.Error("expected MetricRetentionEnabled=false")
				}
				if c.RetentionKeepMetrics != 7*24*time.Hour {
					t.Errorf("expected RetentionKeepMetrics=7d, got %s", c.RetentionKeepMetrics)
				}
			},
		},
		{
			name: "no env vars",
			env:  map[string]string{},
			check: func(t *testing.T, c *Config) {
				if !c.MaintenanceEnabled {
					t.Error("expected default MaintenanceEnabled=true")
				}
			},
		},
		{
			name: "override maintenance enabled",
			env:  map[string]string{"MAINTENANCE_ENABLED": "false"},
			check: func(t *testing.T, c *Config) {
				if c.MaintenanceEnabled {
					t.Error("expected MaintenanceEnabled=false")
				}
			},
		},
		{
			name: "override check interval",
			env:  map[string]string{"MAINTENANCE_CHECK_INTERVAL": "30m"},
			check: func(t *testing.T, c *Config) {
				if c.CheckInterval != 30*time.Minute {
					t.Errorf("expected 30m, got %s", c.CheckInterval)
				}
			},
		},
		{
			name: "override retention settings",
			env: map[string]string{
				"RETENTION_ENABLED":           "true",
				"RETENTION_KEEP_LOGS":         "168h",
				"RETENTION_CLEANUP_INTERVAL":  "12h",
				"RETENTION_DELETE_BATCH_SIZE": "5000",
			},
			check: func(t *testing.T, c *Config) {
				if !c.RetentionEnabled {
					t.Error("expected RetentionEnabled=true")
				}
				if c.RetentionKeepLogs != 7*24*time.Hour {
					t.Errorf("expected 168h, got %s", c.RetentionKeepLogs)
				}
				if c.RetentionCleanupInterval != 12*time.Hour {
					t.Errorf("expected 12h, got %s", c.RetentionCleanupInterval)
				}
				if c.RetentionDeleteBatchSize != 5000 {
					t.Errorf("expected 5000, got %d", c.RetentionDeleteBatchSize)
				}
			},
		},
		{
			name: "override checkpoint mode",
			env: map[string]string{
				"CHECKPOINT_MODE": "TRUNCATE",
			},
			check: func(t *testing.T, c *Config) {
				if c.CheckpointMode != "TRUNCATE" {
					t.Errorf("expected CheckpointMode=TRUNCATE, got %q", c.CheckpointMode)
				}
			},
		},
		{
			name: "override all boolean flags",
			env: map[string]string{
				"CHECKPOINT_ENABLED": "false",
				"OPTIMIZE_ENABLED":   "false",
				"VACUUM_ENABLED":     "true",
			},
			check: func(t *testing.T, c *Config) {
				if c.CheckpointEnabled {
					t.Error("expected CheckpointEnabled=false")
				}
				if c.OptimizeEnabled {
					t.Error("expected OptimizeEnabled=false")
				}
				if !c.VacuumEnabled {
					t.Error("expected VacuumEnabled=true")
				}
			},
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
			name: "invalid duration",
			env:  map[string]string{"MAINTENANCE_CHECK_INTERVAL": "not-a-duration"},
		},
		{
			name: "invalid bool",
			env:  map[string]string{"RETENTION_ENABLED": "maybe"},
		},
		{
			name: "invalid int",
			env:  map[string]string{"RETENTION_DELETE_BATCH_SIZE": "huge"},
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
			name: "zero check interval",
			cfg: func() *Config {
				c := DefaultConfig()
				c.CheckInterval = 0
				return c
			}(),
			wantErr: true,
		},
		{
			name: "negative check interval",
			cfg: func() *Config {
				c := DefaultConfig()
				c.CheckInterval = -1 * time.Hour
				return c
			}(),
			wantErr: true,
		},
		{
			name: "retention enabled but zero keep_logs",
			cfg: func() *Config {
				c := DefaultConfig()
				c.RetentionKeepLogs = 0
				return c
			}(),
			wantErr: true,
		},
		{
			name: "retention enabled but zero cleanup interval",
			cfg: func() *Config {
				c := DefaultConfig()
				c.RetentionCleanupInterval = 0
				return c
			}(),
			wantErr: true,
		},
		{
			name: "retention enabled but zero batch size",
			cfg: func() *Config {
				c := DefaultConfig()
				c.RetentionDeleteBatchSize = 0
				return c
			}(),
			wantErr: true,
		},
		{
			name: "metric retention enabled but zero keep_metrics",
			cfg: func() *Config {
				c := DefaultConfig()
				c.RetentionKeepMetrics = 0
				return c
			}(),
			wantErr: true,
		},
		{
			name: "metric retention disabled with invalid keep_metrics (should pass)",
			cfg: func() *Config {
				c := DefaultConfig()
				c.MetricRetentionEnabled = false
				c.RetentionKeepMetrics = 0
				return c
			}(),
			wantErr: false,
		},
		{
			name: "retention disabled with invalid values (should pass)",
			cfg: func() *Config {
				c := DefaultConfig()
				c.RetentionEnabled = false
				c.RetentionKeepLogs = 0
				return c
			}(),
			wantErr: false,
		},
		{
			name: "checkpoint enabled but zero interval",
			cfg: func() *Config {
				c := DefaultConfig()
				c.CheckpointInterval = 0
				return c
			}(),
			wantErr: true,
		},
		{
			name: "checkpoint enabled with invalid mode",
			cfg: func() *Config {
				c := DefaultConfig()
				c.CheckpointMode = "INVALID"
				return c
			}(),
			wantErr: true,
		},
		{
			name: "checkpoint enabled with empty mode (valid, defaults to PASSIVE)",
			cfg: func() *Config {
				c := DefaultConfig()
				c.CheckpointMode = ""
				return c
			}(),
			wantErr: false,
		},
		{
			name: "vacuum disabled (should pass regardless)",
			cfg: func() *Config {
				c := DefaultConfig()
				c.VacuumEnabled = false
				c.VacuumInterval = 0
				return c
			}(),
			wantErr: false,
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
