package maintenance

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"codeberg.org/nicknad/otel-sqlite/internal/duration"
)

// fileMaintenanceConfig is the YAML-deserializable subset of maintenance config.
// Booleans use pointers to distinguish "not set" (nil) from "set to false".
type fileMaintenanceConfig struct {
	Maintenance struct {
		Enabled       *bool  `yaml:"enabled"`
		CheckInterval string `yaml:"check_interval"`
	} `yaml:"maintenance"`
	Retention struct {
		Enabled                *bool  `yaml:"enabled"`
		KeepLogs               string `yaml:"keep_logs"`
		KeepMetrics            string `yaml:"keep_metrics"`
		MetricRetentionEnabled *bool  `yaml:"metric_retention_enabled"`
		CleanupInterval        string `yaml:"cleanup_interval"`
		DeleteBatchSize        *int   `yaml:"delete_batch_size"`
	} `yaml:"retention"`
	Checkpoint struct {
		Enabled  *bool  `yaml:"enabled"`
		Interval string `yaml:"interval"`
		Mode     string `yaml:"mode"`
	} `yaml:"checkpoint"`
	Optimize struct {
		Enabled  *bool  `yaml:"enabled"`
		Interval string `yaml:"interval"`
	} `yaml:"optimize"`
	Vacuum struct {
		Enabled  *bool  `yaml:"enabled"`
		Interval string `yaml:"interval"`
	} `yaml:"vacuum"`
	Fts struct {
		Enabled         *bool  `yaml:"enabled"`
		RebuildInterval string `yaml:"rebuild_interval"`
	} `yaml:"fts"`
}

// LoadFile loads maintenance configuration from a YAML file.
// Values from the file override defaults. Environment variables take
// precedence over file values (handled separately via LoadFromEnv).
func (c *Config) LoadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file %q: %w", path, err)
	}

	var fc fileMaintenanceConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("parse config file %q: %w", path, err)
	}

	// Maintenance section.
	if fc.Maintenance.CheckInterval != "" {
		d, err := parseDurationExt(fc.Maintenance.CheckInterval)
		if err != nil {
			return fmt.Errorf("maintenance.check_interval: %w", err)
		}
		c.CheckInterval = d
	}
	if fc.Maintenance.Enabled != nil {
		c.MaintenanceEnabled = *fc.Maintenance.Enabled
	}

	// Retention section.
	if fc.Retention.Enabled != nil {
		c.RetentionEnabled = *fc.Retention.Enabled
	}
	if fc.Retention.KeepLogs != "" {
		d, err := parseDurationExt(fc.Retention.KeepLogs)
		if err != nil {
			return fmt.Errorf("retention.keep_logs: %w", err)
		}
		c.RetentionKeepLogs = d
	}
	if fc.Retention.KeepMetrics != "" {
		d, err := parseDurationExt(fc.Retention.KeepMetrics)
		if err != nil {
			return fmt.Errorf("retention.keep_metrics: %w", err)
		}
		c.RetentionKeepMetrics = d
	}
	if fc.Retention.MetricRetentionEnabled != nil {
		c.MetricRetentionEnabled = *fc.Retention.MetricRetentionEnabled
	}
	if fc.Retention.CleanupInterval != "" {
		d, err := parseDurationExt(fc.Retention.CleanupInterval)
		if err != nil {
			return fmt.Errorf("retention.cleanup_interval: %w", err)
		}
		c.RetentionCleanupInterval = d
	}
	if fc.Retention.DeleteBatchSize != nil {
		c.RetentionDeleteBatchSize = *fc.Retention.DeleteBatchSize
	}

	// Checkpoint section.
	if fc.Checkpoint.Enabled != nil {
		c.CheckpointEnabled = *fc.Checkpoint.Enabled
	}
	if fc.Checkpoint.Interval != "" {
		d, err := parseDurationExt(fc.Checkpoint.Interval)
		if err != nil {
			return fmt.Errorf("checkpoint.interval: %w", err)
		}
		c.CheckpointInterval = d
	}
	if fc.Checkpoint.Mode != "" {
		c.CheckpointMode = fc.Checkpoint.Mode
	}

	// Optimize section.
	if fc.Optimize.Enabled != nil {
		c.OptimizeEnabled = *fc.Optimize.Enabled
	}
	if fc.Optimize.Interval != "" {
		d, err := parseDurationExt(fc.Optimize.Interval)
		if err != nil {
			return fmt.Errorf("optimize.interval: %w", err)
		}
		c.OptimizeInterval = d
	}

	// Vacuum section.
	if fc.Vacuum.Enabled != nil {
		c.VacuumEnabled = *fc.Vacuum.Enabled
	}
	if fc.Vacuum.Interval != "" {
		d, err := parseDurationExt(fc.Vacuum.Interval)
		if err != nil {
			return fmt.Errorf("vacuum.interval: %w", err)
		}
		c.VacuumInterval = d
	}

	// FTS section.
	if fc.Fts.Enabled != nil {
		c.FTSEnabled = *fc.Fts.Enabled
	}
	if fc.Fts.RebuildInterval != "" {
		d, err := parseDurationExt(fc.Fts.RebuildInterval)
		if err != nil {
			return fmt.Errorf("fts.rebuild_interval: %w", err)
		}
		c.FTSRebuildInterval = d
	}

	return nil
}

// parseDurationExt parses a duration string supporting Go durations and "d" for days.
// Examples: "30d", "24h", "1h30m", "5s". The implementation lives in the
// shared internal/duration package (config and maintenance both use it).
func parseDurationExt(s string) (time.Duration, error) {
	return duration.Parse(s)
}
