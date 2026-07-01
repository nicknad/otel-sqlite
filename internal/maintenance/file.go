package maintenance

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// fileMaintenanceConfig is the YAML-deserializable subset of maintenance config.
// Booleans use pointers to distinguish "not set" (nil) from "set to false".
type fileMaintenanceConfig struct {
	Maintenance struct {
		Enabled       *bool  `yaml:"enabled"`
		CheckInterval string `yaml:"check_interval"`
	} `yaml:"maintenance"`
	Retention struct {
		Enabled         *bool  `yaml:"enabled"`
		KeepLogs        string `yaml:"keep_logs"`
		CleanupInterval string `yaml:"cleanup_interval"`
		DeleteBatchSize *int   `yaml:"delete_batch_size"`
	} `yaml:"retention"`
	Checkpoint struct {
		Enabled  *bool  `yaml:"enabled"`
		Interval string `yaml:"interval"`
	} `yaml:"checkpoint"`
	Optimize struct {
		Enabled  *bool  `yaml:"enabled"`
		Interval string `yaml:"interval"`
	} `yaml:"optimize"`
	Vacuum struct {
		Enabled  *bool  `yaml:"enabled"`
		Interval string `yaml:"interval"`
	} `yaml:"vacuum"`
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

	return nil
}

// parseDurationExt is a copy of config.parseDurationExt to avoid a
// circular dependency. It supports Go durations and "d" for days.
func parseDurationExt(s string) (time.Duration, error) {
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	if len(s) >= 2 && s[len(s)-1] == 'd' {
		var days int
		if _, err := fmt.Sscanf(s[:len(s)-1], "%d", &days); err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		if days < 0 {
			return 0, fmt.Errorf("invalid duration %q: negative days not allowed", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return 0, fmt.Errorf("invalid duration %q", s)
}
