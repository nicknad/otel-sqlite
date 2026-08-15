package maintenance

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all maintenance framework configuration.
type Config struct {
	// MaintenanceEnabled controls whether the maintenance worker runs.
	MaintenanceEnabled bool `yaml:"enabled"`

	// CheckInterval is how often the worker wakes to evaluate tasks.
	CheckInterval time.Duration `yaml:"check_interval"`

	// Retention config.
	RetentionEnabled         bool          `yaml:"-"`
	RetentionKeepLogs        time.Duration `yaml:"keep_logs"`
	RetentionCleanupInterval time.Duration `yaml:"cleanup_interval"`
	RetentionDeleteBatchSize int           `yaml:"delete_batch_size"`

	// Metric retention config. The cleanup interval and delete batch size
	// are shared with the log retention task (RetentionCleanupInterval /
	// RetentionDeleteBatchSize).
	MetricRetentionEnabled bool          `yaml:"-"`
	RetentionKeepMetrics   time.Duration `yaml:"keep_metrics"`

	// Checkpoint config.
	CheckpointEnabled  bool          `yaml:"-"`
	CheckpointInterval time.Duration `yaml:"interval"`
	CheckpointMode     string        `yaml:"-"`

	// Optimize config.
	OptimizeEnabled  bool          `yaml:"-"`
	OptimizeInterval time.Duration `yaml:"interval"`

	// Vacuum config.
	VacuumEnabled  bool          `yaml:"-"`
	VacuumInterval time.Duration `yaml:"interval"`

	// FTS rebuild config.
	FTSEnabled         bool          `yaml:"-"`
	FTSRebuildInterval time.Duration `yaml:"rebuild_interval"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		MaintenanceEnabled:       true,
		CheckInterval:            1 * time.Hour,
		RetentionEnabled:         true,
		RetentionKeepLogs:        30 * 24 * time.Hour,
		RetentionCleanupInterval: 24 * time.Hour,
		RetentionDeleteBatchSize: 10000,
		MetricRetentionEnabled:   true,
		RetentionKeepMetrics:     30 * 24 * time.Hour,
		CheckpointEnabled:        true,
		CheckpointInterval:       24 * time.Hour,
		CheckpointMode:           "PASSIVE",
		OptimizeEnabled:          true,
		OptimizeInterval:         24 * time.Hour,
		VacuumEnabled:            false,
		VacuumInterval:           7 * 24 * time.Hour,
		FTSEnabled:               true,
		FTSRebuildInterval:       1 * time.Hour,
	}
}

// LoadFromEnv overrides config fields from environment variables.
// Returns the number of overrides applied and any parse errors.
func (c *Config) LoadFromEnv() (int, error) {
	envMap := map[string]func(string) error{
		"MAINTENANCE_ENABLED":         func(v string) error { return parseBool(&c.MaintenanceEnabled, v) },
		"MAINTENANCE_CHECK_INTERVAL":  func(v string) error { return parseDuration(&c.CheckInterval, v) },
		"RETENTION_ENABLED":           func(v string) error { return parseBool(&c.RetentionEnabled, v) },
		"RETENTION_KEEP_LOGS":         func(v string) error { return parseDuration(&c.RetentionKeepLogs, v) },
		"RETENTION_CLEANUP_INTERVAL":  func(v string) error { return parseDuration(&c.RetentionCleanupInterval, v) },
		"RETENTION_DELETE_BATCH_SIZE": func(v string) error { return parseInt(&c.RetentionDeleteBatchSize, v) },
		"METRIC_RETENTION_ENABLED":    func(v string) error { return parseBool(&c.MetricRetentionEnabled, v) },
		"RETENTION_KEEP_METRICS":      func(v string) error { return parseDuration(&c.RetentionKeepMetrics, v) },
		"CHECKPOINT_ENABLED":          func(v string) error { return parseBool(&c.CheckpointEnabled, v) },
		"CHECKPOINT_INTERVAL":         func(v string) error { return parseDuration(&c.CheckpointInterval, v) },
		"CHECKPOINT_MODE":             func(v string) error { c.CheckpointMode = v; return nil },
		"OPTIMIZE_ENABLED":            func(v string) error { return parseBool(&c.OptimizeEnabled, v) },
		"OPTIMIZE_INTERVAL":           func(v string) error { return parseDuration(&c.OptimizeInterval, v) },
		"VACUUM_ENABLED":              func(v string) error { return parseBool(&c.VacuumEnabled, v) },
		"VACUUM_INTERVAL":             func(v string) error { return parseDuration(&c.VacuumInterval, v) },
		"FTS_ENABLED":                 func(v string) error { return parseBool(&c.FTSEnabled, v) },
		"FTS_REBUILD_INTERVAL":        func(v string) error { return parseDuration(&c.FTSRebuildInterval, v) },
	}

	count := 0
	for envName, setter := range envMap {
		val, ok := os.LookupEnv(envName)
		if !ok {
			continue
		}
		if err := setter(val); err != nil {
			return count, fmt.Errorf("env %s: %w", envName, err)
		}
		count++
	}
	return count, nil
}

// Validate checks the configuration for errors.
// Invalid durations, negative intervals, etc. fail fast.
func (c *Config) Validate() error {
	if c.CheckInterval <= 0 {
		return fmt.Errorf("maintenance.check_interval must be positive, got %s", c.CheckInterval)
	}

	if c.RetentionEnabled {
		if c.RetentionKeepLogs <= 0 {
			return fmt.Errorf("retention.keep_logs must be positive, got %s", c.RetentionKeepLogs)
		}
		if c.RetentionCleanupInterval <= 0 {
			return fmt.Errorf("retention.cleanup_interval must be positive, got %s", c.RetentionCleanupInterval)
		}
		if c.RetentionDeleteBatchSize <= 0 {
			return fmt.Errorf("retention.delete_batch_size must be positive, got %d", c.RetentionDeleteBatchSize)
		}
	}

	if c.MetricRetentionEnabled {
		if c.RetentionKeepMetrics <= 0 {
			return fmt.Errorf("retention.keep_metrics must be positive, got %s", c.RetentionKeepMetrics)
		}
	}

	if c.CheckpointEnabled {
		if c.CheckpointInterval <= 0 {
			return fmt.Errorf("checkpoint.interval must be positive, got %s", c.CheckpointInterval)
		}
		switch c.CheckpointMode {
		case "PASSIVE", "FULL", "RESTART", "TRUNCATE", "":
			// valid
		default:
			return fmt.Errorf(
				"checkpoint.mode must be one of PASSIVE, FULL, RESTART, TRUNCATE, got %q",
				c.CheckpointMode)
		}
	}

	if c.OptimizeEnabled && c.OptimizeInterval <= 0 {
		return fmt.Errorf("optimize.interval must be positive, got %s", c.OptimizeInterval)
	}

	if c.VacuumEnabled && c.VacuumInterval <= 0 {
		return fmt.Errorf("vacuum.interval must be positive, got %s", c.VacuumInterval)
	}

	if c.FTSEnabled && c.FTSRebuildInterval <= 0 {
		return fmt.Errorf("fts.rebuild_interval must be positive, got %s", c.FTSRebuildInterval)
	}

	return nil
}

// parseBool parses a boolean string and sets the target.
func parseBool(target *bool, value string) error {
	b, err := strconv.ParseBool(value)
	if err != nil {
		return fmt.Errorf("invalid bool %q: %w", value, err)
	}
	*target = b
	return nil
}

// parseDuration parses a duration string and sets the target.
func parseDuration(target *time.Duration, value string) error {
	d, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value, err)
	}
	*target = d
	return nil
}

// parseInt parses an integer string and sets the target.
func parseInt(target *int, value string) error {
	n, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("invalid int %q: %w", value, err)
	}
	*target = n
	return nil
}
