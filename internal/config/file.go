package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// FileConfig mirrors the top-level YAML config structure.
// Maintenance sections (maintenance, retention, checkpoint, etc.) are handled
// separately by the maintenance package's LoadFile.
type FileConfig struct {
	ListenAddress                 string `yaml:"listen_address"`
	SQLitePath                    string `yaml:"sqlite_path"`
	IngressQueueCapacity          int    `yaml:"ingress_queue_capacity"`
	BatchQueueCapacity            int    `yaml:"batch_queue_capacity"`
	BatcherBatchSize              int    `yaml:"batcher_batch_size"`
	BatcherFlushInterval          string `yaml:"batcher_flush_interval"`
	BatcherErrorSeverityThreshold string `yaml:"batcher_error_severity_threshold"`
	WriterBatchSize               int    `yaml:"writer_batch_size"`
	WriterFlushInterval           string `yaml:"writer_flush_interval"`
	// Legacy fields (mapped to both batcher and writer if specific fields unset)
	BatchSize                         int     `yaml:"batch_size"`
	FlushInterval                     string  `yaml:"flush_interval"`
	GrpcMaxRecvMsgSize                int     `yaml:"grpc_max_recv_msg_size"`
	GrpcMaxSendMsgSize                int     `yaml:"grpc_max_send_msg_size"`
	GrpcMaxConcurrentStreams          int     `yaml:"grpc_max_concurrent_streams"`
	IngressQueueBackpressureThreshold float64 `yaml:"ingress_queue_backpressure_threshold"`
	GoMemoryLimitMB                   int     `yaml:"go_memory_limit_mb"`
	MetricsAddress                    string  `yaml:"metrics_address"`
	ShutdownTimeout                   string  `yaml:"shutdown_timeout"`

	// Notification
	Notification *FileNotificationConfig `yaml:"notification"`
}

// FileNotificationConfig mirrors the notification section in YAML.
type FileNotificationConfig struct {
	Enabled         bool                          `yaml:"enabled"`
	EventQueueDepth int                           `yaml:"event_queue_depth"`
	StorePath       string                        `yaml:"store_path"`
	RetryInterval   string                        `yaml:"retry_interval"`
	Notifiers       map[string]FileNotifierConfig `yaml:"notifiers"`
	Rules           []FileRuleConfig              `yaml:"rules"`
}

// FileNotifierConfig mirrors a notifier config in YAML.
type FileNotifierConfig struct {
	Type       string `yaml:"type"`
	URL        string `yaml:"url"`
	Timeout    string `yaml:"timeout"`
	AuthHeader string `yaml:"auth_header"`
}

// FileRuleConfig mirrors a rule in YAML.
type FileRuleConfig struct {
	Name             string            `yaml:"name"`
	MatchSeverity    string            `yaml:"match_severity"`
	ResourceFilter   string            `yaml:"resource_filter"`
	BodyFilter       string            `yaml:"body_filter"`
	AttributeFilters map[string]string `yaml:"attribute_filters"`
	Cooldown         string            `yaml:"cooldown"`
	RateLimit        int               `yaml:"rate_limit"`
	RateWindow       string            `yaml:"rate_window"`
	DedupWindow      string            `yaml:"dedup_window"`
	MaxRetries       int               `yaml:"max_retries"`
	RetryBackoff     string            `yaml:"retry_backoff"`
	Destination      string            `yaml:"destination"`
}

// LoadFile loads configuration from a YAML file.
// Values from the file override defaults. Environment variables take
// precedence over file values (handled separately via LoadFromEnv).
func (c *Config) LoadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file %q: %w", path, err)
	}

	var fc FileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("parse config file %q: %w", path, err)
	}

	// Apply simple string fields.
	if fc.ListenAddress != "" {
		c.ListenAddress = fc.ListenAddress
	}
	if fc.SQLitePath != "" {
		c.SQLitePath = fc.SQLitePath
	}
	if fc.GrpcMaxRecvMsgSize != 0 {
		c.GrpcMaxRecvMsgSize = fc.GrpcMaxRecvMsgSize
	}
	if fc.GrpcMaxSendMsgSize != 0 {
		c.GrpcMaxSendMsgSize = fc.GrpcMaxSendMsgSize
	}
	if fc.GrpcMaxConcurrentStreams != 0 {
		c.GrpcMaxConcurrentStreams = fc.GrpcMaxConcurrentStreams
	}
	if fc.IngressQueueBackpressureThreshold > 0 {
		c.IngressQueueBackpressureThreshold = fc.IngressQueueBackpressureThreshold
	}
	if fc.GoMemoryLimitMB != 0 {
		c.GoMemoryLimitMB = fc.GoMemoryLimitMB
	}
	if fc.MetricsAddress != "" {
		c.MetricsAddress = fc.MetricsAddress
	}

	// Apply int fields (only if non-zero to distinguish "unset" from "set to 0").
	if fc.IngressQueueCapacity != 0 {
		c.IngressQueueCapacity = fc.IngressQueueCapacity
	}
	if fc.BatchQueueCapacity != 0 {
		c.BatchQueueCapacity = fc.BatchQueueCapacity
	}

	// Apply batcher/writer batch sizes. Legacy BatchSize applies to both
	// if the specific fields are not set.
	if fc.BatcherBatchSize != 0 {
		c.BatcherBatchSize = fc.BatcherBatchSize
	} else if fc.BatchSize != 0 {
		c.BatcherBatchSize = fc.BatchSize
	}
	if fc.WriterBatchSize != 0 {
		c.WriterBatchSize = fc.WriterBatchSize
	} else if fc.BatchSize != 0 {
		c.WriterBatchSize = fc.BatchSize
	}

	// Parse duration fields.
	if fc.BatcherFlushInterval != "" {
		d, err := parseDurationExt(fc.BatcherFlushInterval)
		if err != nil {
			return fmt.Errorf("batcher_flush_interval: %w", err)
		}
		c.BatcherFlushInterval = d
	} else if fc.FlushInterval != "" {
		d, err := parseDurationExt(fc.FlushInterval)
		if err != nil {
			return fmt.Errorf("flush_interval: %w", err)
		}
		c.BatcherFlushInterval = d
	}
	if fc.BatcherErrorSeverityThreshold != "" {
		c.BatcherErrorSeverityThreshold = fc.BatcherErrorSeverityThreshold
	}
	if fc.WriterFlushInterval != "" {
		d, err := parseDurationExt(fc.WriterFlushInterval)
		if err != nil {
			return fmt.Errorf("writer_flush_interval: %w", err)
		}
		c.WriterFlushInterval = d
	} else if fc.FlushInterval != "" {
		d, err := parseDurationExt(fc.FlushInterval)
		if err != nil {
			return fmt.Errorf("flush_interval: %w", err)
		}
		c.WriterFlushInterval = d
	}
	if fc.ShutdownTimeout != "" {
		d, err := parseDurationExt(fc.ShutdownTimeout)
		if err != nil {
			return fmt.Errorf("shutdown_timeout: %w", err)
		}
		c.ShutdownTimeout = d
	}

	// Apply notification config.
	if fc.Notification != nil {
		nc := c.ensureNotification()
		nc.Enabled = fc.Notification.Enabled
		if fc.Notification.EventQueueDepth > 0 {
			nc.EventQueueDepth = fc.Notification.EventQueueDepth
		}
		if fc.Notification.StorePath != "" {
			nc.StorePath = fc.Notification.StorePath
		}
		if fc.Notification.RetryInterval != "" {
			d, err := parseDurationExt(fc.Notification.RetryInterval)
			if err != nil {
				return fmt.Errorf("notification.retry_interval: %w", err)
			}
			nc.RetryInterval = d
		}

		// Load notifier configs.
		if nc.Notifiers == nil {
			nc.Notifiers = make(map[string]NotifierConfig)
		}
		for name, fn := range fc.Notification.Notifiers {
			ncfg := NotifierConfig{
				Type:       fn.Type,
				URL:        fn.URL,
				AuthHeader: fn.AuthHeader,
			}
			if fn.Timeout != "" {
				d, err := parseDurationExt(fn.Timeout)
				if err != nil {
					return fmt.Errorf("notification.notifiers[%s].timeout: %w", name, err)
				}
				ncfg.Timeout = d
			}
			nc.Notifiers[name] = ncfg
		}

		for i := range fc.Notification.Rules {
			fr := &fc.Notification.Rules[i]
			rc := RuleConfig{
				Name:             fr.Name,
				MatchSeverity:    fr.MatchSeverity,
				ResourceFilter:   fr.ResourceFilter,
				BodyFilter:       fr.BodyFilter,
				AttributeFilters: fr.AttributeFilters,
				RateLimit:        fr.RateLimit,
				MaxRetries:       fr.MaxRetries,
				Destination:      fr.Destination,
			}
			if fr.Cooldown != "" {
				d, err := parseDurationExt(fr.Cooldown)
				if err != nil {
					return fmt.Errorf("notification.rules[%s].cooldown: %w", fr.Name, err)
				}
				rc.Cooldown = d.String()
			}
			if fr.RateWindow != "" {
				d, err := parseDurationExt(fr.RateWindow)
				if err != nil {
					return fmt.Errorf("notification.rules[%s].rate_window: %w", fr.Name, err)
				}
				rc.RateWindow = d.String()
			}
			if fr.DedupWindow != "" {
				d, err := parseDurationExt(fr.DedupWindow)
				if err != nil {
					return fmt.Errorf("notification.rules[%s].dedup_window: %w", fr.Name, err)
				}
				rc.DedupWindow = d.String()
			}
			if fr.RetryBackoff != "" {
				d, err := parseDurationExt(fr.RetryBackoff)
				if err != nil {
					return fmt.Errorf("notification.rules[%s].retry_backoff: %w", fr.Name, err)
				}
				rc.RetryBackoff = d.String()
			}
			nc.Rules = append(nc.Rules, rc)
		}
	}

	return nil
}

// parseDurationExt parses a duration string supporting Go durations and "d" for days.
// Examples: "30d", "24h", "1h30m", "5s".
func parseDurationExt(s string) (time.Duration, error) {
	// Try standard Go duration first.
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// Custom handling for "d" suffix (days).
	if len(s) >= 2 && s[len(s)-1] == 'd' {
		daysStr := s[:len(s)-1]
		var days int
		if _, err := fmt.Sscanf(daysStr, "%d", &days); err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		if days < 0 {
			return 0, fmt.Errorf("invalid duration %q: negative days not allowed", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}

	return 0, fmt.Errorf("invalid duration %q", s)
}
