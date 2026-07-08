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
	AlertStorePath  string                        `yaml:"alert_store_path"`
	RetryInterval   string                        `yaml:"retry_interval"`
	GCInterval      string                        `yaml:"gc_interval"`
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
	MaxRetries       int               `yaml:"max_retries"`
	RetryBackoff     string            `yaml:"retry_backoff"`
	Destination      string            `yaml:"destination"`

	// Alert-specific fields.
	AlertWindow        string `yaml:"alert_window"`
	AlertThreshold     int    `yaml:"alert_threshold"`
	AlertResolveWindow string `yaml:"alert_resolve_window"`
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

	if err := c.loadCoreConfig(&fc); err != nil {
		return err
	}
	return c.loadNotificationConfig(&fc)
}

// loadCoreConfig applies top-level scalar fields from a FileConfig.
func (c *Config) loadCoreConfig(fc *FileConfig) error {
	// String fields.
	if fc.ListenAddress != "" {
		c.ListenAddress = fc.ListenAddress
	}
	if fc.SQLitePath != "" {
		c.SQLitePath = fc.SQLitePath
	}
	if fc.MetricsAddress != "" {
		c.MetricsAddress = fc.MetricsAddress
	}
	if fc.BatcherErrorSeverityThreshold != "" {
		c.BatcherErrorSeverityThreshold = fc.BatcherErrorSeverityThreshold
	}

	// Int fields (only if non-zero to distinguish "unset" from "set to 0").
	if fc.IngressQueueCapacity != 0 {
		c.IngressQueueCapacity = fc.IngressQueueCapacity
	}
	if fc.BatchQueueCapacity != 0 {
		c.BatchQueueCapacity = fc.BatchQueueCapacity
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

	// Legacy BatchSize falls back to both batcher/writer if specific fields unset.
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

	// Duration fields.
	var err error
	err = parseOptionalDuration(
		fc.BatcherFlushInterval, &c.BatcherFlushInterval, "batcher_flush_interval",
	)
	if err != nil {
		return err
	}
	if fc.BatcherFlushInterval == "" {
		err = parseOptionalDuration(fc.FlushInterval, &c.BatcherFlushInterval, "flush_interval")
		if err != nil {
			return err
		}
	}
	err = parseOptionalDuration(
		fc.WriterFlushInterval, &c.WriterFlushInterval, "writer_flush_interval",
	)
	if err != nil {
		return err
	}
	if fc.WriterFlushInterval == "" {
		err = parseOptionalDuration(fc.FlushInterval, &c.WriterFlushInterval, "flush_interval")
		if err != nil {
			return err
		}
	}
	return parseOptionalDuration(fc.ShutdownTimeout, &c.ShutdownTimeout, "shutdown_timeout")
}

// loadNotificationConfig applies the notification section from a FileConfig.
func (c *Config) loadNotificationConfig(fc *FileConfig) error {
	if fc.Notification == nil {
		return nil
	}

	nc := c.ensureNotification()
	nc.Enabled = fc.Notification.Enabled

	if fc.Notification.EventQueueDepth > 0 {
		nc.EventQueueDepth = fc.Notification.EventQueueDepth
	}
	if fc.Notification.StorePath != "" {
		nc.StorePath = fc.Notification.StorePath
	}
	if fc.Notification.AlertStorePath != "" {
		nc.AlertStorePath = fc.Notification.AlertStorePath
	}

	// Parse notification-level durations.
	if fc.Notification.RetryInterval != "" {
		d, err := parseDurationExt(fc.Notification.RetryInterval)
		if err != nil {
			return fmt.Errorf("notification.retry_interval: %w", err)
		}
		nc.RetryInterval = d
	}
	if fc.Notification.GCInterval != "" {
		d, err := parseDurationExt(fc.Notification.GCInterval)
		if err != nil {
			return fmt.Errorf("notification.gc_interval: %w", err)
		}
		nc.GCInterval = d
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

	// Load rules.
	for i := range fc.Notification.Rules {
		fr := &fc.Notification.Rules[i]
		rc := RuleConfig{
			Name:             fr.Name,
			MatchSeverity:    fr.MatchSeverity,
			ResourceFilter:   fr.ResourceFilter,
			BodyFilter:       fr.BodyFilter,
			AttributeFilters: fr.AttributeFilters,
			MaxRetries:       fr.MaxRetries,
			Destination:      fr.Destination,
			AlertThreshold:   fr.AlertThreshold,
		}

		// Parse each rule duration field with the shared helper.
		setters := map[string]func(string){
			"cooldown":             func(v string) { rc.Cooldown = v },
			"retry_backoff":        func(v string) { rc.RetryBackoff = v },
			"alert_window":         func(v string) { rc.AlertWindow = v },
			"alert_resolve_window": func(v string) { rc.AlertResolveWindow = v },
		}
		for field, setter := range setters {
			if err := parseRuleDurationField(fr, field, setter); err != nil {
				return err
			}
		}

		nc.Rules = append(nc.Rules, rc)
	}

	return nil
}

// parseOptionalDuration parses s as an extended duration and writes it to *target.
// If s is empty the target is left unchanged. Returns an error on invalid input.
func parseOptionalDuration(s string, target *time.Duration, name string) error {
	if s == "" {
		return nil
	}
	d, err := parseDurationExt(s)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*target = d
	return nil
}

// ruleDurationField maps a logical field name to the getter that reads it from FileRuleConfig.
type ruleDurationField struct {
	name string
	get  func(*FileRuleConfig) string
}

var ruleDurationFields = map[string]ruleDurationField{
	"cooldown":             {"cooldown", func(fr *FileRuleConfig) string { return fr.Cooldown }},
	"retry_backoff":        {"retry_backoff", func(fr *FileRuleConfig) string { return fr.RetryBackoff }},
	"alert_window":         {"alert_window", func(fr *FileRuleConfig) string { return fr.AlertWindow }},
	"alert_resolve_window": {"alert_resolve_window", func(fr *FileRuleConfig) string { return fr.AlertResolveWindow }},
}

// parseRuleDurationField reads a duration string from a FileRuleConfig field,
// parses it, and calls setter with the result. A no-op when the field is empty.
func parseRuleDurationField(fr *FileRuleConfig, field string, setter func(string)) error {
	rf, ok := ruleDurationFields[field]
	if !ok {
		return nil
	}
	s := rf.get(fr)
	if s == "" {
		return nil
	}
	d, err := parseDurationExt(s)
	if err != nil {
		return fmt.Errorf("notification.rules[%s].%s: %w", fr.Name, rf.name, err)
	}
	setter(d.String())
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
