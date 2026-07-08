package notifications

import (
	"context"
	"fmt"

	"codeberg.org/nicknad/otel-sqlite/internal/alerts"
)

// Notifier sends an alert notification to an external system.
// Implementations must be safe for concurrent use.
type Notifier interface {
	// Send delivers the alert. Returns nil on success, error on failure.
	// The caller decides retry logic.
	Send(ctx context.Context, alert *alerts.Alert) error

	// Name returns a unique name for this notifier (matches Rule.Destination).
	Name() string

	// Close cleans up notifier resources.
	Close() error
}

// NewNotifier constructs a notifier by name. Supported names:
//
//	"log"  — writes to log.Printf (development/testing)
//	"http" — POSTs JSON to a configurable URL
//
// The config parameter is notifier-specific. For HTTP, it should be an *HTTPNotifierConfig.
func NewNotifier(name string, config any) (Notifier, error) {
	switch name {
	case "log":
		return NewLogNotifier(), nil
	case "http":
		cfg, ok := config.(*HTTPNotifierConfig)
		if !ok {
			return nil, fmt.Errorf("notifications: http notifier requires *HTTPNotifierConfig, got %T", config)
		}
		return NewHTTPNotifier(cfg), nil
	default:
		return nil, fmt.Errorf("notifications: unknown notifier %q", name)
	}
}
