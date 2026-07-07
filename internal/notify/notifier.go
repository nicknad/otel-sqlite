package notify

import (
	"context"
	"fmt"
)

// Notifier sends a notification to an external system.
// Implementations must be safe for concurrent use.
type Notifier interface {
	// Send delivers the event. Returns nil on success, error on failure.
	// The caller decides retry logic.
	Send(ctx context.Context, event *Event) error

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
			return nil, fmt.Errorf("notify: http notifier requires *HTTPNotifierConfig, got %T", config)
		}
		return NewHTTPNotifier(cfg), nil
	default:
		return nil, fmt.Errorf("notify: unknown notifier %q", name)
	}
}
