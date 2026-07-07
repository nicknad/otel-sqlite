package notify

import (
	"context"
	"log"
)

// LogNotifier writes notifications to the application log.
// Useful for development and testing.
type LogNotifier struct{}

// NewLogNotifier creates a new LogNotifier.
func NewLogNotifier() *LogNotifier {
	return &LogNotifier{}
}

// Send logs the event via log.Printf.
func (n *LogNotifier) Send(_ context.Context, event *Event) error {
	log.Printf("[NOTIFY] severity=%s resource=%s body=%q", event.Severity.String(), event.ResourceID, event.Body)
	return nil
}

// Name returns "log".
func (n *LogNotifier) Name() string {
	return "log"
}

// Close is a no-op for LogNotifier.
func (n *LogNotifier) Close() error {
	return nil
}
