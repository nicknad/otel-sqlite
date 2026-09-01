package notifications

import (
	"context"
	"log"

	"codeberg.org/nicknad/otel-sqlite/internal/alerts"
)

// LogNotifier writes alert notifications to the application log.
// Useful for development and testing.
type LogNotifier struct{}

// NewLogNotifier creates a new LogNotifier.
func NewLogNotifier() *LogNotifier {
	return &LogNotifier{}
}

// Send logs the alert via log.Printf.
func (n *LogNotifier) Send(_ context.Context, alert *alerts.Alert) error {
	log.Printf("[NOTIFY] alert=%s rule=%s resource=%s status=%s severity=%s count=%d",
		alert.ID, alert.RuleID, alert.ResourceID, alert.Status.String(),
		alert.Severity.String(), alert.Count)
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
