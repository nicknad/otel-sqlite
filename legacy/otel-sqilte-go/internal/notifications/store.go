package notifications

import (
	"context"
	"strings"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/alerts"
)

// Store is the persistence layer for notification delivery state and DLQ.
type Store interface {
	// GetNotificationState retrieves delivery state for an alert.
	// Returns ErrNotFound if absent.
	GetNotificationState(ctx context.Context, alertID string) (*NotificationState, error)

	// PutNotificationState upserts delivery state for an alert.
	PutNotificationState(ctx context.Context, alertID string, state *NotificationState) error

	// DeleteNotificationState removes delivery state.
	DeleteNotificationState(ctx context.Context, alertID string) error

	// EnqueueDLQ moves an alert to the dead-letter queue.
	EnqueueDLQ(ctx context.Context, alert *alerts.Alert, reason string) error

	// ListDLQ returns all alerts currently in the dead-letter queue.
	ListDLQ(ctx context.Context) ([]*DLQEntry, error)

	// AckDLQ removes a DLQ entry after manual intervention or replay.
	AckDLQ(ctx context.Context, id string) error

	// ScanRetryable returns all alert IDs where NextRetry is in the past.
	ScanRetryable(ctx context.Context) ([]RetryableEntry, error)

	// Close cleans up store resources.
	Close() error

	// PurgeDLQ removes DLQ entries older than maxAge. Returns the number of entries deleted.
	PurgeDLQ(ctx context.Context, maxAge time.Duration) (int, error)

	// Compact rewrites the bbolt database into a smaller file after bulk
	// deletions. Safe to call while the store is serving reads/writes.
	Compact(ctx context.Context) error
}

// RetryableEntry pairs an alert ID with its retry count for scanning.
type RetryableEntry struct {
	AlertID    string
	RetryCount int
}

// NotificationState tracks the delivery state for an alert.
type NotificationState struct {
	RetryCount    int
	LastAttempt   int64 // unix nanos
	LastSuccess   int64 // unix nanos
	NextRetry     int64 // unix nanos
	CooldownUntil int64 // unix nanos
	DeadLettered  bool
	UpdatedAt     int64 // unix nanos
}

// DLQEntry represents an alert in the dead-letter queue.
type DLQEntry struct {
	ID         string
	Alert      *alerts.Alert
	FailReason string
	RetryCount int
	FailedAt   int64 // unix nanos
}

// NotificationKey builds a state key from an alert ID.
func NotificationKey(alertID string) string {
	return "notif:" + alertID
}

// DecodeNotificationKey extracts the alert ID from a notification state key.
func DecodeNotificationKey(key string) string {
	if strings.HasPrefix(key, "notif:") {
		return key[len("notif:"):]
	}
	return key
}
