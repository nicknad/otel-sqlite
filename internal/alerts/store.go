package alerts

import (
	"context"
	"time"
)

// AlertStore is the persistence layer for Alert objects.
type AlertStore interface {
	// Get retrieves an alert by its ID. Returns nil if not found.
	Get(ctx context.Context, id string) (*Alert, error)

	// Put upserts an alert.
	Put(ctx context.Context, alert *Alert) error

	// Delete removes an alert and its notification state (see also DeleteWithState).
	Delete(ctx context.Context, id string) error

	// DeleteWithState removes an alert along with its notification delivery
	// state. The notifStore argument provides state cleanup; nil is safe.
	DeleteWithState(ctx context.Context, id string, notifStore AlertNotifStore) error

	// ListByStatus returns all alerts in the given state.
	ListByStatus(ctx context.Context, status AlertStatus) ([]*Alert, error)

	// ListAll returns all alerts.
	ListAll(ctx context.Context) ([]*Alert, error)

	// DeleteIdleAlerts removes Pending and Firing alerts whose LastMatched
	// (falling back to UpdatedAt) is older than idleTTL. Deleted alerts also
	// have their notification delivery state cleaned up via notifStore.
	// Returns the number of alerts deleted.
	DeleteIdleAlerts(ctx context.Context, idleTTL time.Duration, notifStore AlertNotifStore) (int, error)

	// Compact rewrites the bbolt database into a smaller file after bulk
	// deletions. Must be safe to call while the store is serving reads/writes.
	Compact(ctx context.Context) error

	// Close cleans up store resources.
	Close() error
}

// AlertNotifStore is the subset of notification Store needed by AlertStore
// for coordinated deletions.
type AlertNotifStore interface {
	DeleteNotificationState(ctx context.Context, alertID string) error
}
