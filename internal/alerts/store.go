package alerts

import "context"

// AlertStore is the persistence layer for Alert objects.
type AlertStore interface {
	// Get retrieves an alert by its ID. Returns nil if not found.
	Get(ctx context.Context, id string) (*Alert, error)

	// Put upserts an alert.
	Put(ctx context.Context, alert *Alert) error

	// Delete removes an alert.
	Delete(ctx context.Context, id string) error

	// ListByStatus returns all alerts in the given state.
	ListByStatus(ctx context.Context, status AlertStatus) ([]*Alert, error)

	// ListAll returns all alerts.
	ListAll(ctx context.Context) ([]*Alert, error)

	// Close cleans up store resources.
	Close() error
}
