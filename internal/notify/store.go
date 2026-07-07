package notify

import (
	"context"
	"fmt"
	"strings"
)

// Store is the persistence layer for notification state.
// Implementations can use bbolt, RocksDB, Badger, or even SQLite.
type Store interface {
	// GetState retrieves state for a notification key.
	// Returns ErrNotFound if absent.
	GetState(ctx context.Context, key string) (*NotificationState, error)

	// PutState upserts state for a notification key.
	PutState(ctx context.Context, key string, state *NotificationState) error

	// DeleteState removes state (after successful delivery or manual ack).
	DeleteState(ctx context.Context, key string) error

	// EnqueueDLQ moves an event to the dead-letter queue.
	EnqueueDLQ(ctx context.Context, event *Event, reason string) error

	// ListDLQ returns all events currently in the dead-letter queue.
	ListDLQ(ctx context.Context) ([]*DLQEntry, error)

	// AckDLQ removes a DLQ entry after manual intervention or replay.
	AckDLQ(ctx context.Context, id string) error

	// ScanRetryable returns all state keys where NextRetry is in the past
	// and RetryCount > 0 (i.e., events that need retry).
	ScanRetryable(ctx context.Context) ([]RetryableEntry, error)

	// Close cleans up store resources.
	Close() error
}

// RetryableEntry pairs a state key with its retry count for scanning.
type RetryableEntry struct {
	Key        string
	RetryCount int
}

// NotificationState tracks the delivery state for a notification key.
type NotificationState struct {
	RetryCount      int
	LastAttempt     int64 // unix nanos
	LastSuccess     int64 // unix nanos
	NextRetry       int64 // unix nanos
	CooldownUntil   int64 // unix nanos
	ErrorRateBucket []int64
	EventDigest     string
	DeadLettered    bool
	UpdatedAt       int64 // unix nanos

	// StoredEvent is the original event preserved for retries.
	// Set on first delivery; used by the retry loop to reconstruct
	// the full event instead of sending a synthetic key-body.
	StoredEvent *Event `json:",omitempty"`
}

// DLQEntry represents an event in the dead-letter queue.
type DLQEntry struct {
	ID          string
	Event       *Event
	FailReason  string
	RetryCount  int
	FailedAt    int64 // unix nanos
	OriginalKey string
}

// StateKey is a structured key for notification state. It encodes the
// rule name, resource ID, and event fingerprint.
type StateKey struct {
	RuleName    string
	ResourceID  string
	Fingerprint string
}

// Encode returns the string representation of the state key.
func (k StateKey) Encode() string {
	return fmt.Sprintf("%s:%s:%s", k.RuleName, k.ResourceID, k.Fingerprint)
}

// DecodeStateKey parses an encoded state key back into its components.
func DecodeStateKey(key string) StateKey {
	first := strings.IndexByte(key, ':')
	if first < 0 {
		return StateKey{}
	}
	second := strings.IndexByte(key[first+1:], ':')
	if second < 0 {
		return StateKey{}
	}
	return StateKey{
		RuleName:    key[:first],
		ResourceID:  key[first+1 : first+1+second],
		Fingerprint: key[first+1+second+1:],
	}
}
