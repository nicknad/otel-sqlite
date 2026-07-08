package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/alerts"

	"go.etcd.io/bbolt"
)

// bboltStore implements Store using bbolt.
type bboltStore struct {
	db *bbolt.DB
}

// bucket names
var (
	bucketNotifState = []byte("notif_state")
	bucketNotifDLQ   = []byte("notif_dlq")
	bucketNotifRetry = []byte("notif_retry") // index: NextRetry:alertID → alertID
)

// maxNotificationStateAge is how long delivery state persists after success.
const maxNotificationStateAge = 24 * time.Hour

// NewBboltStore opens or creates a bbolt-backed Store at the given path.
func NewBboltStore(path string) (Store, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("bbolt open %q: %w", path, err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		for _, name := range [][]byte{bucketNotifState, bucketNotifDLQ, bucketNotifRetry} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("create bucket %q: %w", string(name), err)
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	log.Printf("notifications store: bbolt opened at %q", path)
	return &bboltStore{db: db}, nil
}

// GetNotificationState retrieves state for an alert.
func (s *bboltStore) GetNotificationState(_ context.Context, alertID string) (*NotificationState, error) {
	key := NotificationKey(alertID)
	var state *NotificationState
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNotifState)
		data := b.Get([]byte(key))
		if data == nil {
			return ErrNotFound
		}
		state = &NotificationState{}
		return json.Unmarshal(data, state)
	})
	return state, err
}

// PutNotificationState upserts delivery state.
func (s *bboltStore) PutNotificationState(_ context.Context, alertID string, state *NotificationState) error {
	key := NotificationKey(alertID)
	state.UpdatedAt = time.Now().UnixNano()
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNotifState)
		if err := b.Put([]byte(key), data); err != nil {
			return err
		}
		// Maintain retry index.
		retryB := tx.Bucket(bucketNotifRetry)
		retryKey := retryIndexKey(state.NextRetry, alertID)
		if state.RetryCount > 0 && state.NextRetry > 0 {
			return retryB.Put(retryKey, []byte(alertID))
		}
		_ = retryB.Delete(retryKey)
		_ = deleteRetryEntries(retryB, alertID)
		return nil
	})
}

// DeleteNotificationState removes delivery state.
func (s *bboltStore) DeleteNotificationState(_ context.Context, alertID string) error {
	key := NotificationKey(alertID)
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNotifState)
		if err := b.Delete([]byte(key)); err != nil {
			return err
		}
		return deleteRetryEntries(tx.Bucket(bucketNotifRetry), alertID)
	})
}

// EnqueueDLQ moves an alert to the dead-letter queue.
func (s *bboltStore) EnqueueDLQ(_ context.Context, alert *alerts.Alert, reason string) error {
	entry := &DLQEntry{
		ID:         newDLQID(),
		Alert:      alert,
		FailReason: reason,
		FailedAt:   time.Now().UnixNano(),
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal dlq entry: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNotifDLQ)
		return b.Put([]byte(entry.ID), data)
	})
}

// ListDLQ returns all entries in the dead-letter queue.
func (s *bboltStore) ListDLQ(_ context.Context) ([]*DLQEntry, error) {
	var entries []*DLQEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNotifDLQ)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry DLQEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				return fmt.Errorf("unmarshal dlq entry %q: %w", string(k), err)
			}
			entries = append(entries, &entry)
		}
		return nil
	})
	return entries, err
}

// AckDLQ removes a DLQ entry.
func (s *bboltStore) AckDLQ(_ context.Context, id string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNotifDLQ)
		return b.Delete([]byte(id))
	})
}

// ScanRetryable finds alerts due for retry.
func (s *bboltStore) ScanRetryable(_ context.Context) ([]RetryableEntry, error) {
	now := time.Now().UnixNano()
	var entries []RetryableEntry

	err := s.db.View(func(tx *bbolt.Tx) error {
		retryB := tx.Bucket(bucketNotifRetry)
		c := retryB.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			ts := parseRetryTimestamp(k)
			if ts > now {
				break
			}
			alertID := string(v)
			stateB := tx.Bucket(bucketNotifState)
			data := stateB.Get([]byte(NotificationKey(alertID)))
			if data == nil {
				continue
			}
			var state NotificationState
			if err := json.Unmarshal(data, &state); err != nil {
				continue
			}
			if state.RetryCount > 0 && state.NextRetry > 0 && state.NextRetry <= now {
				entries = append(entries, RetryableEntry{
					AlertID:    alertID,
					RetryCount: state.RetryCount,
				})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	go s.gcOldState()
	return entries, nil
}

// gcOldState removes successful delivery state entries older than the cutoff.
func (s *bboltStore) gcOldState() {
	cutoff := time.Now().Add(-maxNotificationStateAge).UnixNano()
	_ = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNotifState)
		retryB := tx.Bucket(bucketNotifRetry)
		c := b.Cursor()
		var toDelete []string
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var state NotificationState
			if err := json.Unmarshal(v, &state); err != nil {
				continue
			}
			if state.RetryCount == 0 && state.NextRetry == 0 && state.LastSuccess > 0 && state.LastSuccess < cutoff {
				toDelete = append(toDelete, string(k))
			}
		}
		for _, key := range toDelete {
			_ = b.Delete([]byte(key))
			_ = deleteRetryEntries(retryB, DecodeNotificationKey(key))
		}
		if len(toDelete) > 0 {
			log.Printf("notifications store: gc removed %d stale state entries", len(toDelete))
		}
		return nil
	})
}

// retryIndexKey formats a key for the retry bucket.
func retryIndexKey(nextRetry int64, alertID string) []byte {
	return fmt.Appendf(nil, "%020d:%s", nextRetry, alertID)
}

// parseRetryTimestamp extracts the nextRetry timestamp from a retry bucket key.
func parseRetryTimestamp(key []byte) int64 {
	var ts int64
	for i, b := range key {
		if b == ':' {
			_, _ = fmt.Sscanf(string(key[:i]), "%d", &ts)
			return ts
		}
	}
	return 0
}

// deleteRetryEntries removes all retry index entries that point to the given alert ID.
func deleteRetryEntries(b *bbolt.Bucket, alertID string) error {
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if string(v) == alertID {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close closes the bbolt database.
func (s *bboltStore) Close() error {
	log.Println("notifications store: closing bbolt")
	return s.db.Close()
}

// newDLQID generates a unique ID for a DLQ entry.
func newDLQID() string {
	return fmt.Sprintf("dlq_%d", time.Now().UnixNano())
}
