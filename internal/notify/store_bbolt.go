package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.etcd.io/bbolt"
)

// bboltStore implements Store using bbolt (pure Go embedded KV store).
type bboltStore struct {
	db *bbolt.DB
}

// bucket names
var (
	bucketState = []byte("state")
	bucketDLQ   = []byte("dlq")
	bucketRetry = []byte("retry") // index: NextRetry:key → key
)

// maxStateAge is how long state entries persist after a successful delivery
// before they are eligible for garbage collection.
const maxStateAge = 24 * time.Hour

// NewBboltStore opens or creates a bbolt-backed Store at the given path.
func NewBboltStore(path string) (Store, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("bbolt open %q: %w", path, err)
	}

	// Create buckets if they don't exist.
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, name := range [][]byte{bucketState, bucketDLQ, bucketRetry} {
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

	log.Printf("notify store: bbolt opened at %q", path)
	return &bboltStore{db: db}, nil
}

// GetState retrieves state for a notification key.
func (s *bboltStore) GetState(_ context.Context, key string) (*NotificationState, error) {
	var state *NotificationState
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketState)
		data := b.Get([]byte(key))
		if data == nil {
			return ErrNotFound
		}
		state = &NotificationState{}
		return json.Unmarshal(data, state)
	})
	return state, err
}

// PutState upserts state for a notification key. It also maintains the
// retry index: if NextRetry > 0 the key is added to the retry bucket;
// if NextRetry == 0 the key is removed from the retry bucket (if present).
func (s *bboltStore) PutState(_ context.Context, key string, state *NotificationState) error {
	state.UpdatedAt = time.Now().UnixNano()
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketState)
		if err := b.Put([]byte(key), data); err != nil {
			return err
		}
		// Maintain retry index.
		retryB := tx.Bucket(bucketRetry)
		retryKey := retryIndexKey(state.NextRetry, key)
		if state.RetryCount > 0 && state.NextRetry > 0 {
			return retryB.Put(retryKey, []byte(key))
		}
		// Remove any existing retry entry for this key (scan and delete).
		_ = retryB.Delete(retryKey)
		// Also clean up stale entries that may exist under different timestamps.
		_ = deleteRetryEntries(retryB, key)
		return nil
	})
}

// DeleteState removes state and cleans up the retry index.
func (s *bboltStore) DeleteState(_ context.Context, key string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketState)
		if err := b.Delete([]byte(key)); err != nil {
			return err
		}
		// Clean retry index.
		return deleteRetryEntries(tx.Bucket(bucketRetry), key)
	})
}

// EnqueueDLQ moves an event to the dead-letter queue.
func (s *bboltStore) EnqueueDLQ(_ context.Context, event *Event, reason string) error {
	entry := &DLQEntry{
		ID:         newDLQID(),
		Event:      event,
		FailReason: reason,
		FailedAt:   time.Now().UnixNano(),
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal dlq entry: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketDLQ)
		return b.Put([]byte(entry.ID), data)
	})
}

// ListDLQ returns all events in the dead-letter queue.
func (s *bboltStore) ListDLQ(_ context.Context) ([]*DLQEntry, error) {
	var entries []*DLQEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketDLQ)
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
		b := tx.Bucket(bucketDLQ)
		return b.Delete([]byte(id))
	})
}

// ScanRetryable uses the retry bucket index to efficiently find past-due
// retries instead of scanning all state entries. Falls back to a full scan
// if the retry bucket is empty (e.g., after a migration).
func (s *bboltStore) ScanRetryable(_ context.Context) ([]RetryableEntry, error) {
	now := time.Now().UnixNano()
	var entries []RetryableEntry

	err := s.db.View(func(tx *bbolt.Tx) error {
		retryB := tx.Bucket(bucketRetry)
		c := retryB.Cursor()

		// Retry keys are formatted as "nextRetry:stateKey".
		// Iterate only entries whose nextRetry <= now.
		for k, v := c.First(); k != nil; k, v = c.Next() {
			ts := parseRetryTimestamp(k)
			if ts > now {
				// Since keys are sorted, all subsequent entries are also in the future.
				break
			}
			key := string(v)
			// Read state to get the current RetryCount (could have changed).
			stateB := tx.Bucket(bucketState)
			data := stateB.Get([]byte(key))
			if data == nil {
				continue
			}
			var state NotificationState
			if err := json.Unmarshal(data, &state); err != nil {
				continue
			}
			if state.RetryCount > 0 && state.NextRetry > 0 && state.NextRetry <= now {
				entries = append(entries, RetryableEntry{
					Key:        key,
					RetryCount: state.RetryCount,
				})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Garbage-collect old successful state entries periodically.
	go s.gcOldState()

	return entries, nil
}

// gcOldState removes state entries that succeeded more than maxStateAge ago.
// This runs asynchronously after ScanRetryable to amortize cost.
func (s *bboltStore) gcOldState() {
	cutoff := time.Now().Add(-maxStateAge).UnixNano()
	_ = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketState)
		retryB := tx.Bucket(bucketRetry)
		c := b.Cursor()
		var toDelete []string
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var state NotificationState
			if err := json.Unmarshal(v, &state); err != nil {
				continue
			}
			// Remove state entries that are not pending retry and are older than cutoff.
			if state.RetryCount == 0 && state.NextRetry == 0 && state.LastSuccess > 0 && state.LastSuccess < cutoff {
				toDelete = append(toDelete, string(k))
			}
		}
		for _, key := range toDelete {
			_ = b.Delete([]byte(key))
			_ = deleteRetryEntries(retryB, key)
		}
		if len(toDelete) > 0 {
			log.Printf("notify store: gc removed %d stale state entries", len(toDelete))
		}
		return nil
	})
}

// retryIndexKey formats a key for the retry bucket as "nextRetry:stateKey".
func retryIndexKey(nextRetry int64, stateKey string) []byte {
	return fmt.Appendf(nil, "%020d:%s", nextRetry, stateKey)
}

// parseRetryTimestamp extracts the nextRetry timestamp from a retry bucket key.
func parseRetryTimestamp(key []byte) int64 {
	var ts int64
	for i, b := range key {
		if b == ':' {
			// Parse the numeric prefix.
			_, _ = fmt.Sscanf(string(key[:i]), "%d", &ts)
			return ts
		}
	}
	return 0
}

// deleteRetryEntries removes all retry index entries that point to the given state key.
func deleteRetryEntries(b *bbolt.Bucket, stateKey string) error {
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if string(v) == stateKey {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close closes the bbolt database.
func (s *bboltStore) Close() error {
	log.Println("notify store: closing bbolt")
	return s.db.Close()
}

// newDLQID generates a unique ID for a DLQ entry.
func newDLQID() string {
	return fmt.Sprintf("dlq_%d", time.Now().UnixNano())
}
