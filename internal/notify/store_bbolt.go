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
)

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
		if _, err := tx.CreateBucketIfNotExists(bucketState); err != nil {
			return fmt.Errorf("create state bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists(bucketDLQ); err != nil {
			return fmt.Errorf("create dlq bucket: %w", err)
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

// PutState upserts state for a notification key.
func (s *bboltStore) PutState(_ context.Context, key string, state *NotificationState) error {
	state.UpdatedAt = time.Now().UnixNano()
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketState)
		return b.Put([]byte(key), data)
	})
}

// DeleteState removes state for a notification key.
func (s *bboltStore) DeleteState(_ context.Context, key string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketState)
		return b.Delete([]byte(key))
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

// ScanRetryable iterates all state entries and returns those with a past-due NextRetry.
func (s *bboltStore) ScanRetryable(_ context.Context) ([]RetryableEntry, error) {
	now := time.Now().UnixNano()
	var entries []RetryableEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketState)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var state NotificationState
			if err := json.Unmarshal(v, &state); err != nil {
				continue // skip corrupt entries
			}
			if state.RetryCount > 0 && state.NextRetry > 0 && state.NextRetry <= now {
				entries = append(entries, RetryableEntry{
					Key:        string(k),
					RetryCount: state.RetryCount,
				})
			}
		}
		return nil
	})
	return entries, err
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
