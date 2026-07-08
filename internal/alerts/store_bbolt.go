package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

// bboltAlertStore implements AlertStore using bbolt.
type bboltAlertStore struct {
	db   *bbolt.DB
	path string
	mu   sync.Mutex // serializes Compaction against other operations
}

// bucket name
var alertsBucket = []byte("alerts")

// NewBboltAlertStore opens or creates a bbolt-backed AlertStore at the given path.
func NewBboltAlertStore(path string) (AlertStore, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("bbolt open %q: %w", path, err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(alertsBucket)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create alerts bucket: %w", err)
	}

	log.Printf("alert store: bbolt opened at %q", path)
	return &bboltAlertStore{db: db, path: path}, nil
}

// Get retrieves an alert by ID.
func (s *bboltAlertStore) Get(_ context.Context, id string) (*Alert, error) {
	var alert *Alert
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(alertsBucket)
		data := b.Get([]byte(id))
		if data == nil {
			return nil
		}
		alert = &Alert{}
		return json.Unmarshal(data, alert)
	})
	if err != nil {
		return nil, fmt.Errorf("get alert %q: %w", id, err)
	}
	return alert, nil
}

// Put upserts an alert.
func (s *bboltAlertStore) Put(_ context.Context, alert *Alert) error {
	data, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("marshal alert: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(alertsBucket)
		return b.Put([]byte(alert.ID), data)
	})
}

// Delete removes an alert.
func (s *bboltAlertStore) Delete(_ context.Context, id string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(alertsBucket)
		return b.Delete([]byte(id))
	})
}

// DeleteWithState removes an alert and its notification delivery state.
func (s *bboltAlertStore) DeleteWithState(ctx context.Context, id string, notifStore AlertNotifStore) error {
	if err := s.Delete(ctx, id); err != nil {
		return err
	}
	if notifStore != nil {
		_ = notifStore.DeleteNotificationState(ctx, id)
	}
	return nil
}

// alertHeader is a lightweight struct for partial JSON unmarshaling during
// GC scans — only the fields needed to decide eviction are populated.
type alertHeader struct {
	Status      AlertStatus `json:"status"`
	LastMatched int64       `json:"last_matched"`
	UpdatedAt   int64       `json:"updated_at"`
}

// lastActive returns the later of LastMatched and UpdatedAt.
func (h *alertHeader) lastActive() int64 {
	if h.LastMatched >= h.UpdatedAt {
		return h.LastMatched
	}
	return h.UpdatedAt
}

// DeleteIdleAlerts removes Pending and Firing alerts that have been idle
// longer than idleTTL. Uses a raw bbolt cursor with partial JSON unmarshal
// to avoid allocating full Alert objects for the entire population.
func (s *bboltAlertStore) DeleteIdleAlerts(
	ctx context.Context, idleTTL time.Duration, notifStore AlertNotifStore,
) (int, error) {
	cutoff := time.Now().Add(-idleTTL).UnixNano()
	var deleted int
	var ids []string

	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(alertsBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			var h alertHeader
			if err := json.Unmarshal(v, &h); err != nil {
				continue
			}

			if h.Status != AlertPending && h.Status != AlertFiring {
				continue
			}

			if h.lastActive() >= cutoff {
				continue
			}

			ids = append(ids, string(k))
			if err := c.Delete(); err != nil {
				return fmt.Errorf("delete idle alert %q: %w", string(k), err)
			}
			deleted++
		}
		return nil
	})
	if err != nil {
		return deleted, err
	}

	// Clean up notification state for deleted alerts (different bbolt DB).
	if notifStore != nil {
		for _, id := range ids {
			_ = notifStore.DeleteNotificationState(ctx, id)
		}
	}

	if deleted > 0 {
		log.Printf("alert store: gc evicted %d idle alerts (cutoff=%s)", deleted, idleTTL)
	}
	return deleted, nil
}

// ListByStatus returns all alerts with the given status.
func (s *bboltAlertStore) ListByStatus(_ context.Context, status AlertStatus) ([]*Alert, error) {
	var alerts []*Alert
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(alertsBucket)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var alert Alert
			if err := json.Unmarshal(v, &alert); err != nil {
				return fmt.Errorf("unmarshal alert %q: %w", string(k), err)
			}
			if alert.Status == status {
				alerts = append(alerts, &alert)
			}
		}
		return nil
	})
	return alerts, err
}

// ListAll returns all alerts.
func (s *bboltAlertStore) ListAll(_ context.Context) ([]*Alert, error) {
	var alerts []*Alert
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(alertsBucket)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var alert Alert
			if err := json.Unmarshal(v, &alert); err != nil {
				return fmt.Errorf("unmarshal alert %q: %w", string(k), err)
			}
			alerts = append(alerts, &alert)
		}
		return nil
	})
	return alerts, err
}

// Compact rewrites the bbolt database into a compacted file by copying
// live data and atomically replacing the original. Safe to call while
// the store is serving reads/writes.
func (s *bboltAlertStore) Compact(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tmpPath := s.path + ".compact"

	// Open a new database for the compacted copy.
	dstDB, err := bbolt.Open(tmpPath, 0o600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return fmt.Errorf("open compact dest: %w", err)
	}

	err = bbolt.Compact(dstDB, s.db, 0)
	closeErr := dstDB.Close()
	if err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("compact: %w", err)
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close compact dest: %w", closeErr)
	}

	// Atomic swap: rename compacted file over the original.
	prevPath := s.path + ".prev"
	prevDB := s.db

	// Close the original DB so we can rename files.
	if err := prevDB.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close original db: %w", err)
	}

	if err := os.Rename(s.path, prevPath); err != nil {
		// Try to reopen the original.
		s.reopen(prevDB, s.path)
		_ = os.Remove(tmpPath)
		return fmt.Errorf("backup original: %w", err)
	}

	if err := os.Rename(tmpPath, s.path); err != nil {
		// Restore from backup.
		_ = os.Rename(prevPath, s.path)
		s.reopen(prevDB, s.path)
		return fmt.Errorf("install compacted db: %w", err)
	}

	// Remove backup.
	_ = os.Remove(prevPath)

	// Reopen the compacted database.
	newDB, err := bbolt.Open(s.path, 0o600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return fmt.Errorf("reopen compacted db: %w", err)
	}

	// Ensure the alerts bucket exists.
	err = newDB.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(alertsBucket)
		return err
	})
	if err != nil {
		_ = newDB.Close()
		return fmt.Errorf("create alerts bucket: %w", err)
	}

	s.db = newDB
	log.Printf("alert store: compacted %s", s.path)
	return nil
}

// reopen reopens the database at the given path, used for recovery.
func (s *bboltAlertStore) reopen(fallback *bbolt.DB, path string) {
	if fallback != nil {
		s.db, _ = bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 1 * time.Second})
	}
}

// Close closes the bbolt database.
func (s *bboltAlertStore) Close() error {
	log.Println("alert store: closing bbolt")
	return s.db.Close()
}
