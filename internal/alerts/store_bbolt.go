package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.etcd.io/bbolt"
)

// bboltAlertStore implements AlertStore using bbolt.
type bboltAlertStore struct {
	db *bbolt.DB
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
	return &bboltAlertStore{db: db}, nil
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

// Close closes the bbolt database.
func (s *bboltAlertStore) Close() error {
	log.Println("alert store: closing bbolt")
	return s.db.Close()
}
