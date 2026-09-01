// Package alerts provides alert state tracking, time-window aggregation,
// threshold counting, and a state machine for the notification pipeline.
package alerts

import (
	"fmt"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// AlertStatus represents the lifecycle phase of an alert.
type AlertStatus int

const (
	AlertPending  AlertStatus = iota // below threshold, not yet firing
	AlertFiring                      // threshold exceeded, actively firing
	AlertResolved                    // was firing, now back below threshold
)

// String returns a human-readable name for the alert status.
func (s AlertStatus) String() string {
	switch s {
	case AlertPending:
		return "pending"
	case AlertFiring:
		return "firing"
	case AlertResolved:
		return "resolved"
	default:
		return "unknown"
	}
}

// Alert is the persistent alert object representing a rule-resource pair that
// has crossed (or is approaching) a notification threshold.
type Alert struct {
	ID          string         `json:"id"`
	RuleID      string         `json:"rule_id"`
	ResourceID  string         `json:"resource_id"`
	Status      AlertStatus    `json:"status"`
	Severity    model.Severity `json:"severity"`
	OpenedAt    int64          `json:"opened_at"`    // unix nanos — first pending
	UpdatedAt   int64          `json:"updated_at"`   // unix nanos — last state change
	LastMatched int64          `json:"last_matched"` // unix nanos — most recent event
	Count       int            `json:"count"`        // events matched in current window

	// WindowTimestamps stores raw timestamps for window persistence.
	// The sliding window is reconstructed from this on load.
	WindowTimestamps []int64 `json:"window_ts,omitempty"`
}

// AlertID constructs the stable composite key for an alert.
func AlertID(ruleID, resourceID string) string {
	return fmt.Sprintf("%s:%s", ruleID, resourceID)
}

// NewAlert creates a new Alert in Pending state.
func NewAlert(ruleID, resourceID string, severity model.Severity, now int64) *Alert {
	return &Alert{
		ID:          AlertID(ruleID, resourceID),
		RuleID:      ruleID,
		ResourceID:  resourceID,
		Status:      AlertPending,
		Severity:    severity,
		OpenedAt:    now,
		UpdatedAt:   now,
		LastMatched: now,
		Count:       1,
	}
}
