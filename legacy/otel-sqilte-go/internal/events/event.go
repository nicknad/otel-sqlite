// Package events defines the event type that flows through the alerting pipeline.
package events

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// Event is the notification payload passed from the batcher into the
// alerting / notification pipeline.
type Event struct {
	Severity     model.Severity
	SeverityText string
	Body         string
	ResourceID   string
	Resource     *model.Resource
	Timestamp    int64
	TraceID      [16]byte
	SpanID       [8]byte
	Attributes   []model.Attribute
	ScopeName    string
	ScopeVersion string

	// cachedFingerprint is lazily computed by Fingerprint.
	cachedFingerprint string
}

// Fingerprint returns a stable hash of the event body + sorted attributes.
// The result is cached after first computation.
func (e *Event) Fingerprint() string {
	if e.cachedFingerprint != "" {
		return e.cachedFingerprint
	}
	e.cachedFingerprint = eventFingerprint(e)
	return e.cachedFingerprint
}

// EventFromLogRecord creates an Event from a model.LogRecord.
func EventFromLogRecord(record *model.LogRecord) *Event {
	return &Event{
		Severity:     record.SeverityNumber,
		SeverityText: record.SeverityText,
		Body:         record.Body,
		ResourceID:   record.ResourceID,
		Resource:     record.Resource,
		Timestamp:    record.Timestamp,
		TraceID:      record.TraceID,
		SpanID:       record.SpanID,
		Attributes:   record.Attributes,
		ScopeName:    record.ScopeName,
		ScopeVersion: record.ScopeVersion,
	}
}

// eventFingerprint returns a short hash of event body + sorted attributes.
func eventFingerprint(event *Event) string {
	var sb strings.Builder
	sb.WriteString(event.Body)

	// Sort attributes by key for deterministic hashing.
	type kv struct{ k, v string }
	var pairs []kv
	for _, attr := range event.Attributes {
		pairs = append(pairs, kv{attr.Key, attrValueString(&attr)})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].k < pairs[j].k })
	for _, p := range pairs {
		sb.WriteString(p.k)
		sb.WriteString("=")
		sb.WriteString(p.v)
		sb.WriteString(";")
	}

	hash := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(hash[:8])
}

// attrValueString returns the string representation of an attribute value.
func attrValueString(attr *model.Attribute) string {
	switch attr.Kind {
	case model.ValueString:
		return attr.Str
	case model.ValueInt:
		return strconv.FormatInt(attr.Num, 10)
	case model.ValueDouble:
		return fmt.Sprintf("%f", attr.Dbl)
	case model.ValueBool:
		return strconv.FormatBool(attr.Flag)
	default:
		return ""
	}
}
