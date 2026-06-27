// Package otlp provides mapping from OTLP protobuf types to internal domain models.
// This package isolates OTLP protobuf dependencies to the transport layer.
// No other package should import OTLP protobuf types directly.
package otlp

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/nnadolski/otel-sqlite/internal/model"

	// OTLP protobuf types — must not leak beyond this package.
	commonV1 "github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	logsV1 "github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1"
	resourceV1 "github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1"
)

// Mapper converts OTLP protobuf types to internal domain models.
type Mapper struct{}

// NewMapper creates a new Mapper instance.
func NewMapper() *Mapper {
	return &Mapper{}
}

// MapLogsData converts OTLP ResourceLogs to internal LogBatch slices.
// Returns a slice of LogBatch, one for each ResourceLogs in the input.
func (m *Mapper) MapLogsData(resourceLogs []*logsV1.ResourceLogs) []*model.LogBatch {
	if len(resourceLogs) == 0 {
		return nil
	}

	batches := make([]*model.LogBatch, 0, len(resourceLogs))

	for _, rl := range resourceLogs {
		batch := m.mapResourceLogs(rl)
		if batch != nil && !batch.IsEmpty() {
			batches = append(batches, batch)
		}
	}

	return batches
}

// mapResourceLogs converts a single ResourceLogs to a LogBatch.
func (m *Mapper) mapResourceLogs(resourceLogs *logsV1.ResourceLogs) *model.LogBatch {
	if resourceLogs == nil {
		return nil
	}

	batch := model.NewLogBatch(len(resourceLogs.ScopeLogs))

	// Map resource (with a stable ID so identical resources dedup in storage)
	resource := m.mapResource(resourceLogs.Resource, resourceLogs.SchemaUrl)
	batch.Resource = resource
	batch.SchemaURL = resourceLogs.SchemaUrl

	// Map scope logs
	for _, scopeLogs := range resourceLogs.ScopeLogs {
		m.mapScopeLogs(scopeLogs, batch, resource)
	}

	return batch
}

// mapResource converts OTLP Resource to an internal Resource.
// The returned Resource has a deterministic ID derived from the identifying
// attributes (service.name, host.name) plus the schema URL so identical
// resources deduplicate in the writer (via INSERT OR IGNORE) instead of
// producing a new row per batch. schemaURL is sourced from the enclosing
// ResourceLogs message (the OTLP Resource message itself has none).
func (m *Mapper) mapResource(resource *resourceV1.Resource, schemaURL string) *model.Resource {
	if resource == nil {
		return model.NewResource(nil)
	}

	attributes := make(map[string]model.AttributeValue, len(resource.Attributes))
	for _, kv := range resource.Attributes {
		attributes[kv.Key] = mapAnyValue(kv.Value)
	}

	r := model.NewResource(attributes)
	r.SchemaURL = schemaURL
	r.ID = computeResourceID(attributes, schemaURL)
	return r
}

// computeResourceID returns a stable identifier for a resource based on the
// subset of attributes that identify it (service.name, host.name) plus the
// schema URL. Other attributes (e.g. version) are intentionally excluded so
// that logically-equal resources collapse to a single row.
func computeResourceID(attrs map[string]model.AttributeValue, schemaURL string) string {
	var idParts [3]string
	if v, ok := attrs["service.name"]; ok && v.StringValue != nil {
		idParts[0] = *v.StringValue
	}
	if v, ok := attrs["host.name"]; ok && v.StringValue != nil {
		idParts[1] = *v.StringValue
	}
	idParts[2] = schemaURL

	h := sha256.New()
	h.Write([]byte(idParts[0]))
	h.Write([]byte{0})
	h.Write([]byte(idParts[1]))
	h.Write([]byte{0})
	h.Write([]byte(idParts[2]))
	return "res-" + hex.EncodeToString(h.Sum(nil)[:16])
}

// mapScopeLogs converts ScopeLogs and adds records to the batch.
func (m *Mapper) mapScopeLogs(scopeLogs *logsV1.ScopeLogs, batch *model.LogBatch, resource *model.Resource) {
	if scopeLogs == nil {
		return
	}

	scopeName := ""
	scopeVersion := ""
	if scopeLogs.Scope != nil {
		scopeName = scopeLogs.Scope.Name
		scopeVersion = scopeLogs.Scope.Version
	}

	// Map log records
	for _, protoRecord := range scopeLogs.LogRecords {
		record := m.mapLogRecord(protoRecord, resource, scopeName, scopeVersion)
		batch.AddRecord(record)
	}
}

// mapLogRecord converts OTLP LogRecord to internal LogRecord.
func (m *Mapper) mapLogRecord(protoRecord *logsV1.LogRecord, resource *model.Resource, scopeName, scopeVersion string) *model.LogRecord { //nolint:lll
	if protoRecord == nil {
		return nil
	}

	record := model.NewLogRecord()

	// Map timestamps (protobuf fixed64 → int64; overflow impossible for reasonable dates)
	record.Timestamp = int64(protoRecord.TimeUnixNano)                 //nolint:gosec
	record.ObservedTimestamp = int64(protoRecord.ObservedTimeUnixNano) //nolint:gosec

	// Map severity
	record.SeverityNumber = model.Severity(protoRecord.SeverityNumber)
	record.SeverityText = protoRecord.SeverityText

	// Map trace context
	if len(protoRecord.TraceId) == 16 {
		record.TraceID = make([]byte, 16)
		copy(record.TraceID, protoRecord.TraceId)
	}
	if len(protoRecord.SpanId) == 8 {
		record.SpanID = make([]byte, 8)
		copy(record.SpanID, protoRecord.SpanId)
	}

	// Map body
	if protoRecord.Body != nil {
		record.Body = mapAnyValueToString(protoRecord.Body)
	}

	// Map attributes
	if len(protoRecord.Attributes) > 0 {
		record.Attributes = make(map[string]model.AttributeValue, len(protoRecord.Attributes))
		for _, kv := range protoRecord.Attributes {
			record.Attributes[kv.Key] = mapAnyValue(kv.Value)
		}
	}

	record.DroppedAttributesCount = protoRecord.DroppedAttributesCount
	record.Flags = protoRecord.Flags
	record.EventName = protoRecord.EventName

	// Set resource reference
	if resource != nil {
		record.ResourceID = resource.ID
	}

	// Set scope info
	record.ScopeName = scopeName
	record.ScopeVersion = scopeVersion

	return record
}

// mapAnyValue converts OTLP AnyValue to internal AttributeValue.
func mapAnyValue(anyValue *commonV1.AnyValue) model.AttributeValue {
	if anyValue == nil {
		return model.AttributeValue{}
	}

	switch val := anyValue.Value.(type) {
	case *commonV1.AnyValue_StringValue:
		return model.NewStringValue(val.StringValue)
	case *commonV1.AnyValue_IntValue:
		return model.NewIntValue(val.IntValue)
	case *commonV1.AnyValue_DoubleValue:
		return model.NewDoubleValue(val.DoubleValue)
	case *commonV1.AnyValue_BoolValue:
		return model.NewBoolValue(val.BoolValue)
	case *commonV1.AnyValue_BytesValue:
		return model.NewBytesValue(val.BytesValue)
	case *commonV1.AnyValue_ArrayValue:
		return mapArrayValue(val.ArrayValue)
	case *commonV1.AnyValue_KvlistValue:
		return mapKVListValue(val.KvlistValue)
	default:
		// Unknown type (including MapValue and StringValueStrindex), return null value
		return model.AttributeValue{}
	}
}

// mapAnyValueToString converts OTLP AnyValue to a string representation.
func mapAnyValueToString(anyValue *commonV1.AnyValue) string {
	if anyValue == nil {
		return ""
	}

	switch val := anyValue.Value.(type) {
	case *commonV1.AnyValue_StringValue:
		return val.StringValue
	case *commonV1.AnyValue_IntValue:
		return strconv.FormatInt(val.IntValue, 10)
	case *commonV1.AnyValue_DoubleValue:
		return strconv.FormatFloat(val.DoubleValue, 'f', -1, 64)
	case *commonV1.AnyValue_BoolValue:
		if val.BoolValue {
			return "true"
		}
		return "false"
	case *commonV1.AnyValue_BytesValue:
		return string(val.BytesValue)
	case *commonV1.AnyValue_ArrayValue:
		return "[array]"
	case *commonV1.AnyValue_KvlistValue:
		return "[kvlist]"
	default:
		return ""
	}
}

// mapArrayValue converts OTLP ArrayValue to internal AttributeValue array.
func mapArrayValue(arrayValue *commonV1.ArrayValue) model.AttributeValue {
	if arrayValue == nil {
		return model.AttributeValue{}
	}

	values := make([]model.AttributeValue, 0, len(arrayValue.Values))
	for _, val := range arrayValue.Values {
		values = append(values, mapAnyValue(val))
	}

	return model.NewArrayValue(values)
}

// mapKVListValue converts OTLP KVListValue to internal AttributeValue map.
func mapKVListValue(kvListValue *commonV1.KeyValueList) model.AttributeValue {
	if kvListValue == nil {
		return model.AttributeValue{}
	}

	values := make(map[string]model.AttributeValue, len(kvListValue.Values))
	for _, kv := range kvListValue.Values {
		values[kv.Key] = mapAnyValue(kv.Value)
	}

	return model.NewMapValue(values)
}
