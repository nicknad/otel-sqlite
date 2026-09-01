// Package model defines the internal domain models for the OTLP collector.
// These models are independent of OTLP protobuf types and are used throughout
// the ingestion pipeline.
package model

import (
	"fmt"
	"strconv"
)

// AttributeValue represents a value that can be associated with a log record attribute.
// It supports the same types as OTLP AnyValue but is independent of protobuf types.
type AttributeValue struct {
	// Only one of these fields should be set
	StringValue *string                   `json:"string_value,omitempty"`
	IntValue    *int64                    `json:"int_value,omitempty"`
	DoubleValue *float64                  `json:"double_value,omitempty"`
	BoolValue   *bool                     `json:"bool_value,omitempty"`
	BytesValue  []byte                    `json:"bytes_value,omitempty"`
	ArrayValue  []AttributeValue          `json:"array_value,omitempty"`
	MapValue    map[string]AttributeValue `json:"map_value,omitempty"`
}

// Type returns the type of the attribute value.
func (v AttributeValue) Type() string { //nolint:gocritic // value receiver is intentional
	switch {
	case v.StringValue != nil:
		return "string"
	case v.IntValue != nil:
		return "int"
	case v.DoubleValue != nil:
		return "double"
	case v.BoolValue != nil:
		return "bool"
	case v.BytesValue != nil:
		return "bytes"
	case v.ArrayValue != nil:
		return "array"
	case v.MapValue != nil:
		return "map"
	default:
		return "null"
	}
}

// String returns a string representation of the value.
func (v AttributeValue) String() string { //nolint:gocritic // value receiver is intentional
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.IntValue != nil:
		return strconv.FormatInt(*v.IntValue, 10)
	case v.DoubleValue != nil:
		return strconv.FormatFloat(*v.DoubleValue, 'f', -1, 64)
	case v.BoolValue != nil:
		return strconv.FormatBool(*v.BoolValue)
	case v.BytesValue != nil:
		return fmt.Sprintf("[bytes:%d]", len(v.BytesValue))
	case v.ArrayValue != nil:
		return fmt.Sprintf("[array:%d]", len(v.ArrayValue))
	case v.MapValue != nil:
		return fmt.Sprintf("[map:%d]", len(v.MapValue))
	default:
		return "null"
	}
}

// NewStringValue creates a new string attribute value.
func NewStringValue(s string) AttributeValue {
	return AttributeValue{StringValue: &s}
}

// NewIntValue creates a new int64 attribute value.
func NewIntValue(i int64) AttributeValue {
	return AttributeValue{IntValue: &i}
}

// NewDoubleValue creates a new float64 attribute value.
func NewDoubleValue(d float64) AttributeValue {
	return AttributeValue{DoubleValue: &d}
}

// NewBoolValue creates a new bool attribute value.
func NewBoolValue(b bool) AttributeValue {
	return AttributeValue{BoolValue: &b}
}

// NewBytesValue creates a new bytes attribute value.
func NewBytesValue(b []byte) AttributeValue {
	return AttributeValue{BytesValue: b}
}

// NewArrayValue creates a new array attribute value.
func NewArrayValue(values []AttributeValue) AttributeValue {
	return AttributeValue{ArrayValue: values}
}

// NewMapValue creates a new map attribute value.
func NewMapValue(values map[string]AttributeValue) AttributeValue {
	return AttributeValue{MapValue: values}
}
