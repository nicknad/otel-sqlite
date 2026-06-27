// Package model defines the internal domain models for the OTLP collector.
package model

// Resource represents an OpenTelemetry resource.
// It contains metadata about the source of the log records.
type Resource struct {
	// ID is a unique identifier for this resource.
	ID string

	// Attributes contains the resource attributes.
	Attributes map[string]AttributeValue

	// SchemaURL is the schema URL for this resource.
	SchemaURL string
}

// NewResource creates a new Resource with the given attributes.
func NewResource(attributes map[string]AttributeValue) *Resource {
	return &Resource{
		Attributes: attributes,
	}
}

// GetServiceName extracts the service.name attribute from the resource.
// Returns an empty string if not present.
func (r *Resource) GetServiceName() string {
	if r.Attributes == nil {
		return ""
	}
	if val, ok := r.Attributes["service.name"]; ok {
		if val.StringValue != nil {
			return *val.StringValue
		}
	}
	return ""
}

// GetHostName extracts the host.name attribute from the resource.
// Returns an empty string if not present.
func (r *Resource) GetHostName() string {
	if r.Attributes == nil {
		return ""
	}
	if val, ok := r.Attributes["host.name"]; ok {
		if val.StringValue != nil {
			return *val.StringValue
		}
	}
	return ""
}
