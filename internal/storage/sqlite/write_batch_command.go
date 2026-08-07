package sqlite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// SQL statements used by WriteBatchCommand. Prepared once by the Writer
// and bound to each transaction via tx.Stmt() to avoid re-parsing overhead.
const (
	sqlInsertResource = `INSERT OR IGNORE INTO log_resource
		(id, service_name, host_name, schema_url, attributes_json)
		VALUES (?, ?, ?, ?, ?)`

	sqlInsertEvent = `INSERT INTO log_event
		(id, resource_id, timestamp_ns, observed_timestamp_ns,
		severity_number, severity_text, trace_id, span_id,
		body, event_name, flags, dropped_attributes_count,
		scope_name, scope_version, attributes_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

// PreparedStatements holds SQL statements prepared once at startup
// and reused across transactions via tx.Stmt().
type PreparedStatements struct {
	InsertResource *sql.Stmt
	InsertEvent    *sql.Stmt
}

// Close closes all prepared statements.
func (ps *PreparedStatements) Close() {
	if ps == nil {
		return
	}
	if ps.InsertResource != nil {
		ps.InsertResource.Close()
	}
	if ps.InsertEvent != nil {
		ps.InsertEvent.Close()
	}
}

// WriteBatchCommand is a Command that writes a single LogBatch to SQLite.
//
// It is immutable after construction: the batch and resource references
// are set at creation time and never modified.  During Execute the
// command mutates internal bookkeeping fields (e.g. ResourceID on
// records) but the caller-visible LogBatch is effectively owned by the
// command for the duration of execution.
//
// WriteBatchCommand must never begin, commit or roll back a transaction.
type WriteBatchCommand struct {
	batch   *model.LogBatch
	records int // number of log records in the batch (cached at construction)

	// pre-prepared statements, injected by the writer before Execute.
	// When nil, Execute falls back to inline tx.PrepareContext.
	preparedStmts *PreparedStatements
}

// NewWriteBatchCommand creates a new WriteBatchCommand from a LogBatch.
//
// The caller must not modify the batch after submission.
func NewWriteBatchCommand(batch *model.LogBatch) *WriteBatchCommand {
	numRecords := 0
	if batch != nil {
		numRecords = batch.Size()
	}
	return &WriteBatchCommand{
		batch:   batch,
		records: numRecords,
	}
}

// SetPreparedStatements injects pre-prepared SQL statements for use during
// Execute. When set the command uses tx.Stmt() to bind them to the transaction
// instead of calling tx.PrepareContext, avoiding repeated statement compilation.
func (c *WriteBatchCommand) SetPreparedStatements(stmts *PreparedStatements) {
	c.preparedStmts = stmts
}

// Size returns the number of log records in this command.
func (c *WriteBatchCommand) Size() int {
	return c.records
}

// Batch returns the underlying LogBatch (for metrics and diagnostics).
func (c *WriteBatchCommand) Batch() *model.LogBatch {
	return c.batch
}

// Execute writes the log batch inside the supplied transaction.
func (c *WriteBatchCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	if c.batch == nil || c.batch.IsEmpty() {
		return nil
	}

	// Ensure every record has a resource ID before we insert.
	// If the batch has no resource, pull it from the first record that
	// carries one (records retain it through the ingress queue).
	if c.batch.Resource == nil {
		for _, record := range c.batch.Records {
			if record.Resource != nil {
				c.batch.Resource = record.Resource
				break
			}
		}
	}
	if c.batch.Resource != nil {
		resourceID := ensureResourceID(c.batch.Resource)
		for _, record := range c.batch.Records {
			record.ResourceID = resourceID
		}
	}

	// Acquire insert statements, preferring pre-prepared ones.
	var (
		insertResource *sql.Stmt
		insertEvent    *sql.Stmt
	)

	if c.preparedStmts != nil {
		// Bind pre-prepared statements to this transaction.
		insertResource = tx.Stmt(c.preparedStmts.InsertResource)
		insertEvent = tx.Stmt(c.preparedStmts.InsertEvent)
	} else {
		// Fallback: prepare inline (used when command runs without the Writer).
		var err error
		insertResource, err = tx.PrepareContext(ctx, sqlInsertResource)
		if err != nil {
			return fmt.Errorf("prepare log_resource: %w", err)
		}
		insertEvent, err = tx.PrepareContext(ctx, sqlInsertEvent)
		if err != nil {
			_ = insertResource.Close()
			return fmt.Errorf("prepare log_event: %w", err)
		}
	}
	defer func() { _ = insertResource.Close() }()
	defer func() { _ = insertEvent.Close() }()

	// Insert the resource row (INSERT OR IGNORE for dedup).
	if c.batch.Resource != nil {
		serviceName := c.batch.Resource.GetServiceName()
		hostName := c.batch.Resource.GetHostName()
		attrsJSON := marshalResourceAttrs(c.batch.Resource.Attributes)
		if _, err := insertResource.ExecContext(
			ctx,
			c.batch.Resource.ID,
			serviceName,
			hostName,
			c.batch.Resource.SchemaURL,
			attrsJSON,
		); err != nil {
			for _, record := range c.batch.Records {
				model.PutRecord(record)
			}
			return fmt.Errorf("insert resource %q: %w", c.batch.Resource.ID, err)
		}
	}

	// Insert each log record. Event attributes are encoded into the same row;
	// there is no per-attribute SQL operation.
	for i, record := range c.batch.Records {
		if err := insertEventRecord(ctx, insertEvent, record); err != nil {
			for _, pending := range c.batch.Records[i:] {
				model.PutRecord(pending)
			}
			return fmt.Errorf("insert event: %w", err)
		}

		// Return record to pool after writing (ownership: writer releases records)
		model.PutRecord(record)
	}

	return nil
}

// ensureResourceID ensures the resource has a non-empty ID and returns it.
func ensureResourceID(resource *model.Resource) string {
	if resource.ID != "" {
		return resource.ID
	}
	// Generate a unique ID for resources that lack one (e.g., unit tests).
	resource.ID = fmt.Sprintf("res-%d", time.Now().UnixNano())
	return resource.ID
}

// insertEventRecord inserts a single log event row, including all event
// attributes, in one SQL operation.
func insertEventRecord(ctx context.Context, stmt *sql.Stmt, record *model.LogRecord) error {
	// Convert fixed-size arrays to slices for SQLite (only if present).
	var traceID, spanID any
	if record.HasTrace {
		traceID = record.TraceID[:]
	}
	if record.HasSpan {
		spanID = record.SpanID[:]
	}

	attributesJSON, err := marshalEventAttrs(record.Attributes)
	if err != nil {
		return err
	}

	severityText := storedSeverityText(record)

	_, err = stmt.ExecContext(
		ctx,
		nil, // ID will be auto-generated
		record.ResourceID,
		record.Timestamp,
		record.ObservedTimestamp,
		int64(record.SeverityNumber),
		severityText,
		traceID,
		spanID,
		record.Body,
		record.EventName,
		uint64(record.Flags),
		uint64(record.DroppedAttributesCount),
		record.ScopeName,
		record.ScopeVersion,
		attributesJSON,
	)
	return err
}

// marshalResourceAttrs serializes resource attributes to JSON for the
// attributes_json column. Returns "{}" when the map is empty or nil.
func marshalResourceAttrs(attrs map[string]model.AttributeValue) string {
	if len(attrs) == 0 {
		return "{}"
	}
	b, err := json.Marshal(attrs)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func storedSeverityText(record *model.LogRecord) any {
	if record.SeverityText == "" || record.SeverityText == record.SeverityNumber.String() {
		return nil
	}
	return record.SeverityText
}

// marshalEventAttrs serializes event attributes as a compact JSON object.
func marshalEventAttrs(attrs []model.Attribute) (string, error) {
	if len(attrs) == 0 {
		return "{}", nil
	}

	values := make(map[string]any, len(attrs))
	for _, attr := range attrs {
		switch attr.Kind {
		case model.ValueString:
			values[attr.Key] = attr.Str
		case model.ValueInt:
			values[attr.Key] = attr.Num
		case model.ValueDouble:
			if math.IsNaN(attr.Dbl) || math.IsInf(attr.Dbl, 0) {
				return "", fmt.Errorf("attribute %q has non-finite double", attr.Key)
			}
			values[attr.Key] = attr.Dbl
		case model.ValueBool:
			values[attr.Key] = attr.Flag
		case model.ValueBytes:
			values[attr.Key] = map[string]string{
				"$b": base64.StdEncoding.EncodeToString(attr.Raw),
			}
		case model.ValueNull:
			values[attr.Key] = nil
		default:
			return "", fmt.Errorf("attribute %q has unsupported kind %d", attr.Key, attr.Kind)
		}
	}

	encoded, err := json.Marshal(values)
	if err != nil {
		return "", fmt.Errorf("marshal event attributes: %w", err)
	}
	return string(encoded), nil
}
