package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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
		scope_name, scope_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	sqlInsertAttr = `INSERT INTO log_attr
		(event_id, key, value_type, string_value, int_value, double_value, bool_value, bytes_value)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
)

// PreparedStatements holds SQL statements prepared once at startup
// and reused across transactions via tx.Stmt().
type PreparedStatements struct {
	InsertResource *sql.Stmt
	InsertEvent    *sql.Stmt
	InsertAttr     *sql.Stmt
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
	if ps.InsertAttr != nil {
		ps.InsertAttr.Close()
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
		insertAttr     *sql.Stmt
	)

	if c.preparedStmts != nil {
		// Bind pre-prepared statements to this transaction.
		insertResource = tx.Stmt(c.preparedStmts.InsertResource)
		insertEvent = tx.Stmt(c.preparedStmts.InsertEvent)
		insertAttr = tx.Stmt(c.preparedStmts.InsertAttr)
	} else {
		// Fallback: prepare inline (used when command runs without the Writer).
		var err error
		insertResource, err = tx.PrepareContext(ctx, sqlInsertResource)
		if err != nil {
			return fmt.Errorf("prepare log_resource: %w", err)
		}
		insertEvent, err = tx.PrepareContext(ctx, sqlInsertEvent)
		if err != nil {
			insertResource.Close()
			return fmt.Errorf("prepare log_event: %w", err)
		}
		insertAttr, err = tx.PrepareContext(ctx, sqlInsertAttr)
		if err != nil {
			insertResource.Close()
			insertEvent.Close()
			return fmt.Errorf("prepare log_attr: %w", err)
		}
	}
	defer insertResource.Close()
	defer insertEvent.Close()
	defer insertAttr.Close()

	// Insert the resource row (INSERT OR IGNORE for dedup).
	if c.batch.Resource != nil {
		serviceName := c.batch.Resource.GetServiceName()
		hostName := c.batch.Resource.GetHostName()
		if _, err := insertResource.ExecContext(ctx,
			c.batch.Resource.ID,
			serviceName,
			hostName,
			c.batch.Resource.SchemaURL,
			"{}",
		); err != nil {
			return fmt.Errorf("insert resource %q: %w", c.batch.Resource.ID, err)
		}
	}

	// Insert each log record and its attributes.
	for _, record := range c.batch.Records {
		eventID, err := insertEventRecord(ctx, insertEvent, record)
		if err != nil {
			log.Printf("error inserting event: %v", err)
			continue
		}

		if err := insertAttributesRecord(ctx, insertAttr, eventID, record.Attributes); err != nil {
			log.Printf("error inserting attributes for event %d: %v", eventID, err)
			continue
		}
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

// insertEventRecord inserts a single log event row and returns its auto-generated ID.
func insertEventRecord(ctx context.Context, stmt *sql.Stmt, record *model.LogRecord) (int64, error) {
	result, err := stmt.ExecContext(ctx,
		nil, // ID will be auto-generated
		record.ResourceID,
		record.Timestamp,
		record.ObservedTimestamp,
		int64(record.SeverityNumber),
		record.SeverityText,
		record.TraceID,
		record.SpanID,
		record.Body,
		record.EventName,
		uint64(record.Flags),
		uint64(record.DroppedAttributesCount),
		record.ScopeName,
		record.ScopeVersion,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// insertAttributesRecord inserts attribute rows for a log event.
func insertAttributesRecord(ctx context.Context, stmt *sql.Stmt, eventID int64,
	attributes map[string]model.AttributeValue) error {
	if len(attributes) == 0 {
		return nil
	}

	for key, value := range attributes {
		_, err := stmt.ExecContext(ctx,
			eventID,
			key,
			value.Type(),
			getStringValue(&value),
			getIntValue(&value),
			getDoubleValue(&value),
			getBoolValue(&value),
			getBytesValue(&value),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// --- Attribute value extraction helpers -----------------------------------

func getStringValue(v *model.AttributeValue) *string {
	if v.StringValue != nil {
		return v.StringValue
	}
	return nil
}

func getIntValue(v *model.AttributeValue) *int64 {
	if v.IntValue != nil {
		return v.IntValue
	}
	return nil
}

func getDoubleValue(v *model.AttributeValue) *float64 {
	if v.DoubleValue != nil {
		return v.DoubleValue
	}
	return nil
}

func getBoolValue(v *model.AttributeValue) *bool {
	if v.BoolValue != nil {
		return v.BoolValue
	}
	return nil
}

func getBytesValue(v *model.AttributeValue) []byte {
	if v.BytesValue != nil {
		return v.BytesValue
	}
	return nil
}
