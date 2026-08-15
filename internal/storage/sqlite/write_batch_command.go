package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
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
	InsertResource  *sql.Stmt
	InsertEvent     *sql.Stmt
	InsertScope     *sql.Stmt
	InsertMetric    *sql.Stmt
	InsertSeries    *sql.Stmt
	InsertDataPoint *sql.Stmt
}

// Close closes all prepared statements.
func (ps *PreparedStatements) Close() {
	if ps == nil {
		return
	}
	for _, stmt := range []*sql.Stmt{
		ps.InsertResource,
		ps.InsertEvent,
		ps.InsertScope,
		ps.InsertMetric,
		ps.InsertSeries,
		ps.InsertDataPoint,
	} {
		if stmt != nil {
			stmt.Close()
		}
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

	// optional process-local set of resource IDs already inserted successfully.
	// When the batch resource ID is present, the INSERT OR IGNORE is skipped.
	seenResources map[string]struct{}

	// pendingResources holds resource IDs actually inserted during Execute.
	// CommitSeen merges them into seenResources after the transaction
	// commits; on rollback they are discarded with the command so the cache
	// never references uncommitted rows.
	pendingResources map[string]struct{}
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

// SetSeenResources injects a process-local cache of resource IDs that have
// already been inserted successfully. The map is owned by the Writer and must
// only be used from the single writer goroutine. After the surrounding
// transaction commits, the Writer must call CommitSeen so the resource IDs
// inserted by Execute become visible to later transactions; after a rollback
// it must not.
func (c *WriteBatchCommand) SetSeenResources(seen map[string]struct{}) {
	c.seenResources = seen
}

// Size returns the number of log records in this command.
func (c *WriteBatchCommand) Size() int {
	return c.records
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

	// Insert the resource row (INSERT OR IGNORE for dedup). Skip the SQL when
	// this process has already inserted the same resource ID successfully.
	if c.batch.Resource != nil {
		resourceID := c.batch.Resource.ID
		_, alreadySeen := c.seenResources[resourceID]
		if !alreadySeen {
			serviceName := c.batch.Resource.GetServiceName()
			hostName := c.batch.Resource.GetHostName()
			attrsJSON := marshalResourceAttrs(c.batch.Resource.Attributes)
			if _, err := insertResource.ExecContext(
				ctx,
				resourceID,
				serviceName,
				hostName,
				c.batch.Resource.SchemaURL,
				attrsJSON,
			); err != nil {
				for _, record := range c.batch.Records {
					model.PutRecord(record)
				}
				return fmt.Errorf("insert resource %q: %w", resourceID, err)
			}
			if c.seenResources != nil && resourceID != "" {
				if c.pendingResources == nil {
					c.pendingResources = make(map[string]struct{})
				}
				c.pendingResources[resourceID] = struct{}{}
			}
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

// CommitSeen merges the resource IDs actually inserted during Execute into
// the live seenResources cache. The Writer calls it after tx.Commit
// succeeds; a rolled-back transaction leaves the cache untouched so the
// resource row is re-inserted (INSERT OR IGNORE) next time. Safe to call
// with the cache disabled.
func (c *WriteBatchCommand) CommitSeen() {
	if c.seenResources == nil || len(c.pendingResources) == 0 {
		return
	}
	for id := range c.pendingResources {
		c.seenResources[id] = struct{}{}
	}
	c.pendingResources = nil
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

	// Bind attributes as []byte to avoid an extra string copy; SQLite stores TEXT.
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

// emptyEventAttrsJSON is the canonical encoding for records with no attributes.
var emptyEventAttrsJSON = []byte("{}")

// attrJSONBufPool reuses buffers for hand-rolled event-attribute JSON.
var attrJSONBufPool = sync.Pool{
	New: func() any {
		// Typical 4–8 string attrs fit under 512B; grows as needed.
		b := make([]byte, 0, 512)
		return bytes.NewBuffer(b)
	},
}

// marshalEventAttrs serializes event attributes as a compact JSON object.
// Duplicate keys are last-write-wins. Returns a fresh []byte safe to bind
// after the pooled buffer is returned.
func marshalEventAttrs(attrs []model.Attribute) ([]byte, error) {
	if len(attrs) == 0 {
		return emptyEventAttrsJSON, nil
	}

	// Last-write-wins index without allocating map[string]any or using
	// encoding/json reflection on the hot path.
	lastIdx := make(map[string]int, len(attrs))
	for i, attr := range attrs {
		lastIdx[attr.Key] = i
	}

	buf := attrJSONBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer attrJSONBufPool.Put(buf)

	buf.WriteByte('{')
	first := true
	for i, attr := range attrs {
		if lastIdx[attr.Key] != i {
			continue
		}
		if !first {
			buf.WriteByte(',')
		}
		first = false
		writeJSONString(buf, attr.Key)
		buf.WriteByte(':')
		if err := writeJSONAttrValue(buf, attr); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')

	// Copy out of the pooled buffer so the caller owns the result.
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

func writeJSONAttrValue(buf *bytes.Buffer, attr model.Attribute) error {
	switch attr.Kind {
	case model.ValueString:
		writeJSONString(buf, attr.Str)
	case model.ValueInt:
		buf.WriteString(strconv.FormatInt(attr.Num, 10))
	case model.ValueDouble:
		if math.IsNaN(attr.Dbl) || math.IsInf(attr.Dbl, 0) {
			return fmt.Errorf("attribute %q has non-finite double", attr.Key)
		}
		// ES6-style shortest round-trip, matching encoding/json for finite floats.
		buf.WriteString(strconv.FormatFloat(attr.Dbl, 'f', -1, 64))
	case model.ValueBool:
		if attr.Flag {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case model.ValueBytes:
		buf.WriteString(`{"$b":`)
		writeJSONString(buf, base64.StdEncoding.EncodeToString(attr.Raw))
		buf.WriteByte('}')
	case model.ValueNull:
		buf.WriteString("null")
	default:
		return fmt.Errorf("attribute %q has unsupported kind %d", attr.Key, attr.Kind)
	}
	return nil
}

// writeJSONString writes a JSON string value including surrounding quotes.
func writeJSONString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	start := 0
	for i := range len(s) {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		if start < i {
			buf.WriteString(s[start:i])
		}
		switch c {
		case '"', '\\':
			buf.WriteByte('\\')
			buf.WriteByte(c)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			// Control characters as \u00XX.
			const hex = "0123456789abcdef"
			buf.WriteString(`\u00`)
			buf.WriteByte(hex[c>>4])
			buf.WriteByte(hex[c&0xf])
		}
		start = i + 1
	}
	if start < len(s) {
		buf.WriteString(s[start:])
	}
	buf.WriteByte('"')
}
