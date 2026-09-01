package sqlite

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const logsViewWithAttributesSQL = `
CREATE VIEW logs AS
SELECT
    le.id                         AS id,
    le.resource_id                AS resource_id,
    le.timestamp_ns               AS timestamp_ns,
    le.observed_timestamp_ns      AS observed_timestamp_ns,
    le.severity_number            AS severity_number,
    le.severity_text              AS severity_text,
    le.trace_id                   AS trace_id,
    le.span_id                    AS span_id,
    le.body                       AS body,
    le.event_name                 AS event_name,
    le.flags                      AS flags,
    le.dropped_attributes_count   AS dropped_attributes_count,
    le.scope_name                 AS scope_name,
    le.scope_version              AS scope_version,
    le.attributes_json            AS attributes_json,
    lr.service_name               AS service_name,
    lr.host_name                  AS host_name,
    lr.schema_url                 AS schema_url
FROM log_event le
JOIN log_resource lr ON le.resource_id = lr.id;
`

// applyMigration005 performs the non-SQL portion of migration 005. It runs as
// part of the transaction created by RunMigrations.
func applyMigration005(tx *sql.Tx) error {
	columnExists, err := sqliteColumnExists(tx, "log_event", "attributes_json")
	if err != nil {
		return err
	}
	if !columnExists {
		if _, err := tx.Exec(migration005SQL); err != nil {
			return fmt.Errorf("add event attributes column: %w", err)
		}
	}

	legacyExists, err := sqliteTableExists(tx, "log_attr")
	if err != nil {
		return err
	}
	if legacyExists {
		if err := backfillEventAttributes(tx); err != nil {
			return err
		}
		if err := dropLegacyAttributeIndexes(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("DROP TABLE IF EXISTS log_attr"); err != nil {
			return fmt.Errorf("drop log_attr: %w", err)
		}
	}

	if _, err := tx.Exec("DROP VIEW IF EXISTS logs"); err != nil {
		return fmt.Errorf("drop logs view: %w", err)
	}
	if _, err := tx.Exec(logsViewWithAttributesSQL); err != nil {
		return fmt.Errorf("create logs view: %w", err)
	}
	return nil
}

func sqliteTableExists(tx *sql.Tx, table string) (bool, error) {
	var exists int
	if err := tx.QueryRow(
		"SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)",
		table,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("check table %q: %w", table, err)
	}
	return exists == 1, nil
}

func sqliteColumnExists(tx *sql.Tx, table, column string) (bool, error) {
	rows, err := tx.Query("PRAGMA table_info(" + quoteSQLiteIdentifier(table) + ")")
	if err != nil {
		return false, fmt.Errorf("inspect table %q: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, fmt.Errorf("scan table %q metadata: %w", table, err)
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate table %q metadata: %w", table, err)
	}
	return false, nil
}

func backfillEventAttributes(tx *sql.Tx) error {
	rows, err := tx.Query(`
		SELECT event_id, id, key, value_type,
		       string_value, int_value, double_value, bool_value, bytes_value
		FROM log_attr
		ORDER BY event_id, id`)
	if err != nil {
		return fmt.Errorf("read legacy attributes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	update, err := tx.Prepare(`UPDATE log_event SET attributes_json = ? WHERE id = ?`)
	if err != nil {
		return fmt.Errorf("prepare event attribute update: %w", err)
	}
	defer func() { _ = update.Close() }()

	var (
		currentEvent int64
		currentAttrs map[string]any
		haveEvent    bool
	)
	flush := func() error {
		if !haveEvent {
			return nil
		}
		encoded, err := json.Marshal(currentAttrs)
		if err != nil {
			return fmt.Errorf("marshal attributes for event %d: %w", currentEvent, err)
		}
		if _, err := update.Exec(string(encoded), currentEvent); err != nil {
			return fmt.Errorf("update attributes for event %d: %w", currentEvent, err)
		}
		return nil
	}

	for rows.Next() {
		var (
			eventID, attrID int64
			key, valueType  string
			stringValue     sql.NullString
			intValue        sql.NullInt64
			doubleValue     sql.NullFloat64
			boolValue       sql.NullInt64
			bytesValue      []byte
		)
		if err := rows.Scan(
			&eventID, &attrID, &key, &valueType,
			&stringValue, &intValue, &doubleValue, &boolValue, &bytesValue,
		); err != nil {
			return fmt.Errorf("scan legacy attribute %d: %w", attrID, err)
		}
		if !haveEvent || currentEvent != eventID {
			if err := flush(); err != nil {
				return err
			}
			currentEvent = eventID
			currentAttrs = make(map[string]any)
			haveEvent = true
		}

		value, err := legacyAttributeValue(valueType, stringValue, intValue, doubleValue, boolValue, bytesValue)
		if err != nil {
			return fmt.Errorf("attribute %d for event %d: %w", attrID, eventID, err)
		}
		// Rows are ordered by id, so assigning repeatedly makes the legacy
		// duplicate-key behavior explicit and deterministic.
		currentAttrs[key] = value
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate legacy attributes: %w", err)
	}
	return flush()
}

func legacyAttributeValue(
	valueType string,
	stringValue sql.NullString,
	intValue sql.NullInt64,
	doubleValue sql.NullFloat64,
	boolValue sql.NullInt64,
	bytesValue []byte,
) (any, error) {
	switch valueType {
	case "string":
		if !stringValue.Valid {
			return nil, nil
		}
		return stringValue.String, nil
	case "int":
		if !intValue.Valid {
			return nil, nil
		}
		return intValue.Int64, nil
	case "double":
		if !doubleValue.Valid {
			return nil, nil
		}
		if math.IsNaN(doubleValue.Float64) || math.IsInf(doubleValue.Float64, 0) {
			return nil, fmt.Errorf("non-finite double %s", strconv.FormatFloat(doubleValue.Float64, 'g', -1, 64))
		}
		return doubleValue.Float64, nil
	case "bool":
		if !boolValue.Valid {
			return nil, nil
		}
		return boolValue.Int64 != 0, nil
	case "bytes":
		return map[string]string{"$b": base64.StdEncoding.EncodeToString(bytesValue)}, nil
	case "null":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported value_type %q", valueType)
	}
}

func dropLegacyAttributeIndexes(tx *sql.Tx) error {
	rows, err := tx.Query(`
		SELECT name FROM sqlite_master
		WHERE type = 'index' AND name LIKE 'idx_log_attr_%'`)
	if err != nil {
		return fmt.Errorf("list legacy attribute indexes: %w", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan legacy attribute index: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate legacy attribute indexes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close legacy attribute indexes: %w", err)
	}

	for _, name := range names {
		if _, err := tx.Exec("DROP INDEX IF EXISTS " + quoteSQLiteIdentifier(name)); err != nil {
			return fmt.Errorf("drop legacy attribute index %q: %w", name, err)
		}
	}
	return nil
}

func quoteSQLiteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
