package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestMigration005BackfillsLegacyAttributes(t *testing.T) {
	path := fmt.Sprintf("test_migration005_%d.db", time.Now().UnixNano())
	defer os.Remove(path)
	defer os.Remove(path + "-wal")
	defer os.Remove(path + "-shm")

	db, err := openDatabase(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		description TEXT
	)`); err != nil {
		t.Fatal(err)
	}
	for _, migration := range []struct {
		version string
		sql     string
	}{
		{"001", migration001SQL},
		{"002", migration002SQL},
		{"003", migration003SQL},
		{"004", migration004SQL},
	} {
		if _, err := db.Exec(migration.sql); err != nil {
			t.Fatalf("apply legacy migration %s: %v", migration.version, err)
		}
		if _, err := db.Exec(
			"INSERT INTO schema_migrations(version, description) VALUES(?, ?)",
			migration.version, migration.version,
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("INSERT INTO log_resource(id, service_name) VALUES('res-1', 'svc')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO log_event(
		id, resource_id, timestamp_ns, observed_timestamp_ns, severity_number,
		severity_text, flags, dropped_attributes_count)
		VALUES(1, 'res-1', 10, 10, 9, 'ERROR', 0, 0)`); err != nil {
		t.Fatal(err)
	}
	legacyAttrs := []struct {
		key, valueType string
		value          any
	}{
		{"string", "string", "hello \"world\""},
		{"integer", "int", int64(42)},
		{"double", "double", 3.5},
		{"boolean", "bool", true},
		{"bytes", "bytes", []byte{0x00, 0xff}},
		{"null", "null", nil},
		{"duplicate", "string", "first"},
		{"duplicate", "string", "last"},
	}
	for _, attr := range legacyAttrs {
		var err error
		switch attr.valueType {
		case "string":
			err = insertLegacyAttr(db, attr.key, attr.valueType, attr.value, nil, nil, nil, nil)
		case "int":
			err = insertLegacyAttr(db, attr.key, attr.valueType, nil, attr.value, nil, nil, nil)
		case "double":
			err = insertLegacyAttr(db, attr.key, attr.valueType, nil, nil, attr.value, nil, nil)
		case "bool":
			err = insertLegacyAttr(db, attr.key, attr.valueType, nil, nil, nil, attr.value, nil)
		case "bytes":
			err = insertLegacyAttr(db, attr.key, attr.valueType, nil, nil, nil, nil, attr.value)
		default:
			err = insertLegacyAttr(db, attr.key, attr.valueType, nil, nil, nil, nil, nil)
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := RunMigrations(db); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	var encoded string
	if err := db.QueryRow("SELECT attributes_json FROM log_event WHERE id=1").Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var values map[string]interface{}
	if err := json.Unmarshal([]byte(encoded), &values); err != nil {
		t.Fatal(err)
	}
	if values["string"] != `hello "world"` || values["integer"] != float64(42) || values["double"] != 3.5 || values["boolean"] != true || values["null"] != nil || values["duplicate"] != "last" {
		t.Fatalf("unexpected backfill: %s", encoded)
	}
	bytes, ok := values["bytes"].(map[string]interface{})
	if !ok || bytes["$b"] != "AP8=" {
		t.Fatalf("unexpected bytes value: %#v", values["bytes"])
	}

	var tableCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='log_attr'").Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 0 {
		t.Fatal("log_attr still exists after migration")
	}
	var viewValue string
	if err := db.QueryRow("SELECT attributes_json FROM logs WHERE id=1").Scan(&viewValue); err != nil {
		t.Fatalf("logs view does not expose attributes_json: %v", err)
	}
	if viewValue != encoded {
		t.Fatalf("view attributes = %q, event attributes = %q", viewValue, encoded)
	}
}

func insertLegacyAttr(db *sql.DB, key, valueType string, stringValue, intValue, doubleValue, boolValue, bytesValue any) error {
	_, err := db.Exec(`INSERT INTO log_attr(
		 event_id, key, value_type, string_value, int_value, double_value, bool_value, bytes_value)
		 VALUES(1, ?, ?, ?, ?, ?, ?, ?)`,
		key, valueType, stringValue, intValue, doubleValue, boolValue, bytesValue)
	return err
}

func TestMigration005RollbackAndRetry(t *testing.T) {
	path := fmt.Sprintf("test_migration005_retry_%d.db", time.Now().UnixNano())
	defer os.Remove(path)
	defer os.Remove(path + "-wal")
	defer os.Remove(path + "-shm")

	db := openLegacyDatabase(t, path)
	defer db.Close()
	if _, err := db.Exec("INSERT INTO log_resource(id, service_name) VALUES('res-1', 'svc')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO log_event(
		id, resource_id, timestamp_ns, observed_timestamp_ns, severity_number,
		flags, dropped_attributes_count) VALUES(1, 'res-1', 1, 1, 1, 0, 0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO log_attr(event_id, key, value_type)
		VALUES(1, 'bad', 'unsupported')`); err != nil {
		t.Fatal(err)
	}

	if err := RunMigrations(db); err == nil {
		t.Fatal("expected invalid legacy value type to fail migration")
	}
	var columnCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('log_event') WHERE name='attributes_json'").Scan(&columnCount); err != nil {
		t.Fatal(err)
	}
	if columnCount != 0 {
		t.Fatal("failed migration left attributes_json behind")
	}
	var tableCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='log_attr'").Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 1 {
		t.Fatal("failed migration removed log_attr")
	}

	if _, err := db.Exec(`UPDATE log_attr SET value_type='string', string_value='fixed' WHERE event_id=1`); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(db); err != nil {
		t.Fatalf("retry migration: %v", err)
	}
	var encoded string
	if err := db.QueryRow("SELECT attributes_json FROM log_event WHERE id=1").Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	if encoded != `{"bad":"fixed"}` {
		t.Fatalf("retry attributes = %q", encoded)
	}
}

func TestMigration005EmptyLegacyDatabaseCanReopen(t *testing.T) {
	path := fmt.Sprintf("test_migration005_empty_%d.db", time.Now().UnixNano())
	defer os.Remove(path)
	defer os.Remove(path + "-wal")
	defer os.Remove(path + "-shm")

	db := openLegacyDatabase(t, path)
	if err := RunMigrations(db); err != nil {
		t.Fatalf("empty legacy migration: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openDatabase(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := RunMigrations(reopened); err != nil {
		t.Fatalf("reopened migration: %v", err)
	}
}

func openLegacyDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := openDatabase(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		description TEXT
	)`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	for _, migration := range []struct {
		version string
		sql     string
	}{
		{"001", migration001SQL},
		{"002", migration002SQL},
		{"003", migration003SQL},
		{"004", migration004SQL},
	} {
		if _, err := db.Exec(migration.sql); err != nil {
			_ = db.Close()
			t.Fatalf("apply legacy migration %s: %v", migration.version, err)
		}
		if _, err := db.Exec("INSERT INTO schema_migrations(version, description) VALUES(?, ?)", migration.version, migration.version); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	return db
}
