package sqlite

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"

	sqlite3 "github.com/mattn/go-sqlite3"
)

func TestStoredSeverityText(t *testing.T) {
	tests := []struct {
		name     string
		number   model.Severity
		text     string
		expected any
	}{
		{"empty", model.SeverityInfo, "", nil},
		{"standard", model.SeverityInfo, "INFO", nil},
		{"unspecified", model.SeverityUnspecified, "UNSPECIFIED", nil},
		{"custom", model.SeverityInfo, "informational", "informational"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := storedSeverityText(&model.LogRecord{
				SeverityNumber: test.number,
				SeverityText:   test.text,
			})
			if actual != test.expected {
				t.Fatalf("stored severity text = %#v, want %#v", actual, test.expected)
			}
		})
	}
}

func TestMarshalEventAttrs(t *testing.T) {
	encoded, err := marshalEventAttrs([]model.Attribute{
		{Key: "string", Str: "value", Kind: model.ValueString},
		{Key: "int", Num: 42, Kind: model.ValueInt},
		{Key: "double", Dbl: 3.5, Kind: model.ValueDouble},
		{Key: "bool", Flag: true, Kind: model.ValueBool},
		{Key: "bytes", Raw: []byte{0, 255}, Kind: model.ValueBytes},
		{Key: "null", Kind: model.ValueNull},
		{Key: "duplicate", Str: "first", Kind: model.ValueString},
		{Key: "duplicate", Str: "last", Kind: model.ValueString},
	})
	if err != nil {
		t.Fatal(err)
	}

	var values map[string]interface{}
	if err := json.Unmarshal(encoded, &values); err != nil {
		t.Fatal(err)
	}
	if values["string"] != "value" || values["int"] != float64(42) || values["double"] != 3.5 || values["bool"] != true || values["null"] != nil || values["duplicate"] != "last" {
		t.Fatalf("unexpected JSON: %s", encoded)
	}
	bytesVal, ok := values["bytes"].(map[string]interface{})
	if !ok || bytesVal["$b"] != "AP8=" {
		t.Fatalf("unexpected bytes: %#v", values["bytes"])
	}

	empty, err := marshalEventAttrs(nil)
	if err != nil || string(empty) != "{}" {
		t.Fatalf("empty attributes = %q, err = %v", empty, err)
	}

	// Escaping / Unicode round-trip through encoding/json.
	escaped, err := marshalEventAttrs([]model.Attribute{
		{Key: `a"b\c`, Str: "line1\nline2\t\u0001", Kind: model.ValueString},
		{Key: "uni", Str: "żźć", Kind: model.ValueString},
	})
	if err != nil {
		t.Fatal(err)
	}
	var escapedMap map[string]string
	if err := json.Unmarshal(escaped, &escapedMap); err != nil {
		t.Fatalf("escaped JSON %s: %v", escaped, err)
	}
	if escapedMap[`a"b\c`] != "line1\nline2\t\u0001" || escapedMap["uni"] != "żźć" {
		t.Fatalf("escaped round-trip: %#v", escapedMap)
	}
}

func TestMarshalEventAttrsRejectsNonFiniteDouble(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := marshalEventAttrs([]model.Attribute{{Key: "bad", Dbl: value, Kind: model.ValueDouble}}); err == nil {
			t.Errorf("marshalEventAttrs(%v) returned no error", value)
		}
	}
}

// TestWriteBatchInsertMix verifies the hot path issues exactly one
// log_resource insert attempt and one log_event insert per record, with no
// log_attr (or other) table writes. Uses go-sqlite3's update hook as a
// statement-level counter so source inspection is not the only proof.
func TestWriteBatchInsertMix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "insert-mix.db")
	db, err := openDatabase(path, true, false)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	defer db.Close()
	if err := RunMigrations(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var (
		mu      sync.Mutex
		inserts = map[string]int{}
	)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()
	if err := conn.Raw(func(driverConn any) error {
		sc, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			t.Fatalf("driver conn type %T, want *sqlite3.SQLiteConn", driverConn)
		}
		sc.RegisterUpdateHook(func(op int, _ string, table string, _ int64) {
			if op != sqlite3.SQLITE_INSERT {
				return
			}
			mu.Lock()
			inserts[table]++
			mu.Unlock()
		})
		return nil
	}); err != nil {
		t.Fatalf("RegisterUpdateHook: %v", err)
	}

	const nEvents = 25
	resource := model.NewResource(map[string]model.AttributeValue{
		"service.name": model.NewStringValue("insert-mix"),
	})
	resource.ID = "res-insert-mix"

	batch := model.NewLogBatch(nEvents)
	batch.Resource = resource
	for i := 0; i < nEvents; i++ {
		rec := model.GetRecord()
		rec.Timestamp = time.Now().UnixNano()
		rec.ObservedTimestamp = rec.Timestamp
		rec.SeverityNumber = model.SeverityInfo
		rec.Body = "mix"
		rec.Attributes = []model.Attribute{
			{Key: "i", Num: int64(i), Kind: model.ValueInt},
			{Key: "k", Str: "v", Kind: model.ValueString},
			{Key: "ok", Flag: true, Kind: model.ValueBool},
			{Key: "n", Kind: model.ValueNull},
		}
		batch.AddRecord(rec)
	}

	// BeginTx on the hooked connection so the update hook sees the writes.
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	cmd := NewWriteBatchCommand(batch)
	if err := cmd.Execute(context.Background(), tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("Execute: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	assertInserts := func(wantEvents, wantResources int, label string) {
		t.Helper()
		mu.Lock()
		defer mu.Unlock()
		if inserts["log_event"] != wantEvents {
			t.Fatalf("%s: log_event inserts = %d, want %d; all=%v",
				label, inserts["log_event"], wantEvents, inserts)
		}
		if inserts["log_resource"] != wantResources {
			t.Fatalf("%s: log_resource inserts = %d, want %d; all=%v",
				label, inserts["log_resource"], wantResources, inserts)
		}
		if n := inserts["log_attr"]; n != 0 {
			t.Fatalf("%s: log_attr inserts = %d, want 0", label, n)
		}
		for table, n := range inserts {
			switch table {
			case "log_event", "log_resource":
			default:
				t.Fatalf("%s: unexpected insert into %s (%d times)", label, table, n)
			}
		}
	}
	assertInserts(nEvents, 1, "first batch")

	// Query through the same reserved conn: openDatabase uses MaxOpenConns(1),
	// so db.QueryRow would block forever while conn is held.
	var events int
	if err := conn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM log_event`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != nEvents {
		t.Fatalf("event rows = %d, want %d", events, nEvents)
	}

	// Second batch with the same resource must not insert another resource row
	// (INSERT OR IGNORE), and still one event insert per record.
	mu.Lock()
	clear(inserts)
	mu.Unlock()

	batch2 := model.NewLogBatch(nEvents)
	batch2.Resource = resource
	for i := 0; i < nEvents; i++ {
		rec := model.GetRecord()
		rec.Timestamp = time.Now().UnixNano()
		rec.ObservedTimestamp = rec.Timestamp
		rec.SeverityNumber = model.SeverityInfo
		rec.Body = "mix2"
		batch2.AddRecord(rec)
	}
	tx2, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewWriteBatchCommand(batch2).Execute(context.Background(), tx2); err != nil {
		_ = tx2.Rollback()
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}

	assertInserts(nEvents, 0, "second batch")
}

func BenchmarkMarshalEventAttrs(b *testing.B) {
	attrs := []model.Attribute{
		{Key: "http.method", Str: "GET", Kind: model.ValueString},
		{Key: "http.status_code", Num: 200, Kind: model.ValueInt},
		{Key: "ok", Flag: true, Kind: model.ValueBool},
		{Key: "ratio", Dbl: 0.5, Kind: model.ValueDouble},
		{Key: "payload", Raw: []byte{1, 2, 3, 4}, Kind: model.ValueBytes},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := marshalEventAttrs(attrs); err != nil {
			b.Fatal(err)
		}
	}
}
