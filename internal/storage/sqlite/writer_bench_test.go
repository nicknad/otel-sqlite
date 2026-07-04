package sqlite

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

const insertEventSQL = "INSERT INTO log_event (resource_id, timestamp, severity, body, " +
	"trace_id, span_id, flags, attributes_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

const insertEventPrefix = "INSERT INTO log_event (resource_id, timestamp, severity, body, " +
	"trace_id, span_id, flags, attributes_json) VALUES "

func BenchmarkWriterInsert(b *testing.B) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create schema
	schema := `
	CREATE TABLE log_resource (
		id TEXT PRIMARY KEY,
		service_name TEXT,
		host_name TEXT,
		schema_url TEXT,
		attributes_json TEXT
	);
	
	CREATE TABLE log_event (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		resource_id TEXT,
		timestamp INTEGER,
		severity INTEGER,
		body TEXT,
		trace_id TEXT,
		span_id TEXT,
		flags INTEGER,
		attributes_json TEXT,
		FOREIGN KEY (resource_id) REFERENCES log_resource(id)
	);
	
	CREATE INDEX idx_log_event_timestamp ON log_event(timestamp);
	CREATE INDEX idx_log_event_severity ON log_event(severity);
	CREATE INDEX idx_log_event_resource_id ON log_event(resource_id);
	CREATE INDEX idx_log_event_trace_id ON log_event(trace_id);
	CREATE INDEX idx_log_event_span_id ON log_event(span_id);
	CREATE INDEX idx_log_event_flags ON log_event(flags);
	CREATE INDEX idx_log_event_resource_timestamp ON log_event(resource_id, timestamp);
	CREATE INDEX idx_log_event_severity_timestamp ON log_event(severity, timestamp);
	CREATE INDEX idx_log_event_trace_timestamp ON log_event(trace_id, timestamp);
	`
	if _, err := db.Exec(schema); err != nil {
		b.Fatal(err)
	}

	// Insert a resource
	if _, err := db.Exec("INSERT INTO log_resource (id, service_name) VALUES (?, ?)",
		"res-1", "test-service"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.Run("IndividualInserts", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}

			stmt, err := tx.PrepareContext(ctx, insertEventSQL)
			if err != nil {
				tx.Rollback()
				b.Fatal(err)
			}

			for j := 0; j < 100; j++ {
				_, err := stmt.ExecContext(
					ctx,
					"res-1",
					time.Now().UnixNano(),
					1,
					"test log message",
					"trace-123",
					"span-456",
					0,
					"{}",
				)
				if err != nil {
					stmt.Close()
					tx.Rollback()
					b.Fatal(err)
				}
			}

			stmt.Close()
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("BatchInsert", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}

			// Build batch INSERT with 100 rows
			query := insertEventPrefix
			args := make([]interface{}, 0, 800)
			for j := 0; j < 100; j++ {
				if j > 0 {
					query += ","
				}
				query += "(?, ?, ?, ?, ?, ?, ?, ?)"
				args = append(
					args,
					"res-1",
					time.Now().UnixNano(),
					1,
					"test log message",
					"trace-123",
					"span-456",
					0,
					"{}",
				)
			}

			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				tx.Rollback()
				b.Fatal(err)
			}

			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("PreparedBatchInsert", func(b *testing.B) {
		ctx := context.Background()

		// Prepare statement once
		stmt, err := db.PrepareContext(ctx, insertEventSQL)
		if err != nil {
			b.Fatal(err)
		}
		defer stmt.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}

			txStmt := tx.Stmt(stmt)
			for j := 0; j < 100; j++ {
				_, err := txStmt.ExecContext(
					ctx,
					"res-1",
					time.Now().UnixNano(),
					1,
					"test log message",
					"trace-123",
					"span-456",
					0,
					"{}",
				)
				if err != nil {
					tx.Rollback()
					b.Fatal(err)
				}
			}

			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkWriterWithFewerIndexes(b *testing.B) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create schema with only 3 essential indexes
	schema := `
	CREATE TABLE log_resource (
		id TEXT PRIMARY KEY,
		service_name TEXT,
		host_name TEXT,
		schema_url TEXT,
		attributes_json TEXT
	);
	
	CREATE TABLE log_event (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		resource_id TEXT,
		timestamp INTEGER,
		severity INTEGER,
		body TEXT,
		trace_id TEXT,
		span_id TEXT,
		flags INTEGER,
		attributes_json TEXT,
		FOREIGN KEY (resource_id) REFERENCES log_resource(id)
	);
	
	CREATE INDEX idx_log_event_timestamp ON log_event(timestamp);
	CREATE INDEX idx_log_event_resource_id ON log_event(resource_id);
	CREATE INDEX idx_log_event_trace_id ON log_event(trace_id);
	`
	if _, err := db.Exec(schema); err != nil {
		b.Fatal(err)
	}

	// Insert a resource
	if _, err := db.Exec("INSERT INTO log_resource (id, service_name) VALUES (?, ?)",
		"res-1", "test-service"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.Run("FewerIndexes", func(b *testing.B) {
		ctx := context.Background()

		stmt, err := db.PrepareContext(ctx, insertEventSQL)
		if err != nil {
			b.Fatal(err)
		}
		defer stmt.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}

			txStmt := tx.Stmt(stmt)
			for j := 0; j < 100; j++ {
				_, err := txStmt.ExecContext(
					ctx,
					"res-1",
					time.Now().UnixNano(),
					1,
					"test log message",
					"trace-123",
					"span-456",
					0,
					"{}",
				)
				if err != nil {
					tx.Rollback()
					b.Fatal(err)
				}
			}

			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
