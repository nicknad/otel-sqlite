package sqlite

import (
	"fmt"
	"testing"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// BenchmarkGetRecordPutRecord measures the allocator behavior of
// GetRecord + PutRecord without any SQLite I/O. This isolates the
// pool-vs-alloc difference.
func BenchmarkGetRecordPutRecord(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := model.GetRecord()
		r.Body = "test message"
		r.SeverityText = "INFO"
		r.ResourceID = "res-1"
		r.Attributes = append(
			r.Attributes,
			model.Attribute{Key: "k1", Str: "v1", Kind: model.ValueString},
			model.Attribute{Key: "k2", Str: "v2", Kind: model.ValueString},
		)
		model.PutRecord(r)
	}
}

// BenchmarkGetRecordFillOnly measures allocation without PutRecord —
// this shows the raw alloc cost when records are eventually GC'd
// (the nopool steady-state).
func BenchmarkGetRecordFillOnly(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := model.GetRecord()
		r.Body = "test message"
		r.SeverityText = "INFO"
		r.ResourceID = "res-1"
		r.Attributes = append(
			r.Attributes,
			model.Attribute{Key: "k1", Str: "v1", Kind: model.ValueString},
			model.Attribute{Key: "k2", Str: "v2", Kind: model.ValueString},
		)
		_ = r
	}
}

// BenchmarkLogRecordBatch_Throughput simulates the mapper path:
// allocate N records, fill them, create a batch, then release.
// Batch size and attribute count match production defaults.
func BenchmarkLogRecordBatch_Throughput(b *testing.B) {
	const (
		batchSize   = 250
		attrsPerRec = 4
	)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		batch := model.NewLogBatch(batchSize)
		for j := 0; j < batchSize; j++ {
			rec := model.GetRecord()
			rec.Body = fmt.Sprintf("msg-%d-%d", i, j)
			rec.ResourceID = "res-1"
			for k := 0; k < attrsPerRec; k++ {
				rec.Attributes = append(rec.Attributes, model.Attribute{
					Key:  fmt.Sprintf("attr_%d", k),
					Str:  fmt.Sprintf("val_%d", k),
					Kind: model.ValueString,
				})
			}
			batch.AddRecord(rec)
		}
		// Simulate writer releasing records.
		for _, rec := range batch.Records {
			model.PutRecord(rec)
		}
	}
}
