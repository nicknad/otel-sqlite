package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// newTestMetrics creates a Metrics with a custom registry so tests don't conflict.
func newTestMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)

	return &Metrics{
		LogsReceived: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "ingest", Name: "logs_received_total",
			Help: "Total number of log records received",
		}),
		LogsWritten: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "storage", Name: "logs_written_total",
			Help: "Total number of log records written to storage",
		}),
		IngressQueueDepth: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "otel_collector", Subsystem: "ingest", Name: "ingress_queue_depth",
			Help: "Current depth of the ingress queue",
		}),
		BatchQueueDepth: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "otel_collector", Subsystem: "ingest", Name: "batch_queue_depth",
			Help: "Current depth of the batch queue",
		}),
		BatchesCreated: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "batcher", Name: "batches_created_total",
			Help: "Total number of batches created",
		}),
		BatchesWritten: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "storage", Name: "batches_written_total",
			Help: "Total number of batches written to storage",
		}),
		BatchSize: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "otel_collector", Subsystem: "batcher", Name: "batch_size",
			Help:    "Size of batches (number of log records)",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		}),
		WriteLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "otel_collector", Subsystem: "storage", Name: "write_latency_seconds",
			Help:    "Latency of write operations in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		WriteErrors: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "storage", Name: "write_errors_total",
			Help: "Total number of write errors",
		}),
		ActiveResources: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "otel_collector", Subsystem: "storage", Name: "active_resources",
			Help: "Number of active resources",
		}),
		TotalResources: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "storage", Name: "total_resources",
			Help: "Total number of unique resources",
		}),
	}
}

func TestMetricsAll(t *testing.T) {
	m := newTestMetrics()

	// Increment methods
	m.IncrementLogsReceived(5)
	m.IncrementLogsWritten(3)
	m.IncrementBatchesCreated()
	m.IncrementBatchesWritten()
	m.IncrementWriteErrors()
	m.IncrementTotalResources()

	// Update methods
	m.UpdateIngressQueueDepth(50)
	m.UpdateBatchQueueDepth(10)
	m.RecordBatchSize(100)
	m.RecordWriteLatency(0.05)
	m.SetActiveResources(3)
}

func TestProductionNewMetrics(t *testing.T) {
	// This must be the only test that calls the production NewMetrics()
	// since promauto registers with the global registry.
	m := NewMetrics()
	if m == nil {
		t.Fatal("NewMetrics() returned nil")
	}
}
