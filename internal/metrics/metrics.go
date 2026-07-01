// Package metrics provides Prometheus metrics for the OTLP collector.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the collector.
type Metrics struct {
	// Ingestion metrics
	LogsReceived      prometheus.Counter
	LogsWritten       prometheus.Counter
	IngressQueueDepth prometheus.Gauge
	BatchQueueDepth   prometheus.Gauge

	// Batch metrics
	BatchesCreated prometheus.Counter
	BatchesWritten prometheus.Counter
	BatchSize      prometheus.Histogram

	// Storage metrics
	WriteLatency prometheus.Histogram
	WriteErrors  prometheus.Counter

	// Command execution metrics
	CommandQueueDepth        prometheus.Gauge
	CommandExecutionDuration prometheus.Histogram
	CommandsExecutedTotal    prometheus.Counter
	CommandFailuresTotal     prometheus.Counter

	// Resource metrics
	ActiveResources prometheus.Gauge
	TotalResources  prometheus.Counter
}

// NewMetrics creates a new Metrics instance with all metrics registered.
func NewMetrics() *Metrics {
	return &Metrics{
		// Ingestion metrics
		LogsReceived: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector",
			Subsystem: "ingest",
			Name:      "logs_received_total",
			Help:      "Total number of log records received",
		}),

		LogsWritten: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector",
			Subsystem: "storage",
			Name:      "logs_written_total",
			Help:      "Total number of log records written to storage",
		}),

		IngressQueueDepth: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "otel_collector",
			Subsystem: "ingest",
			Name:      "ingress_queue_depth",
			Help:      "Current depth of the ingress queue",
		}),

		BatchQueueDepth: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "otel_collector",
			Subsystem: "ingest",
			Name:      "batch_queue_depth",
			Help:      "Current depth of the batch queue",
		}),

		// Batch metrics
		BatchesCreated: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector",
			Subsystem: "batcher",
			Name:      "batches_created_total",
			Help:      "Total number of batches created",
		}),

		BatchesWritten: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector",
			Subsystem: "storage",
			Name:      "batches_written_total",
			Help:      "Total number of batches written to storage",
		}),

		BatchSize: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "otel_collector",
			Subsystem: "batcher",
			Name:      "batch_size",
			Help:      "Size of batches (number of log records)",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
		}),

		// Storage metrics
		WriteLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "otel_collector",
			Subsystem: "storage",
			Name:      "write_latency_seconds",
			Help:      "Latency of write operations in seconds",
			Buckets:   prometheus.DefBuckets,
		}),

		WriteErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector",
			Subsystem: "storage",
			Name:      "write_errors_total",
			Help:      "Total number of write errors",
		}),

		// Command execution metrics
		CommandQueueDepth: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "otel_collector",
			Subsystem: "command",
			Name:      "queue_depth",
			Help:      "Current depth of the command queue",
		}),

		CommandExecutionDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "otel_collector",
			Subsystem: "command",
			Name:      "execution_duration_seconds",
			Help:      "Duration of command execution batches in seconds",
			Buckets:   prometheus.DefBuckets,
		}),

		CommandsExecutedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector",
			Subsystem: "command",
			Name:      "executed_total",
			Help:      "Total number of commands executed",
		}),

		CommandFailuresTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector",
			Subsystem: "command",
			Name:      "failures_total",
			Help:      "Total number of command execution failures",
		}),

		// Resource metrics
		ActiveResources: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "otel_collector",
			Subsystem: "storage",
			Name:      "active_resources",
			Help:      "Number of active resources",
		}),

		TotalResources: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector",
			Subsystem: "storage",
			Name:      "total_resources",
			Help:      "Total number of unique resources",
		}),
	}
}

// All methods are nil-receiver safe so callers (e.g. the storage writer and
// batcher) do not need to nil-check when run without metrics in unit tests.

// UpdateIngressQueueDepth updates the ingress queue depth metric.
func (m *Metrics) UpdateIngressQueueDepth(depth int) {
	if m == nil {
		return
	}
	m.IngressQueueDepth.Set(float64(depth))
}

// UpdateBatchQueueDepth updates the batch queue depth metric.
func (m *Metrics) UpdateBatchQueueDepth(depth int) {
	if m == nil {
		return
	}
	m.BatchQueueDepth.Set(float64(depth))
}

// RecordBatchSize records the size of a batch.
func (m *Metrics) RecordBatchSize(size int) {
	if m == nil {
		return
	}
	m.BatchSize.Observe(float64(size))
}

// RecordWriteLatency records the latency of a write operation.
func (m *Metrics) RecordWriteLatency(durationSeconds float64) {
	if m == nil {
		return
	}
	m.WriteLatency.Observe(durationSeconds)
}

// IncrementLogsReceived increments the logs received counter.
func (m *Metrics) IncrementLogsReceived(count int) {
	if m == nil {
		return
	}
	m.LogsReceived.Add(float64(count))
}

// IncrementLogsWritten increments the logs written counter.
func (m *Metrics) IncrementLogsWritten(count int) {
	if m == nil {
		return
	}
	m.LogsWritten.Add(float64(count))
}

// IncrementBatchesCreated increments the batches created counter.
func (m *Metrics) IncrementBatchesCreated() {
	if m == nil {
		return
	}
	m.BatchesCreated.Inc()
}

// IncrementBatchesWritten increments the batches written counter.
func (m *Metrics) IncrementBatchesWritten() {
	if m == nil {
		return
	}
	m.BatchesWritten.Inc()
}

// IncrementWriteErrors increments the write errors counter.
func (m *Metrics) IncrementWriteErrors() {
	if m == nil {
		return
	}
	m.WriteErrors.Inc()
}

// UpdateCommandQueueDepth updates the command queue depth metric.
func (m *Metrics) UpdateCommandQueueDepth(depth int) {
	if m == nil {
		return
	}
	m.CommandQueueDepth.Set(float64(depth))
}

// RecordCommandExecutionDuration records the duration of a command execution batch.
func (m *Metrics) RecordCommandExecutionDuration(durationSeconds float64) {
	if m == nil {
		return
	}
	m.CommandExecutionDuration.Observe(durationSeconds)
}

// IncrementCommandsExecuted increments the commands executed counter.
func (m *Metrics) IncrementCommandsExecuted() {
	if m == nil {
		return
	}
	m.CommandsExecutedTotal.Inc()
}

// IncrementCommandFailures increments the command failures counter.
func (m *Metrics) IncrementCommandFailures() {
	if m == nil {
		return
	}
	m.CommandFailuresTotal.Inc()
}

// SetActiveResources sets the number of active resources.
func (m *Metrics) SetActiveResources(count int) {
	if m == nil {
		return
	}
	m.ActiveResources.Set(float64(count))
}

// IncrementTotalResources increments the total resources counter.
func (m *Metrics) IncrementTotalResources() {
	if m == nil {
		return
	}
	m.TotalResources.Inc()
}
