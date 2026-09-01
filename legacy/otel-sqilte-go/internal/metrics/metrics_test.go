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
		CommandQueueDepth: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "otel_collector", Subsystem: "command", Name: "queue_depth",
			Help: "Current depth of the command queue",
		}),
		CommandExecutionDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "otel_collector", Subsystem: "command", Name: "execution_duration_seconds",
			Help:    "Duration of command execution batches in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		CommandsExecutedTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "command", Name: "executed_total",
			Help: "Total number of commands executed",
		}),
		CommandFailuresTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "command", Name: "failures_total",
			Help: "Total number of command execution failures",
		}),
		ActiveResources: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "otel_collector", Subsystem: "storage", Name: "active_resources",
			Help: "Number of active resources",
		}),
		TotalResources: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "storage", Name: "total_resources",
			Help: "Total number of unique resources",
		}),
		MetricsReceived: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "ingest", Name: "metrics_received_total",
			Help: "Total number of OTLP metric data points received",
		}),
		MetricDataPointsWritten: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "storage", Name: "metric_data_points_written_total",
			Help: "Total number of OTLP metric data points written to storage",
		}),
		BackpressureRejections: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "ingest", Name: "backpressure_rejections_total",
			Help: "Total number of requests rejected by backpressure",
		}),
		NotifyEventsReceived: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "notify", Name: "events_received_total",
			Help: "Total number of notification events received",
		}),
		NotifyEventsMatched: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "notify", Name: "events_matched_total",
			Help: "Total number of notification events matched by rule",
		}, []string{"rule"}),
		NotifyEventsDelivered: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "notify", Name: "events_delivered_total",
			Help: "Total number of notifications delivered",
		}, []string{"destination"}),
		NotifyEventsFailed: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "notify", Name: "events_failed_total",
			Help: "Total number of notifications that failed",
		}, []string{"destination"}),
		NotifyEventsDeadLettered: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector", Subsystem: "notify", Name: "events_dead_lettered_total",
			Help: "Total number of notifications dead-lettered",
		}),
		NotifyQueueDepth: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "otel_collector", Subsystem: "notify", Name: "queue_depth",
			Help: "Current depth of the notification event queue",
		}),
		NotifyRetryQueueDepth: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "otel_collector", Subsystem: "notify", Name: "retry_queue_depth",
			Help: "Current depth of the notification retry queue",
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
	m.IncrementCommandsExecuted()
	m.IncrementCommandFailures()

	// Update methods
	m.UpdateIngressQueueDepth(50)
	m.UpdateBatchQueueDepth(10)
	m.RecordBatchSize(100)
	m.RecordWriteLatency(0.05)
	m.UpdateCommandQueueDepth(5)
	m.RecordCommandExecutionDuration(0.1)
	m.SetActiveResources(3)

	// Metrics-pipeline counters
	m.IncrementMetricsReceived(7)
	m.IncrementMetricDataPointsWritten(7)
	m.IncrementBackpressureRejections()

	// Notification counters (Vec-based take a rule/destination label)
	m.IncrementNotifyEventsReceived()
	m.IncrementNotifyEventsMatched("rule-a")
	m.IncrementNotifyEventsDelivered("http")
	m.IncrementNotifyEventsFailed("http")
	m.IncrementNotifyEventsDeadLettered()
	m.UpdateNotifyQueueDepth(3)
	m.UpdateNotifyRetryQueueDepth(2)

	// Verify nil-receiver safety
	var nilM *Metrics
	nilM.IncrementLogsReceived(1)
	nilM.UpdateIngressQueueDepth(1)
	nilM.RecordBatchSize(1)
	nilM.IncrementCommandsExecuted()
	nilM.IncrementMetricsReceived(1)
	nilM.IncrementMetricDataPointsWritten(1)
	nilM.IncrementBackpressureRejections()
	nilM.IncrementNotifyEventsReceived()
	nilM.IncrementNotifyEventsMatched("r")
	nilM.IncrementNotifyEventsDelivered("d")
	nilM.IncrementNotifyEventsFailed("d")
	nilM.IncrementNotifyEventsDeadLettered()
	nilM.UpdateNotifyQueueDepth(1)
	nilM.UpdateNotifyRetryQueueDepth(1)
}

func TestProductionNewMetrics(t *testing.T) {
	// This must be the only test that calls the production NewMetrics()
	// since promauto registers with the global registry.
	m := NewMetrics()
	if m == nil {
		t.Fatal("NewMetrics() returned nil")
	}
}
