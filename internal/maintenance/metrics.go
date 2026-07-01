package maintenance

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds Prometheus metrics for the maintenance framework.
type Metrics struct {
	// RunsTotal counts the total number of worker evaluation cycles.
	RunsTotal prometheus.Counter

	// TaskRunsTotal counts individual task executions, labeled by task name.
	TaskRunsTotal *prometheus.CounterVec

	// FailuresTotal counts task execution failures, labeled by task name.
	FailuresTotal *prometheus.CounterVec

	// DurationSeconds records task execution duration, labeled by task name.
	DurationSeconds *prometheus.HistogramVec

	// LastRunTimestamp records the last successful run time, labeled by task name.
	LastRunTimestamp *prometheus.GaugeVec
}

// NewMetrics creates a new Metrics instance with all metrics registered.
func NewMetrics() *Metrics {
	return &Metrics{
		RunsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "otel_collector",
			Subsystem: "maintenance",
			Name:      "runs_total",
			Help:      "Total number of maintenance worker evaluation cycles",
		}),

		TaskRunsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "otel_collector",
			Subsystem: "maintenance",
			Name:      "task_runs_total",
			Help:      "Total number of maintenance task executions, labeled by task",
		}, []string{"task"}),

		FailuresTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "otel_collector",
			Subsystem: "maintenance",
			Name:      "failures_total",
			Help:      "Total number of maintenance task failures, labeled by task",
		}, []string{"task"}),

		DurationSeconds: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "otel_collector",
			Subsystem: "maintenance",
			Name:      "duration_seconds",
			Help:      "Duration of maintenance task execution in seconds",
			Buckets:   prometheus.DefBuckets,
		}, []string{"task"}),

		LastRunTimestamp: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "otel_collector",
			Subsystem: "maintenance",
			Name:      "last_run_timestamp",
			Help:      "Unix timestamp of the last successful maintenance task run",
		}, []string{"task"}),
	}
}

// All methods are nil-receiver safe so callers do not need to nil-check.

func (m *Metrics) incRunsTotal() {
	if m == nil {
		return
	}
	m.RunsTotal.Inc()
}

func (m *Metrics) incTaskRunsTotal(task string) {
	if m == nil {
		return
	}
	m.TaskRunsTotal.WithLabelValues(task).Inc()
}

func (m *Metrics) incFailuresTotal(task string) {
	if m == nil {
		return
	}
	m.FailuresTotal.WithLabelValues(task).Inc()
}

func (m *Metrics) observeDuration(task string, seconds float64) {
	if m == nil {
		return
	}
	m.DurationSeconds.WithLabelValues(task).Observe(seconds)
}

func (m *Metrics) setLastRunTimestamp(task string, ts float64) {
	if m == nil {
		return
	}
	m.LastRunTimestamp.WithLabelValues(task).Set(ts)
}
