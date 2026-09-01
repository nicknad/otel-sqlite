//! Harness-side telemetry collection.
//!
//! The production crates emit their instrumentation through the `metrics`
//! facade (`otlp_*`, `ingress_*`, `storage_*`). This module installs a global
//! recorder that aggregates those emissions into per-window snapshots so
//! benchmark reports can include queue depths, queue-full events, batch sizes,
//! transaction durations, and error counts without touching production code.
//!
//! Windows delimit measurement phases: `begin_window` resets peaks/histograms
//! and records counter baselines; `end_window` produces counter deltas plus
//! in-window gauge and histogram summaries.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram as MetricsHistogram, HistogramFn, Key, KeyName,
    Metadata, Recorder, SharedString, Unit,
};

/// Aggregated view of one window of metric activity.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct WindowReport {
    /// Counter deltas over the window.
    pub counters: BTreeMap<String, u64>,
    /// Last observed gauge values.
    pub gauges_last: BTreeMap<String, f64>,
    /// Maximum gauge values observed during the window.
    pub gauges_peak: BTreeMap<String, f64>,
    /// Histogram summaries over the window (milliseconds).
    pub histograms: BTreeMap<String, HistogramSummary>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct HistogramSummary {
    pub count: u64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p99_9_ms: f64,
    pub max_ms: f64,
}

impl HistogramSummary {
    fn from(histogram: &hdrhistogram::Histogram<u64>) -> Self {
        Self {
            count: histogram.len(),
            p50_ms: micros_to_ms(histogram.value_at_quantile(0.50)),
            p90_ms: micros_to_ms(histogram.value_at_quantile(0.90)),
            p95_ms: micros_to_ms(histogram.value_at_quantile(0.95)),
            p99_ms: micros_to_ms(histogram.value_at_quantile(0.99)),
            p99_9_ms: micros_to_ms(histogram.value_at_quantile(0.999)),
            max_ms: micros_to_ms(histogram.max()),
        }
    }
}

fn micros_to_ms(micros: u64) -> f64 {
    micros as f64 / 1_000.0
}

fn format_key(key: &Key) -> String {
    let labels = key.labels().collect::<Vec<_>>();
    if labels.is_empty() {
        return key.name().to_owned();
    }
    let rendered = labels
        .iter()
        .map(|label| format!("{}={}", label.key(), label.value()))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}{{{}}}", key.name(), rendered)
}

// ---------------------------------------------------------------------------
// Metric cells
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct CounterCell(AtomicU64);

impl CounterFn for CounterCell {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

/// Lock-free gauge cell tracking the last value and the running peak.
#[derive(Debug)]
struct GaugeCell {
    last: AtomicU64,
    peak: AtomicU64,
}

impl GaugeCell {
    fn new() -> Self {
        Self {
            last: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        }
    }

    fn set(&self, value: f64) {
        self.last.store(value.to_bits(), Ordering::Relaxed);
        let mut prev = self.peak.load(Ordering::Relaxed);
        while f64::from_bits(prev) < value {
            match self.peak.compare_exchange_weak(
                prev,
                value.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => prev = current,
            }
        }
    }

    fn reset_peak(&self) {
        self.peak.store(0, Ordering::Relaxed);
    }

    fn last(&self) -> f64 {
        f64::from_bits(self.last.load(Ordering::Relaxed))
    }

    fn peak(&self) -> f64 {
        f64::from_bits(self.peak.load(Ordering::Relaxed))
    }
}

impl GaugeFn for GaugeCell {
    fn increment(&self, value: f64) {
        self.set(self.last() + value);
    }

    fn decrement(&self, value: f64) {
        self.set((self.last() - value).max(0.0));
    }

    fn set(&self, value: f64) {
        GaugeCell::set(self, value);
    }
}

/// Values arrive in seconds from the production crates; stored as microseconds.
#[derive(Debug)]
struct HistogramCell(std::sync::Mutex<hdrhistogram::Histogram<u64>>);

impl HistogramFn for HistogramCell {
    fn record(&self, value: f64) {
        let micros = (value * 1_000_000.0).clamp(0.0, 3_600_000_000.0) as u64;
        if let Ok(mut guard) = self.0.lock() {
            guard.saturating_record(micros);
        }
    }
}

// ---------------------------------------------------------------------------
// Registry + recorder
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Registry {
    counters: RwLock<HashMap<String, Arc<CounterCell>>>,
    gauges: RwLock<HashMap<String, Arc<GaugeCell>>>,
    histograms: RwLock<HashMap<String, Arc<HistogramCell>>>,
    baselines: RwLock<HashMap<String, u64>>,
}

impl Registry {
    fn counter_cell(&self, key: &Key) -> Arc<CounterCell> {
        let name = format_key(key);
        self.counters
            .read()
            .ok()
            .and_then(|map| map.get(&name).cloned())
            .unwrap_or_else(|| {
                self.counters
                    .write()
                    .expect("counter registry lock")
                    .entry(name)
                    .or_insert_with(|| Arc::new(CounterCell(AtomicU64::new(0))))
                    .clone()
            })
    }

    fn gauge_cell(&self, key: &Key) -> Arc<GaugeCell> {
        let name = format_key(key);
        self.gauges
            .read()
            .ok()
            .and_then(|map| map.get(&name).cloned())
            .unwrap_or_else(|| {
                self.gauges
                    .write()
                    .expect("gauge registry lock")
                    .entry(name)
                    .or_insert_with(|| Arc::new(GaugeCell::new()))
                    .clone()
            })
    }

    fn histogram_cell(&self, key: &Key) -> Arc<HistogramCell> {
        let name = format_key(key);
        self.histograms
            .read()
            .ok()
            .and_then(|map| map.get(&name).cloned())
            .unwrap_or_else(|| {
                self.histograms
                    .write()
                    .expect("histogram registry lock")
                    .entry(name)
                    .or_insert_with(|| {
                        Arc::new(HistogramCell(std::sync::Mutex::new(
                            hdrhistogram::Histogram::<u64>::new_with_max(3_600_000_000, 3)
                                .expect("valid histogram configuration"),
                        )))
                    })
                    .clone()
            })
    }

    fn reset_windows(&self) {
        if let Ok(gauges) = self.gauges.read() {
            for cell in gauges.values() {
                cell.reset_peak();
            }
        }
        if let Ok(histograms) = self.histograms.read() {
            for cell in histograms.values() {
                if let Ok(mut guard) = cell.0.lock() {
                    *guard = hdrhistogram::Histogram::<u64>::new_with_max(3_600_000_000, 3)
                        .expect("valid histogram configuration");
                }
            }
        }
        let mut baselines = self.baselines.write().expect("baseline lock");
        baselines.clear();
        if let Ok(counters) = self.counters.read() {
            for (name, cell) in counters.iter() {
                baselines.insert(name.clone(), cell.0.load(Ordering::Relaxed));
            }
        }
    }

    fn window_report(&self) -> WindowReport {
        let mut report = WindowReport::default();
        let baselines = self.baselines.read().expect("baseline lock");

        if let Ok(counters) = self.counters.read() {
            for (name, cell) in counters.iter() {
                let total = cell.0.load(Ordering::Relaxed);
                let base = baselines.get(name).copied().unwrap_or(0);
                report
                    .counters
                    .insert(name.clone(), total.saturating_sub(base));
            }
        }

        if let Ok(gauges) = self.gauges.read() {
            for (name, cell) in gauges.iter() {
                report.gauges_last.insert(name.clone(), cell.last());
                report.gauges_peak.insert(name.clone(), cell.peak());
            }
        }

        if let Ok(histograms) = self.histograms.read() {
            for (name, cell) in histograms.iter() {
                if let Ok(guard) = cell.0.lock() {
                    report
                        .histograms
                        .insert(name.clone(), HistogramSummary::from(&guard));
                }
            }
        }

        report
    }
}

/// The `metrics` facade recorder installed inside the benchmark process.
#[derive(Debug)]
pub struct HarnessRecorder {
    registry: Arc<Registry>,
}

impl Recorder for HarnessRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(self.registry.counter_cell(key))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(self.registry.gauge_cell(key))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> MetricsHistogram {
        MetricsHistogram::from_arc(self.registry.histogram_cell(key))
    }
}

// ---------------------------------------------------------------------------
// Public handle
// ---------------------------------------------------------------------------

/// Cheap handle onto the installed recorder.
#[derive(Clone)]
pub struct TelemetryHandle {
    registry: Arc<Registry>,
}

impl TelemetryHandle {
    /// Install the global recorder exactly once. Panics if one was already
    /// installed (the facade enforces single installation).
    pub fn install() -> Self {
        let registry = Arc::new(Registry::default());
        let recorder = HarnessRecorder {
            registry: Arc::clone(&registry),
        };
        metrics::set_global_recorder(recorder).expect("telemetry recorder installed once");
        Self { registry }
    }

    /// Start a new measurement window (resets peaks, histograms, baselines).
    pub fn begin_window(&self) {
        self.registry.reset_windows();
    }

    /// Close the current measurement window and return its aggregate report.
    pub fn end_window(&self) -> WindowReport {
        self.registry.window_report()
    }

    /// Current gauge values (for periodic samplers).
    pub fn sample_gauges(&self) -> BTreeMap<String, f64> {
        let mut out = BTreeMap::new();
        if let Ok(gauges) = self.registry.gauges.read() {
            for (name, cell) in gauges.iter() {
                out.insert(name.clone(), cell.last());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // All tests share one process-global recorder, and windows are global
    // state: serialize the windowed assertions so concurrent tests cannot
    // reset each other's baselines mid-flight.
    static WINDOW_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn counters_delta_across_window() {
        let handle = install_for_test();
        let _guard = WINDOW_LOCK.lock().unwrap();
        ::metrics::counter!("test_counter_total").increment(5);

        handle.begin_window();
        ::metrics::counter!("test_counter_total").increment(7);
        let report = handle.end_window();

        assert_eq!(report.counters.get("test_counter_total"), Some(&7));
    }

    #[test]
    fn gauge_peak_tracked_within_window() {
        let handle = install_for_test();
        let _guard = WINDOW_LOCK.lock().unwrap();

        handle.begin_window();
        ::metrics::gauge!("test_depth").set(12.0);
        ::metrics::gauge!("test_depth").set(4.0);
        let report = handle.end_window();

        assert_eq!(report.gauges_last.get("test_depth"), Some(&4.0));
        assert_eq!(report.gauges_peak.get("test_depth"), Some(&12.0));
    }

    #[test]
    fn histogram_records_and_resets() {
        let handle = install_for_test();
        let _guard = WINDOW_LOCK.lock().unwrap();

        handle.begin_window();
        ::metrics::histogram!("test_duration").record(0.001);
        ::metrics::histogram!("test_duration").record(0.003);
        let report = handle.end_window();

        let summary = report.histograms.get("test_duration").expect("summary");
        assert_eq!(summary.count, 2);
        assert!(summary.p99_ms >= 1.0 && summary.p99_ms <= 4.0);

        handle.begin_window();
        let cleared = handle.end_window();
        assert!(
            cleared
                .histograms
                .get("test_duration")
                .is_none_or(|s| s.count == 0)
        );
    }

    fn install_for_test() -> TelemetryHandle {
        // Each test needs its own recorder but the facade allows only one
        // global installation; share a lazily-created instance and rely on
        // distinct metric names across tests.
        static HANDLE: std::sync::OnceLock<TelemetryHandle> = std::sync::OnceLock::new();
        HANDLE.get_or_init(TelemetryHandle::install).clone()
    }
}
