//! Metric persistence across all five metric kinds.
//!
//! Each record resolves (and upserts) its scope/metric/series dimensions,
//! then appends one `metric_data_point` row per data point. Kind-specific
//! payloads that have no dedicated columns (histogram buckets, exponential
//! buckets, summary quantiles) are stored as JSON documents.

use otel_sqlite_core::model::{
    Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint, MetricData, NumberDataPoint,
    NumberValue, Resource, SummaryDataPoint, Temporality,
};
use otel_sqlite_core::storage::MetricWriteBatch;
use rusqlite::{Connection, Statement, params};

use super::InsertScratch;
use super::identity::{resolve_metric, resolve_resource, resolve_scope, resolve_series};
use super::json::{
    ExemplarsDoc, ExponentialDoc, HistogramDoc, SummaryDoc, write_attributes_json, write_json,
};
use crate::error::{FailureClass, StorageError};

const METRIC_TYPE_GAUGE: i64 = 0;
const METRIC_TYPE_SUM: i64 = 1;
const METRIC_TYPE_HISTOGRAM: i64 = 2;
const METRIC_TYPE_EXPONENTIAL_HISTOGRAM: i64 = 3;
const METRIC_TYPE_SUMMARY: i64 = 4;

/// Reserved `nan_mask` column value: OTLP data points carry no NaN-presence
/// bits, so every row is written with the "no NaNs" mask.
const RESERVED_NAN_MASK: i64 = 0;

const SQL_INSERT_SERIES: &str = "
INSERT INTO metric_series (id, metric_id, attributes_json)
VALUES (?1, ?2, ?3)
ON CONFLICT (id) DO NOTHING";

const SQL_INSERT_DATA_POINT: &str = "
INSERT INTO metric_data_point (
    series_id,
    timestamp_ns,
    start_timestamp_ns,
    flags,
    double_value,
    int_value,
    count,
    sum,
    min,
    max,
    nan_mask,
    histogram_json,
    exponential_histogram_json,
    summary_json,
    exemplars_json
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)";

/// Persists one storage-sized metric write batch (one transaction per call).
pub(crate) fn insert_metrics(
    conn: &Connection,
    batch: &MetricWriteBatch,
) -> Result<u64, StorageError> {
    insert_metrics_inner(conn, batch, false).map(|(written, _dropped)| written)
}

/// Salvage variant of [`insert_metrics`]: data points that fail with
/// poison-class errors are skipped (and counted) while healthy points are
/// still inserted. Returns `(written, dropped)`.
pub(crate) fn insert_metrics_tolerant(
    conn: &Connection,
    batch: &MetricWriteBatch,
) -> Result<(u64, u64), StorageError> {
    insert_metrics_inner(conn, batch, true)
}

fn insert_metrics_inner(
    conn: &Connection,
    batch: &MetricWriteBatch,
    tolerant: bool,
) -> Result<(u64, u64), StorageError> {
    // Row-level serialization reuses these buffers; the second scratch only
    // serves the rare records carrying their own resource so the batch-level
    // dimensions in `scratch` survive them (mirrors `insert_logs`).
    let mut scratch = InsertScratch::new();
    let mut record_scratch = InsertScratch::new();

    let default_resource;
    let batch_resource = if let Some(resource) = &batch.origin.resource {
        resource
    } else {
        default_resource = Resource::default();
        &default_resource
    };
    resolve_resource(conn, batch_resource, &mut scratch)?;

    // Prepared once per batch; `prepare_cached` lookups are not free.
    let mut series_statement = conn.prepare_cached(SQL_INSERT_SERIES)?;
    let mut point_statement = conn.prepare_cached(SQL_INSERT_DATA_POINT)?;

    let mut written = 0u64;
    let mut dropped = 0u64;
    for record in batch.records.records() {
        // A record-level resource overrides the batch origin for this
        // record's scope/metric/series chain, exactly like log records.
        let scratch = match &record.resource {
            Some(resource) => {
                resolve_resource(conn, resource, &mut record_scratch)?;
                &mut record_scratch
            }
            None => &mut scratch,
        };
        resolve_scope(
            conn,
            record.scope_name.as_str(),
            record.scope_version.as_str(),
            record.scope_schema_url.as_str(),
            &record.scope_attributes,
            scratch,
        )?;

        let (metric_type, is_monotonic, temporality) = match &record.data {
            MetricData::Gauge(_) => (METRIC_TYPE_GAUGE, false, Temporality::Unspecified),
            MetricData::Sum(sum) => (
                METRIC_TYPE_SUM,
                sum.is_monotonic,
                sum.aggregation_temporality,
            ),
            MetricData::Histogram(histogram) => (
                METRIC_TYPE_HISTOGRAM,
                false,
                histogram.aggregation_temporality,
            ),
            MetricData::ExponentialHistogram(histogram) => (
                METRIC_TYPE_EXPONENTIAL_HISTOGRAM,
                false,
                histogram.aggregation_temporality,
            ),
            MetricData::Summary(_) => (METRIC_TYPE_SUMMARY, false, Temporality::Unspecified),
        };

        resolve_metric(
            conn,
            record.name.as_str(),
            record.description.as_str(),
            record.unit.as_str(),
            &record.metadata,
            metric_type,
            is_monotonic,
            temporality,
            scratch,
        )?;

        match &record.data {
            MetricData::Gauge(gauge) => {
                let (kept, points_dropped) = write_number_points(
                    &mut series_statement,
                    &mut point_statement,
                    &gauge.data_points,
                    tolerant,
                    scratch,
                )?;
                written += kept;
                dropped += points_dropped;
            }
            MetricData::Sum(sum) => {
                let (kept, points_dropped) = write_number_points(
                    &mut series_statement,
                    &mut point_statement,
                    &sum.data_points,
                    tolerant,
                    scratch,
                )?;
                written += kept;
                dropped += points_dropped;
            }
            MetricData::Histogram(histogram) => {
                let (kept, points_dropped) = write_histogram_points(
                    &mut series_statement,
                    &mut point_statement,
                    &histogram.data_points,
                    tolerant,
                    scratch,
                )?;
                written += kept;
                dropped += points_dropped;
            }
            MetricData::ExponentialHistogram(histogram) => {
                let (kept, points_dropped) = write_exponential_points(
                    &mut series_statement,
                    &mut point_statement,
                    &histogram.data_points,
                    tolerant,
                    scratch,
                )?;
                written += kept;
                dropped += points_dropped;
            }
            MetricData::Summary(summary) => {
                let (kept, points_dropped) = write_summary_points(
                    &mut series_statement,
                    &mut point_statement,
                    &summary.data_points,
                    tolerant,
                    scratch,
                )?;
                written += kept;
                dropped += points_dropped;
            }
        }
    }
    Ok((written, dropped))
}

/// Renders a point's exemplars into `out` and returns the parameter to bind
/// for the `exemplars_json` column (`NULL` when the point carries no
/// exemplars, matching the pre-exemplar storage shape).
fn exemplars_param<'a>(exemplars: &[Exemplar], out: &'a mut String) -> Option<&'a str> {
    if exemplars.is_empty() {
        return None;
    }
    write_json(&ExemplarsDoc(exemplars), out);
    Some(out.as_str())
}

/// Converts a `u64` OTLP count for the `count` column. A value past
/// `i64::MAX` cannot be represented; treating it as a row-local
/// (poison-class) error makes the writer's salvage pass quarantine just that
/// point instead of silently persisting NULL.
fn count_param(
    count: u64,
    tolerant: bool,
    timestamp_ns: i64,
    dropped: &mut u64,
) -> Result<Option<i64>, StorageError> {
    match i64::try_from(count) {
        Ok(value) => Ok(Some(value)),
        Err(_) => {
            if tolerant {
                *dropped += 1;
                tracing::warn!(
                    timestamp_ns,
                    count,
                    "metric data point count exceeds i64::MAX; quarantined (dropped)"
                );
                Ok(None)
            } else {
                Err(StorageError::Sqlite(
                    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "metric data point count exceeds i64::MAX",
                    ))),
                ))
            }
        }
    }
}

/// Classifies one data-point insert: counts it written, quarantines a
/// poisonous row when `tolerant` (warn + drop), or fails the batch.
fn handle_point_result(
    result: Result<usize, rusqlite::Error>,
    tolerant: bool,
    timestamp_ns: i64,
    written: &mut u64,
    dropped: &mut u64,
) -> Result<(), StorageError> {
    match result {
        Ok(_) => *written += 1,
        Err(error) if tolerant && crate::error::classify_sqlite(&error) == FailureClass::Poison => {
            *dropped += 1;
            tracing::warn!(
                timestamp_ns,
                %error,
                "poisonous metric data point quarantined (dropped); healthy points keep committing"
            );
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

/// Writes gauge/sum data points. The metric id is read from
/// `scratch.metric_id`; the per-point series id and attribute/payload JSON
/// land in the remaining scratch buffers, reused across points.
fn write_number_points(
    series_statement: &mut Statement<'_>,
    point_statement: &mut Statement<'_>,
    points: &[NumberDataPoint],
    tolerant: bool,
    scratch: &mut InsertScratch,
) -> Result<(u64, u64), StorageError> {
    let mut written = 0u64;
    let mut dropped = 0u64;
    for point in points {
        write_attributes_json(&point.attributes, scratch);
        resolve_series(
            series_statement,
            &scratch.metric_id,
            &scratch.json,
            &mut scratch.series_id,
        )?;

        let (value_int, value_double) = match point.value {
            Some(NumberValue::Int(value)) => (Some(value), None),
            Some(NumberValue::Double(value)) => (None, Some(value)),
            None => (None, None),
        };
        let exemplars = exemplars_param(&point.exemplars, &mut scratch.exemplars_json);

        handle_point_result(
            point_statement.execute(params![
                &*scratch.series_id,
                point.time_unix_nano,
                point.start_time_unix_nano,
                point.flags,
                value_double,
                value_int,
                None::<i64>,
                None::<f64>,
                None::<f64>,
                None::<f64>,
                RESERVED_NAN_MASK,
                None::<String>,
                None::<String>,
                None::<String>,
                exemplars,
            ]),
            tolerant,
            point.time_unix_nano,
            &mut written,
            &mut dropped,
        )?;
    }
    Ok((written, dropped))
}

fn write_histogram_points(
    series_statement: &mut Statement<'_>,
    point_statement: &mut Statement<'_>,
    points: &[HistogramDataPoint],
    tolerant: bool,
    scratch: &mut InsertScratch,
) -> Result<(u64, u64), StorageError> {
    let mut written = 0u64;
    let mut dropped = 0u64;
    for point in points {
        let Some(row_count) =
            count_param(point.count, tolerant, point.time_unix_nano, &mut dropped)?
        else {
            continue;
        };
        write_attributes_json(&point.attributes, scratch);
        resolve_series(
            series_statement,
            &scratch.metric_id,
            &scratch.json,
            &mut scratch.series_id,
        )?;

        // The attributes JSON was consumed by the series fingerprint above;
        // the buffer is free for this point's payload document.
        write_json(
            &HistogramDoc {
                bounds: &point.explicit_bounds,
                counts: &point.bucket_counts,
            },
            &mut scratch.json,
        );
        let exemplars = exemplars_param(&point.exemplars, &mut scratch.exemplars_json);

        handle_point_result(
            point_statement.execute(params![
                &*scratch.series_id,
                point.time_unix_nano,
                point.start_time_unix_nano,
                point.flags,
                None::<f64>,
                None::<i64>,
                row_count,
                point.sum,
                point.min,
                point.max,
                RESERVED_NAN_MASK,
                &*scratch.json,
                None::<String>,
                None::<String>,
                exemplars,
            ]),
            tolerant,
            point.time_unix_nano,
            &mut written,
            &mut dropped,
        )?;
    }
    Ok((written, dropped))
}

fn write_exponential_points(
    series_statement: &mut Statement<'_>,
    point_statement: &mut Statement<'_>,
    points: &[ExponentialHistogramDataPoint],
    tolerant: bool,
    scratch: &mut InsertScratch,
) -> Result<(u64, u64), StorageError> {
    let mut written = 0u64;
    let mut dropped = 0u64;
    for point in points {
        let Some(row_count) =
            count_param(point.count, tolerant, point.time_unix_nano, &mut dropped)?
        else {
            continue;
        };
        write_attributes_json(&point.attributes, scratch);
        resolve_series(
            series_statement,
            &scratch.metric_id,
            &scratch.json,
            &mut scratch.series_id,
        )?;

        // The attributes JSON was consumed by the series fingerprint above;
        // the buffer is free for this point's payload document.
        write_json(
            &ExponentialDoc {
                negative: point.negative.as_ref(),
                positive: point.positive.as_ref(),
                scale: point.scale,
                zero_count: point.zero_count,
                zero_threshold: point.zero_threshold,
            },
            &mut scratch.json,
        );
        let exemplars = exemplars_param(&point.exemplars, &mut scratch.exemplars_json);

        handle_point_result(
            point_statement.execute(params![
                &*scratch.series_id,
                point.time_unix_nano,
                point.start_time_unix_nano,
                point.flags,
                None::<f64>,
                None::<i64>,
                row_count,
                point.sum,
                point.min,
                point.max,
                RESERVED_NAN_MASK,
                None::<String>,
                &*scratch.json,
                None::<String>,
                exemplars,
            ]),
            tolerant,
            point.time_unix_nano,
            &mut written,
            &mut dropped,
        )?;
    }
    Ok((written, dropped))
}

fn write_summary_points(
    series_statement: &mut Statement<'_>,
    point_statement: &mut Statement<'_>,
    points: &[SummaryDataPoint],
    tolerant: bool,
    scratch: &mut InsertScratch,
) -> Result<(u64, u64), StorageError> {
    let mut written = 0u64;
    let mut dropped = 0u64;
    for point in points {
        let Some(row_count) =
            count_param(point.count, tolerant, point.time_unix_nano, &mut dropped)?
        else {
            continue;
        };
        write_attributes_json(&point.attributes, scratch);
        resolve_series(
            series_statement,
            &scratch.metric_id,
            &scratch.json,
            &mut scratch.series_id,
        )?;

        // The attributes JSON was consumed by the series fingerprint above;
        // the buffer is free for this point's payload document.
        write_json(
            &SummaryDoc {
                count: point.count,
                quantile_values: &point.quantile_values,
                sum: point.sum,
            },
            &mut scratch.json,
        );
        let exemplars = exemplars_param(&point.exemplars, &mut scratch.exemplars_json);

        handle_point_result(
            point_statement.execute(params![
                &*scratch.series_id,
                point.time_unix_nano,
                point.start_time_unix_nano,
                point.flags,
                None::<f64>,
                None::<i64>,
                row_count,
                Some(point.sum),
                None::<f64>,
                None::<f64>,
                RESERVED_NAN_MASK,
                None::<String>,
                None::<String>,
                &*scratch.json,
                exemplars,
            ]),
            tolerant,
            point.time_unix_nano,
            &mut written,
            &mut dropped,
        )?;
    }
    Ok((written, dropped))
}
