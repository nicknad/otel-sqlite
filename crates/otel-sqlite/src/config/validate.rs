use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use otel_sqlite_ingress::expand_bind_address;
use otel_sqlite_storage::warn_if_world_readable;

use super::{
    Config, MAX_ATTRIBUTE_KEY_BYTES, MAX_ATTRIBUTE_VALUE_BYTES, MAX_ATTRIBUTES_PER_RECORD,
    MAX_BODY_BYTES, MAX_GRPC_CONCURRENT_STREAMS, MAX_GRPC_RECV_MSG_SIZE, MAX_INGEST_QUEUE_CAPACITY,
    MAX_RECORDS_PER_REQUEST, MAX_RETENTION, MAX_SCOPE_METADATA_EXPANSION_BYTES, MAX_TIMEOUT,
    MIN_DB_QUOTA_BYTES, is_loopback_listen,
};

/// Validates the complete effective configuration before any listener or
/// worker is started. One function per section so limits, durations and
/// security gates evolve independently.
pub(crate) fn validate_config(config: &Config) -> Result<()> {
    validate_listen(config)?;
    validate_queues_and_grpc(config)?;
    validate_mapping_limits(config)?;
    validate_quota(config)?;
    validate_maintenance(config)?;
    validate_watchdog(config)?;
    validate_endpoints(config)?;
    validate_security(config)?;
    Ok(())
}

fn validate_listen(config: &Config) -> Result<()> {
    let listen = expand_bind_address(&config.listen_address)
        .parse::<std::net::SocketAddr>()
        .with_context(|| format!("invalid listen_address `{}`", config.listen_address))?;
    if listen.port() == 0 {
        bail!("listen_address must use a non-zero port");
    }
    // Fail closed on cleartext exposure: a non-loopback bind without TLS
    // lets anyone on the path read telemetry and bearer tokens (and
    // inject their own). Bearer auth alone does not satisfy this — the
    // token itself would travel in cleartext — so only a TLS identity or
    // the explicit isolated-network opt-in gets past here.
    if !is_loopback_listen(&config.listen_address) && config.tls.is_none() {
        if !config.allow_insecure_remote {
            bail!(
                "listen_address `{}` is non-loopback but no [tls] is configured; \
                 telemetry and bearer tokens would travel in cleartext. Configure [tls] \
                 (see `otel-sqlite gen-certs`) or set allow_insecure_remote = true only \
                 for an isolated network you trust",
                config.listen_address
            );
        }
        tracing::warn!(
            listen_address = config.listen_address.as_str(),
            "ingress binds a non-loopback address without TLS; traffic is cleartext",
        );
    }
    Ok(())
}

fn validate_queues_and_grpc(config: &Config) -> Result<()> {
    validate_range(
        "ingest_queue_capacity",
        config.ingest_queue_capacity,
        1,
        MAX_INGEST_QUEUE_CAPACITY,
    )?;
    validate_range(
        "command_queue_capacity",
        config.command_queue_capacity,
        1,
        MAX_INGEST_QUEUE_CAPACITY,
    )?;
    validate_range(
        "batcher_max_batch_records",
        config.batcher_max_batch_records,
        1,
        super::MAX_BATCH_RECORDS,
    )?;
    validate_range(
        "grpc_max_recv_msg_size",
        config.grpc_max_recv_msg_size,
        1024,
        MAX_GRPC_RECV_MSG_SIZE,
    )?;
    validate_range(
        "grpc_max_concurrent_streams",
        config.grpc_max_concurrent_streams,
        1,
        MAX_GRPC_CONCURRENT_STREAMS,
    )?;
    // Very large wire budgets multiply by the stream count into gigabytes
    // of worst-case inbound buffering; make sure operators raising them
    // past these marks see the cost in the logs (startup only).
    if config.grpc_max_recv_msg_size > 64 * 1024 * 1024 {
        tracing::warn!(
            grpc_max_recv_msg_size = config.grpc_max_recv_msg_size,
            grpc_max_concurrent_streams = config.grpc_max_concurrent_streams,
            "large grpc_max_recv_msg_size multiplies by the stream count into GiB-scale inbound buffering; raise host memory accordingly",
        );
    }
    validate_duration("batcher_max_batch_age", config.batcher_max_batch_age)?;
    validate_duration("shutdown_timeout", config.shutdown_timeout)?;
    Ok(())
}

fn validate_mapping_limits(config: &Config) -> Result<()> {
    validate_range(
        "max_records_per_request",
        config.max_records_per_request,
        1,
        MAX_RECORDS_PER_REQUEST,
    )?;
    validate_range(
        "max_attributes_per_record",
        config.max_attributes_per_record,
        1,
        MAX_ATTRIBUTES_PER_RECORD,
    )?;
    validate_range(
        "max_attribute_key_bytes",
        config.max_attribute_key_bytes,
        1,
        MAX_ATTRIBUTE_KEY_BYTES,
    )?;
    validate_range(
        "max_attribute_value_bytes",
        config.max_attribute_value_bytes,
        1,
        MAX_ATTRIBUTE_VALUE_BYTES,
    )?;
    validate_range("max_body_bytes", config.max_body_bytes, 1, MAX_BODY_BYTES)?;
    validate_range(
        "max_scope_metadata_expansion_bytes",
        config.max_scope_metadata_expansion_bytes,
        1024,
        MAX_SCOPE_METADATA_EXPANSION_BYTES,
    )?;
    Ok(())
}

fn validate_quota(config: &Config) -> Result<()> {
    if let Some(quota) = config.max_db_bytes
        && quota < MIN_DB_QUOTA_BYTES
    {
        bail!("max_db_bytes must be at least {MIN_DB_QUOTA_BYTES} bytes (1 MiB); got {quota}");
    }
    Ok(())
}

fn validate_maintenance(config: &Config) -> Result<()> {
    validate_duration("maintenance.retry_delay", config.maintenance.retry_delay)?;
    validate_optional_duration(
        "maintenance.retention",
        config.maintenance.retention,
        MAX_RETENTION,
    )?;
    validate_optional_duration(
        "maintenance.purge_interval",
        config.maintenance.purge_interval,
        MAX_TIMEOUT,
    )?;
    validate_optional_duration(
        "maintenance.checkpoint_interval",
        config.maintenance.checkpoint_interval,
        MAX_TIMEOUT,
    )?;
    validate_optional_duration(
        "maintenance.optimize_interval",
        config.maintenance.optimize_interval,
        MAX_RETENTION,
    )?;
    validate_optional_duration(
        "maintenance.vacuum_interval",
        config.maintenance.vacuum_interval,
        MAX_RETENTION,
    )?;
    validate_optional_duration(
        "maintenance.rebuild_fts_interval",
        config.maintenance.rebuild_fts_interval,
        MAX_RETENTION,
    )?;
    Ok(())
}

fn validate_watchdog(config: &Config) -> Result<()> {
    validate_duration("watchdog.tick_interval", config.watchdog.tick_interval)?;
    validate_duration("watchdog.degraded_after", config.watchdog.degraded_after)?;
    validate_duration("watchdog.unhealthy_after", config.watchdog.unhealthy_after)?;
    validate_duration("watchdog.ack_stall_after", config.watchdog.ack_stall_after)?;
    validate_duration(
        "watchdog.maintenance_stall_after",
        config.watchdog.maintenance_stall_after,
    )?;
    if config.watchdog.maintenance_stall_after < config.watchdog.unhealthy_after {
        bail!("watchdog.maintenance_stall_after must be at least unhealthy_after");
    }
    if config.watchdog.degraded_after >= config.watchdog.unhealthy_after {
        bail!("watchdog.degraded_after must be less than unhealthy_after");
    }
    if !config.watchdog.queue_pressure_ratio.is_finite()
        || !(0.0..=1.0).contains(&config.watchdog.queue_pressure_ratio)
        || config.watchdog.queue_pressure_ratio == 0.0
    {
        bail!("watchdog.queue_pressure_ratio must be finite and in the range (0, 1]");
    }
    Ok(())
}

fn validate_endpoints(config: &Config) -> Result<()> {
    if let Some(address) = &config.metrics_address {
        let parsed = expand_bind_address(address)
            .parse::<std::net::SocketAddr>()
            .with_context(|| format!("invalid metrics_address `{address}`"))?;
        if parsed.port() == 0 {
            bail!("metrics_address must use a non-zero port");
        }
        if !parsed.ip().is_loopback() && !config.allow_remote_metrics {
            bail!(
                "metrics_address `{address}` is non-loopback; set allow_remote_metrics = true only when a protected metrics exposure is intended"
            );
        }
    }
    Ok(())
}

fn validate_security(config: &Config) -> Result<()> {
    if let Some(tls) = &config.tls {
        readable_file("tls.cert", &tls.cert)?;
        readable_file("tls.key", &tls.key)?;
        if let Some(ca) = &tls.client_ca {
            readable_file("tls.client_ca", ca)?;
        }
    }
    if let Some(auth) = &config.auth {
        readable_file("auth.token_file", &auth.token_file)?;
    }
    Ok(())
}

pub(crate) fn validate_range<T>(key: &str, value: T, minimum: T, maximum: T) -> Result<()>
where
    T: Copy + Ord + std::fmt::Display,
{
    if value < minimum || value > maximum {
        bail!("config key `{key}` must be in the range {minimum}..={maximum}; got {value}");
    }
    Ok(())
}

pub(crate) fn validate_duration(key: &str, value: Duration) -> Result<()> {
    if value.is_zero() || value > MAX_TIMEOUT {
        bail!(
            "config key `{key}` must be greater than 0 and no more than {MAX_TIMEOUT:?}; got {value:?}"
        );
    }
    Ok(())
}

pub(crate) fn validate_optional_duration(
    key: &str,
    value: Option<Duration>,
    maximum: Duration,
) -> Result<()> {
    if value.is_some_and(|value| value.is_zero() || value > maximum) {
        bail!(
            "config key `{key}` must be greater than 0 and no more than {maximum:?}; got {value:?}"
        );
    }
    Ok(())
}

pub(crate) fn readable_file(key: &str, path: &Path) -> Result<()> {
    let metadata = std::fs::metadata(path).with_context(|| {
        format!(
            "config key `{key}` must point to a readable file: {}",
            path.display()
        )
    })?;
    if !metadata.is_file() {
        bail!(
            "config key `{key}` must point to a readable file: {} is not a regular file",
            path.display()
        );
    }
    std::fs::File::open(path).with_context(|| {
        format!(
            "config key `{key}` must point to a readable file: {}",
            path.display()
        )
    })?;
    // Private keys and bearer tokens must not be world-readable; public certs
    // are fine to skip. Warn (don't fail) to avoid breaking existing setups.
    if matches!(key, "tls.key" | "auth.token_file") {
        warn_if_world_readable(path, key);
    }
    Ok(())
}
