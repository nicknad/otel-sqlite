use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use otel_sqlite_ingress::expand_bind_address;
use otel_sqlite_storage::{FtsMode, warn_if_world_readable};

use super::{Config, MAX_INGEST_QUEUE_CAPACITY, MAX_RETENTION, MAX_TIMEOUT, is_loopback_listen};

/// Validates the complete effective configuration before any listener or
/// worker is started.
///
/// Per-section defaults and invariants live with the owning crates —
/// [`otel_sqlite_ingress::IngressConfig::validate`] for the ingress section,
/// [`otel_sqlite_storage::StorageConfig::validate`] for storage,
/// [`otel_sqlite_runtime::MaintenanceConfig::validate`] and
/// [`otel_sqlite_runtime::WatchdogConfig::validate_batch_age`] for the
/// runtime workers. This module keeps only the cross-section gates: rules
/// that compare knobs from more than one section, plus the assembly knobs
/// and duration ceilings the CLI itself owns.
pub(crate) fn validate_config(config: &Config) -> Result<()> {
    config
        .ingress
        .validate()
        .context("invalid ingress configuration")?;
    config
        .storage
        .validate()
        .context("invalid storage configuration")?;
    validate_listen(config)?;
    validate_queues_and_durations(config)?;
    validate_maintenance(config)?;
    validate_watchdog(config)?;
    validate_endpoints(config)?;
    validate_security(config)?;
    Ok(())
}

/// Listen address checks and the cleartext-exposure gate: `listen_address`
/// and `[tls]` are ingress-owned, the opt-in is binary-owned, so the rule
/// itself belongs to the assembly.
fn validate_listen(config: &Config) -> Result<()> {
    let listen = expand_bind_address(&config.ingress.listen_address)
        .parse::<std::net::SocketAddr>()
        .with_context(|| format!("invalid listen_address `{}`", config.ingress.listen_address))?;
    if listen.port() == 0 {
        bail!("listen_address must use a non-zero port");
    }
    // Fail closed on cleartext exposure: a non-loopback bind without TLS
    // lets anyone on the path read telemetry and bearer tokens (and
    // inject their own). Bearer auth alone does not satisfy this — the
    // token itself would travel in cleartext — so only a TLS identity or
    // the explicit isolated-network opt-in gets past here.
    if !is_loopback_listen(&config.ingress.listen_address) && config.ingress.tls.is_none() {
        if !config.allow_insecure_remote {
            bail!(
                "listen_address `{}` is non-loopback but no [tls] is configured; \
                 telemetry and bearer tokens would travel in cleartext. Configure [tls] \
                 (see `otel-sqlite gen-certs`) or set allow_insecure_remote = true only \
                 for an isolated network you trust",
                config.ingress.listen_address
            );
        }
        tracing::warn!(
            listen_address = config.ingress.listen_address.as_str(),
            "ingress binds a non-loopback address without TLS; traffic is cleartext",
        );
    }
    Ok(())
}

/// Binary-owned assembly knobs plus the duration ceilings the CLI applies
/// across sections. Zero and nonsense values are already rejected by the
/// owning crates, so only the cross-section pieces live here.
fn validate_queues_and_durations(config: &Config) -> Result<()> {
    validate_range(
        "ingest_queue_capacity",
        config.ingest_queue_capacity,
        1,
        MAX_INGEST_QUEUE_CAPACITY,
    )?;
    // Cross-section advisory: a group-commit ceiling below the insert-batch
    // capacity cannot coalesce a full batch.
    if config.storage.group_commit_max_records > 0
        && config.storage.group_commit_max_records < config.storage.insert_batcher.max_batch_records
    {
        tracing::warn!(
            group_commit_max_records = config.storage.group_commit_max_records,
            batcher_max_batch_records = config.storage.insert_batcher.max_batch_records,
            "group_commit_max_records is below batcher_max_batch_records; full batches will not coalesce"
        );
    }
    // The owning crates reject zero for both of these; the CLI keeps only
    // the uniform ceiling.
    validate_duration_ceiling(
        "batcher_max_batch_age",
        config.storage.insert_batcher.max_batch_age,
        MAX_TIMEOUT,
    )?;
    validate_duration_ceiling(
        "shutdown_timeout",
        config.ingress.shutdown_timeout,
        MAX_TIMEOUT,
    )?;
    Ok(())
}

/// Maintenance schedule: runtime-owned zero rules (via delegation) plus the
/// CLI duration ceilings and the search-mode interplay.
fn validate_maintenance(config: &Config) -> Result<()> {
    // Runtime-owned invariants (non-zero retry delay and purge schedule).
    config
        .maintenance
        .validate()
        .context("invalid maintenance configuration")?;
    // CLI-specific upper bounds on the durations operators can express. The
    // purge cadence zero rule is runtime-owned; the optional intervals keep
    // their CLI-only zero rejection ("off" is the documented way to disable).
    validate_duration_ceiling(
        "maintenance.retry_delay",
        config.maintenance.retry_delay,
        MAX_TIMEOUT,
    )?;
    validate_optional_duration_ceiling(
        "maintenance.purge.retention",
        config.maintenance.purge.map(|schedule| schedule.retention),
        MAX_RETENTION,
    )?;
    validate_optional_duration_ceiling(
        "maintenance.purge.interval",
        config.maintenance.purge.map(|schedule| schedule.interval),
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
    // Cross-section by nature: the schedule knob is meaningless when the
    // storage-owned index mode disables maintenance of the index.
    if config.storage.fts_mode == FtsMode::Off && config.maintenance.rebuild_fts_interval.is_some()
    {
        tracing::warn!("search.mode = \"off\" makes maintenance.rebuild_fts_interval a no-op");
    }
    Ok(())
}

/// Watchdog policy: runtime-owned ordering and zero rules (via delegation)
/// plus the CLI duration ceilings and the batch-age interplay.
fn validate_watchdog(config: &Config) -> Result<()> {
    // The runtime clamps a small tick instead of rejecting it; the CLI keeps
    // the historical hard rejection so the TOML contract stays as strict.
    validate_duration("watchdog.tick_interval", config.watchdog.tick_interval)?;
    validate_duration_ceiling(
        "watchdog.degraded_after",
        config.watchdog.degraded_after,
        MAX_TIMEOUT,
    )?;
    validate_duration_ceiling(
        "watchdog.unhealthy_after",
        config.watchdog.unhealthy_after,
        MAX_TIMEOUT,
    )?;
    validate_duration_ceiling(
        "watchdog.ack_stall_after",
        config.watchdog.ack_stall_after,
        MAX_TIMEOUT,
    )?;
    validate_duration_ceiling(
        "watchdog.maintenance_stall_after",
        config.watchdog.maintenance_stall_after,
        MAX_TIMEOUT,
    )?;
    // Cross-section: the oldest buffered record must not outlive the first
    // health threshold, or a healthy low-volume pipeline would be halted.
    config
        .watchdog
        .validate_batch_age(config.storage.insert_batcher.max_batch_age)
        .context("invalid watchdog configuration")?;
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
    if let Some(tls) = &config.ingress.tls {
        readable_file("tls.cert", &tls.cert)?;
        readable_file("tls.key", &tls.key)?;
        if let Some(ca) = &tls.client_ca {
            readable_file("tls.client_ca", ca)?;
        }
    }
    if let Some(auth) = &config.ingress.auth {
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

/// Ceiling-only duration check: the owning crate already rejects zero and
/// nonsense values, so only the CLI-wide ceiling is checked here.
pub(crate) fn validate_duration_ceiling(
    key: &str,
    value: Duration,
    maximum: Duration,
) -> Result<()> {
    if value > maximum {
        bail!("config key `{key}` must be no more than {maximum:?}; got {value:?}");
    }
    Ok(())
}

/// Ceiling-only check for optional durations whose zero rule is owned
/// elsewhere (`PurgeSchedule` is runtime-owned and rejects zero itself).
pub(crate) fn validate_optional_duration_ceiling(
    key: &str,
    value: Option<Duration>,
    maximum: Duration,
) -> Result<()> {
    if value.is_some_and(|value| value > maximum) {
        bail!("config key `{key}` must be no more than {maximum:?}; got {value:?}");
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
