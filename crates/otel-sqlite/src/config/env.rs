//! `OTEL_SQLITE_*` environment overlays for the effective configuration.
//!
//! Every setting the TOML file accepts also has an environment form: the
//! variable's suffix is the setting's name upper-cased, with section keys
//! flattened under a section prefix (`OTEL_SQLITE_TLS_CERT` feeds `tls.cert`).
//! Values keep their TOML shapes: strings raw (`:4317`, `"off"`), booleans
//! `true|false`, integers plain digits, durations with a unit (`"250ms"`,
//! `"10s"`) or bare seconds, floats like `0.5`.
//!
//! The overlay renders the recognized variables into a TOML document and feeds
//! it through the file overlay ([`FileConfig`]), so both paths share one key
//! table, one unknown-key discipline and one validation pass.
//!
//! Precedence: built-in defaults, then the file named by
//! [`CONFIG_PATH_ENV_VAR`](super::CONFIG_PATH_ENV_VAR), then the environment
//! — the environment wins, so a container can override its image's baked-in
//! config per setting without mounting anything.
//!
//! `OTEL_SQLITE_*` variables that are neither settings nor reserved for other
//! components are ignored and reported (see [`EnvOverrides::ignored`]); the
//! prefix is shared with non-config variables such as
//! `OTEL_SQLITE_HEALTHCHECK_ENDPOINT`, so a typo must not be fatal.

use anyhow::{Context, Result, bail};

use super::Config;
use super::file::FileConfig;

/// How one environment value is converted into its TOML form.
#[derive(Debug, Clone, Copy)]
enum EnvValueKind {
    /// Always a TOML string, even when it looks like a number:
    /// `OTEL_SQLITE_LISTEN_ADDRESS=:4317` must not become an integer.
    Str,
    /// `true` or `false`, case-insensitive.
    Bool,
    /// Plain decimal integer (negative values fail the setting's own bounds).
    Int,
    /// Plain decimal float.
    Float,
    /// Inferred: boolean, then integer, then float, else string. Used for
    /// durations, which accept a unit-suffixed string or bare seconds.
    Any,
}

/// One recognized `OTEL_SQLITE_*` setting: the variable suffix and the dotted
/// TOML key it feeds.
struct Mapping {
    suffix: &'static str,
    key: &'static str,
    kind: EnvValueKind,
}

macro_rules! mapping {
    ($suffix:literal, $key:literal, $kind:ident) => {
        Mapping {
            suffix: $suffix,
            key: $key,
            kind: EnvValueKind::$kind,
        }
    };
}

/// The environment prefix shared by config overlays and the reserved
/// non-config variables kept out of them.
const ENV_PREFIX: &str = "OTEL_SQLITE_";

/// Every recognized setting, grouped the way the TOML file is.
const MAPPINGS: &[Mapping] = &[
    mapping!("LISTEN_ADDRESS", "listen_address", Str),
    mapping!("GRPC_MAX_RECV_MSG_SIZE", "grpc_max_recv_msg_size", Int),
    mapping!(
        "GRPC_MAX_CONCURRENT_STREAMS",
        "grpc_max_concurrent_streams",
        Int
    ),
    mapping!("MAPPING_CONCURRENCY", "mapping_concurrency", Int),
    mapping!("MAX_RECORDS_PER_REQUEST", "max_records_per_request", Int),
    mapping!(
        "MAX_ATTRIBUTES_PER_RECORD",
        "max_attributes_per_record",
        Int
    ),
    mapping!("MAX_ATTRIBUTE_KEY_BYTES", "max_attribute_key_bytes", Int),
    mapping!(
        "MAX_ATTRIBUTE_VALUE_BYTES",
        "max_attribute_value_bytes",
        Int
    ),
    mapping!("MAX_BODY_BYTES", "max_body_bytes", Int),
    mapping!(
        "MAX_SCOPE_METADATA_EXPANSION_BYTES",
        "max_scope_metadata_expansion_bytes",
        Int
    ),
    mapping!("SHUTDOWN_TIMEOUT", "shutdown_timeout", Any),
    mapping!("SQLITE_PATH", "sqlite_path", Str),
    mapping!("COMMAND_QUEUE_CAPACITY", "command_queue_capacity", Int),
    mapping!(
        "BATCHER_MAX_BATCH_RECORDS",
        "batcher_max_batch_records",
        Int
    ),
    mapping!("BATCHER_MAX_BATCH_AGE", "batcher_max_batch_age", Any),
    mapping!("GROUP_COMMIT_MAX_RECORDS", "group_commit_max_records", Int),
    mapping!("MAX_DB_BYTES", "max_db_bytes", Int),
    mapping!("INGEST_QUEUE_CAPACITY", "ingest_queue_capacity", Int),
    mapping!("METRICS_ADDRESS", "metrics_address", Str),
    mapping!("ALLOW_REMOTE_METRICS", "allow_remote_metrics", Bool),
    mapping!("ALLOW_INSECURE_REMOTE", "allow_insecure_remote", Bool),
    mapping!("TLS_CERT", "tls.cert", Str),
    mapping!("TLS_KEY", "tls.key", Str),
    mapping!("TLS_CLIENT_CA", "tls.client_ca", Str),
    mapping!("AUTH_MODE", "auth.mode", Str),
    mapping!("AUTH_TOKEN_FILE", "auth.token_file", Str),
    mapping!("DURABILITY_MODE", "durability.mode", Str),
    mapping!("DURABILITY_SYNCHRONOUS", "durability.synchronous", Str),
    mapping!("SEARCH_MODE", "search.mode", Str),
    mapping!("MAINTENANCE_RETENTION", "maintenance.retention", Str),
    mapping!(
        "MAINTENANCE_PURGE_INTERVAL",
        "maintenance.purge_interval",
        Any
    ),
    mapping!(
        "MAINTENANCE_CHECKPOINT_INTERVAL",
        "maintenance.checkpoint_interval",
        Any
    ),
    mapping!(
        "MAINTENANCE_CHECKPOINT_MODE",
        "maintenance.checkpoint_mode",
        Str
    ),
    mapping!(
        "MAINTENANCE_OPTIMIZE_INTERVAL",
        "maintenance.optimize_interval",
        Any
    ),
    mapping!(
        "MAINTENANCE_VACUUM_INTERVAL",
        "maintenance.vacuum_interval",
        Any
    ),
    mapping!(
        "MAINTENANCE_REBUILD_FTS_INTERVAL",
        "maintenance.rebuild_fts_interval",
        Any
    ),
    mapping!("MAINTENANCE_RETRY_DELAY", "maintenance.retry_delay", Any),
    mapping!("WATCHDOG_TICK_INTERVAL", "watchdog.tick_interval", Any),
    mapping!("WATCHDOG_DEGRADED_AFTER", "watchdog.degraded_after", Any),
    mapping!("WATCHDOG_UNHEALTHY_AFTER", "watchdog.unhealthy_after", Any),
    mapping!("WATCHDOG_ACK_STALL_AFTER", "watchdog.ack_stall_after", Any),
    mapping!(
        "WATCHDOG_MAINTENANCE_STALL_AFTER",
        "watchdog.maintenance_stall_after",
        Any
    ),
    mapping!(
        "WATCHDOG_QUEUE_PRESSURE_RATIO",
        "watchdog.queue_pressure_ratio",
        Float
    ),
    mapping!(
        "WATCHDOG_HALT_ON_UNHEALTHY",
        "watchdog.halt_on_unhealthy",
        Bool
    ),
];

/// Reserved exact variable suffixes: `OTEL_SQLITE_`-prefixed variables that
/// other components own. They never reach the overlay and are not reported as
/// ignored.
const RESERVED_EXACT: &[&str] = &["CFG_PATH", "HEALTHCHECK_ENDPOINT", "BIN", "CRASH_BIN"];

/// Reserved variable suffix prefixes (fault-injection, e2e and bench knobs).
const RESERVED_PREFIXES: &[&str] = &["FAULT_", "E2E_", "BENCH_"];

/// Whether the suffix names a variable owned outside the config overlay.
fn is_reserved(suffix: &str) -> bool {
    RESERVED_EXACT.contains(&suffix)
        || RESERVED_PREFIXES
            .iter()
            .any(|prefix| suffix.starts_with(prefix))
}

/// Report of one environment-overlay application pass.
#[derive(Debug, Default)]
pub(crate) struct EnvOverrides {
    /// Environment variables applied as configuration overlays, in order.
    pub(crate) applied: Vec<String>,
    /// `OTEL_SQLITE_*` variables that are neither settings nor reserved
    /// names; the binary warns about each of them at startup.
    pub(crate) ignored: Vec<String>,
}

/// Applies the process environment's `OTEL_SQLITE_*` overlays onto `config`.
pub(crate) fn apply(config: &mut Config) -> Result<EnvOverrides> {
    apply_pairs(config, std::env::vars())
}

/// Applies one explicit set of `OTEL_SQLITE_*` variables onto `config`; the
/// production path passes [`std::env::vars`], tests pass literal pairs so no
/// global state is involved.
pub(crate) fn apply_pairs(
    config: &mut Config,
    vars: impl IntoIterator<Item = (String, String)>,
) -> Result<EnvOverrides> {
    let mut table = toml::Table::new();
    let mut report = EnvOverrides::default();
    for (name, raw) in vars {
        let Some(suffix) = name.strip_prefix(ENV_PREFIX) else {
            continue;
        };
        if is_reserved(suffix) {
            continue;
        }
        let Some(mapping) = MAPPINGS.iter().find(|mapping| mapping.suffix == suffix) else {
            report.ignored.push(name);
            continue;
        };
        let value = toml_value(&raw, &name, mapping.kind)?;
        insert_path(&mut table, mapping.key, value);
        report.applied.push(name);
    }
    if table.is_empty() {
        return Ok(report);
    }
    let text = toml::to_string(&toml::Value::Table(table))
        .context("failed to serialize environment overlays")?;
    let overlay: FileConfig = toml::from_str(&text)
        .with_context(|| format!("failed to parse environment overlays: {text}"))?;
    overlay
        .apply_to(config)
        .context("invalid environment overlays")?;
    Ok(report)
}

/// Converts one raw environment value into its typed TOML form, naming the
/// variable in every error so a bad value is diagnosable from the message.
fn toml_value(raw: &str, var: &str, kind: EnvValueKind) -> Result<toml::Value> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("{var} is empty; unset it instead of setting it to nothing");
    }
    match kind {
        EnvValueKind::Str => Ok(toml::Value::String(trimmed.to_owned())),
        EnvValueKind::Bool => match trimmed.to_ascii_lowercase().as_str() {
            "true" => Ok(toml::Value::Boolean(true)),
            "false" => Ok(toml::Value::Boolean(false)),
            _ => bail!("{var} expects true or false, got `{raw}`"),
        },
        EnvValueKind::Int => trimmed
            .parse::<i64>()
            .map(toml::Value::Integer)
            .with_context(|| format!("{var} expects an integer, got `{raw}`")),
        EnvValueKind::Float => trimmed
            .parse::<f64>()
            .map(toml::Value::Float)
            .with_context(|| format!("{var} expects a number, got `{raw}`")),
        EnvValueKind::Any => Ok(if let Ok(value) = trimmed.parse::<i64>() {
            toml::Value::Integer(value)
        } else if let Ok(value) = trimmed.parse::<f64>() {
            toml::Value::Float(value)
        } else if let Ok(value) = trimmed.parse::<bool>() {
            toml::Value::Boolean(value)
        } else {
            toml::Value::String(trimmed.to_owned())
        }),
    }
}

/// Inserts `value` at the dotted key `key`, creating the intermediate tables.
///
/// The mapping keys are static and disjoint, so a section name can never
/// collide with a previously inserted leaf; the expects state that invariant.
fn insert_path(root: &mut toml::Table, key: &str, value: toml::Value) {
    let parts: Vec<&str> = key.split('.').collect();
    let (leaf, path) = parts
        .split_last()
        .expect("mapping keys are nonempty dotted paths");
    let mut node = root;
    for part in path {
        let entry = node
            .entry((*part).to_owned())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        node = entry
            .as_table_mut()
            .expect("mapping keys never reuse a leaf as a section");
    }
    node.insert((*leaf).to_owned(), value);
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use otel_sqlite_core::storage::DurabilityMode;

    use super::apply_pairs;
    use crate::config::Config;

    /// Test stand-in for `std::env::vars` with literal pairs.
    fn vars(items: &'static [(&'static str, &'static str)]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn top_level_overlays_apply_and_validate() {
        let mut config = Config::default();
        let report = apply_pairs(
            &mut config,
            vars(&[
                ("OTEL_SQLITE_LISTEN_ADDRESS", ":4317"),
                ("OTEL_SQLITE_ALLOW_INSECURE_REMOTE", "true"),
                ("OTEL_SQLITE_SQLITE_PATH", "/data/test.db"),
            ]),
        )
        .expect("valid overlays must apply");
        assert_eq!(config.ingress.listen_address, ":4317");
        assert!(config.allow_insecure_remote);
        assert_eq!(config.storage.sqlite_path, PathBuf::from("/data/test.db"));
        assert_eq!(report.applied.len(), 3);
        assert_eq!(report.ignored, [] as [std::string::String; 0]);
        config
            .validate()
            .expect("wildcard bind with allow_insecure_remote must validate");
    }

    #[test]
    fn environment_wins_over_prior_values() {
        let mut config = Config::default();
        config.storage.sqlite_path = PathBuf::from("/file/value.db");
        apply_pairs(
            &mut config,
            vars(&[("OTEL_SQLITE_SQLITE_PATH", "/env/value.db")]),
        )
        .expect("valid overlays must apply");
        assert_eq!(config.storage.sqlite_path, PathBuf::from("/env/value.db"));
    }

    #[test]
    fn section_overlays_reach_their_configs() {
        let mut config = Config::default();
        apply_pairs(
            &mut config,
            vars(&[
                ("OTEL_SQLITE_TLS_CERT", "/tls/server.pem"),
                ("OTEL_SQLITE_TLS_KEY", "/tls/server.key"),
                ("OTEL_SQLITE_TLS_CLIENT_CA", "/tls/ca.pem"),
                ("OTEL_SQLITE_DURABILITY_MODE", "enqueue"),
                ("OTEL_SQLITE_SEARCH_MODE", "full"),
            ]),
        )
        .expect("valid section overlays must apply");
        let tls = config
            .ingress
            .tls
            .as_ref()
            .expect("[tls] must be configured");
        assert_eq!(tls.cert, PathBuf::from("/tls/server.pem"));
        assert_eq!(tls.key, PathBuf::from("/tls/server.key"));
        assert_eq!(
            tls.client_ca.as_deref(),
            Some(std::path::Path::new("/tls/ca.pem"))
        );
        assert_eq!(
            config.ingress.durability_mode,
            DurabilityMode::parse("enqueue").expect("valid mode")
        );
    }

    #[test]
    fn typed_values_reach_their_configs() {
        let mut config = Config::default();
        apply_pairs(
            &mut config,
            vars(&[
                ("OTEL_SQLITE_SHUTDOWN_TIMEOUT", "45s"),
                ("OTEL_SQLITE_MAX_DB_BYTES", "123"),
                ("OTEL_SQLITE_MAINTENANCE_RETENTION", "7d"),
                ("OTEL_SQLITE_WATCHDOG_QUEUE_PRESSURE_RATIO", "0.5"),
                ("OTEL_SQLITE_WATCHDOG_HALT_ON_UNHEALTHY", "true"),
                ("OTEL_SQLITE_METRICS_ADDRESS", "off"),
            ]),
        )
        .expect("valid typed overlays must apply");
        assert_eq!(config.ingress.shutdown_timeout, Duration::from_secs(45));
        assert_eq!(config.storage.max_db_bytes, Some(123));
        let purge = config
            .maintenance
            .purge
            .as_ref()
            .expect("retention enables purging");
        assert_eq!(purge.retention, Duration::from_secs(7 * 24 * 60 * 60));
        assert_eq!(config.watchdog.queue_pressure_ratio, 0.5);
        assert!(config.watchdog.halt_on_unhealthy);
        assert_eq!(config.metrics_address, None);
    }

    #[test]
    fn cleartex_exposure_gate_survives_overlays() {
        let mut config = Config::default();
        apply_pairs(
            &mut config,
            vars(&[("OTEL_SQLITE_LISTEN_ADDRESS", ":4317")]),
        )
        .expect("the overlay itself must apply");
        assert!(
            config.validate().is_err(),
            "non-loopback bind without [tls] or allow_insecure_remote must still fail"
        );
    }

    #[test]
    fn bad_bool_names_the_variable() {
        let mut config = Config::default();
        let error = apply_pairs(
            &mut config,
            vars(&[("OTEL_SQLITE_ALLOW_INSECURE_REMOTE", "maybe")]),
        )
        .expect_err("bad booleans must be rejected");
        assert!(
            error
                .to_string()
                .contains("OTEL_SQLITE_ALLOW_INSECURE_REMOTE")
        );
    }

    #[test]
    fn bad_integer_names_the_variable() {
        let mut config = Config::default();
        let error = apply_pairs(
            &mut config,
            vars(&[("OTEL_SQLITE_INGEST_QUEUE_CAPACITY", "many")]),
        )
        .expect_err("bad integers must be rejected");
        assert!(
            error
                .to_string()
                .contains("OTEL_SQLITE_INGEST_QUEUE_CAPACITY")
        );
    }

    #[test]
    fn empty_value_is_rejected() {
        let mut config = Config::default();
        let error = apply_pairs(&mut config, vars(&[("OTEL_SQLITE_LISTEN_ADDRESS", "  ")]))
            .expect_err("empty values must be rejected");
        assert!(error.to_string().contains("OTEL_SQLITE_LISTEN_ADDRESS"));
    }

    #[test]
    fn unknown_and_reserved_names_are_reported_not_applied() {
        let mut config = Config::default();
        let report = apply_pairs(
            &mut config,
            vars(&[
                ("OTEL_SQLITE_NOT_A_SETTING", "1"),
                ("OTEL_SQLITE_HEALTHCHECK_ENDPOINT", "http://127.0.0.1:4317"),
                ("OTEL_SQLITE_FAULT_WRITER_AFTER_N", "15"),
                ("SOMETHING_ELSE", "1"),
            ]),
        )
        .expect("reserved and unknown names must not be fatal");
        assert_eq!(report.applied, [] as [std::string::String; 0]);
        assert_eq!(report.ignored, ["OTEL_SQLITE_NOT_A_SETTING"]);
        assert_eq!(
            config.ingress.listen_address,
            Config::default().ingress.listen_address
        );
        assert!(!config.allow_insecure_remote);
        assert_eq!(
            config.storage.sqlite_path,
            Config::default().storage.sqlite_path
        );
    }
}
