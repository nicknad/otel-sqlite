use std::path::{Path, PathBuf};
use std::time::Duration;

use std::sync::atomic::{AtomicU64, Ordering};

use otel_sqlite_core::storage::{CheckpointMode, DurabilityMode, SyncMode};
use otel_sqlite_ingress::{
    AuthConfig, DEFAULT_LISTEN_ADDRESS, DEFAULT_MAX_CONCURRENT_STREAMS,
    DEFAULT_MAX_RECORDS_PER_REQUEST, DEFAULT_SHUTDOWN_TIMEOUT, MAX_ATTRIBUTE_KEY_BYTES,
    MAX_ATTRIBUTE_VALUE_BYTES, MAX_ATTRIBUTES_PER_RECORD, MAX_BODY_BYTES, TlsConfig,
};
use otel_sqlite_runtime::WatchdogConfig;

use super::*;

fn file_config(toml_text: &str) -> FileConfig {
    toml::from_str(toml_text).expect("valid config TOML")
}

fn temp_config_path(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "otel-sqlite-config-{tag}-{}-{unique}.toml",
        std::process::id()
    ))
}

#[test]
fn empty_overlay_keeps_defaults() {
    let mut config = Config::default();
    file_config("").apply_to(&mut config).expect("applies");
    assert_eq!(config.listen_address, DEFAULT_LISTEN_ADDRESS);
    assert!(
        is_loopback_listen(&config.listen_address),
        "default listen_address must be loopback-only (H1), got {}",
        config.listen_address
    );
    assert_eq!(config.sqlite_path, PathBuf::from("otel-logs.db"));
    assert_eq!(config.ingest_queue_capacity, DEFAULT_INGEST_QUEUE_CAPACITY);
    assert_eq!(
        config.command_queue_capacity,
        otel_sqlite_storage::DEFAULT_COMMAND_QUEUE_CAPACITY
    );
    assert_eq!(config.batcher_max_batch_age, Duration::from_millis(10));
    assert_eq!(
        config.grpc_max_concurrent_streams,
        DEFAULT_MAX_CONCURRENT_STREAMS
    );
    assert_eq!(
        config.max_records_per_request,
        DEFAULT_MAX_RECORDS_PER_REQUEST
    );
    assert_eq!(config.shutdown_timeout, DEFAULT_SHUTDOWN_TIMEOUT);
    assert_eq!(
        config.ingress_config().max_records_per_request,
        DEFAULT_MAX_RECORDS_PER_REQUEST
    );
    config.validate().expect("built-in defaults are valid");
}

#[test]
fn top_level_keys_override_defaults() {
    let mut config = Config::default();
    file_config(
        r#"
        listen_address = "[::1]:4317"
        sqlite_path = "data/logs.db"
        ingest_queue_capacity = 10
        command_queue_capacity = 20
        batcher_max_batch_records = 42
        batcher_max_batch_age = "250ms"
         grpc_max_recv_msg_size = 1024
         grpc_max_concurrent_streams = 8
         max_records_per_request = 777
         max_attributes_per_record = 111
         max_attribute_key_bytes = 222
         max_attribute_value_bytes = 333
         max_body_bytes = 444
         max_scope_metadata_expansion_bytes = 8192
         shutdown_timeout = 90
        "#,
    )
    .apply_to(&mut config)
    .expect("applies");
    assert_eq!(config.listen_address, "[::1]:4317");
    assert_eq!(config.sqlite_path, PathBuf::from("data/logs.db"));
    assert_eq!(config.ingest_queue_capacity, 10);
    assert_eq!(config.command_queue_capacity, 20);
    assert_eq!(config.batcher_max_batch_records, 42);
    assert_eq!(config.batcher_max_batch_age, Duration::from_millis(250));
    assert_eq!(config.grpc_max_recv_msg_size, 1024);
    assert_eq!(config.grpc_max_concurrent_streams, 8);
    assert_eq!(config.max_records_per_request, 777);
    assert_eq!(config.max_attributes_per_record, 111);
    assert_eq!(config.max_attribute_key_bytes, 222);
    assert_eq!(config.max_attribute_value_bytes, 333);
    assert_eq!(config.max_body_bytes, 444);
    assert_eq!(config.max_scope_metadata_expansion_bytes, 8192);
    assert_eq!(config.shutdown_timeout, Duration::from_secs(90));
}

#[test]
fn size_quota_parses_validates_and_reaches_storage() {
    let mut config = Config::default();
    assert_eq!(config.max_db_bytes, None, "unbounded by default");
    file_config("max_db_bytes = 1073741824\n")
        .apply_to(&mut config)
        .expect("applies");
    assert_eq!(config.max_db_bytes, Some(1_073_741_824));
    config.validate().expect("1 GiB quota is valid");
    assert_eq!(config.storage_config().max_db_bytes, Some(1_073_741_824));
    let dump = config.dump_json().expect("dumps");
    assert!(dump.contains("max_db_bytes"), "dump must expose the quota");

    // Below 1 MiB the writer would evict on nearly every batch: reject.
    let tiny = Config {
        max_db_bytes: Some(1024),
        ..Config::default()
    };
    assert!(tiny.validate().is_err(), "sub-MiB quotas must be rejected");
}

#[test]
fn duration_formats_are_accepted() {
    let mut config = Config::default();
    file_config(
        r#"
        batcher_max_batch_age = "250ms"
        shutdown_timeout = "1h"
        [watchdog]
        tick_interval = "2m"
        unhealthy_after = "1d"
        "#,
    )
    .apply_to(&mut config)
    .expect("applies");
    assert_eq!(config.batcher_max_batch_age, Duration::from_millis(250));
    assert_eq!(config.shutdown_timeout, Duration::from_secs(3_600));
    assert_eq!(config.watchdog.tick_interval, Duration::from_secs(120));
    assert_eq!(config.watchdog.unhealthy_after, Duration::from_secs(86_400));
}

#[test]
fn nested_sections_merge_field_wise() {
    let mut config = Config::default();
    file_config(
        r#"
        [maintenance]
        checkpoint_mode = "TRUNCATE"
        retention = "168h"
        vacuum_interval = "off"

        [watchdog]
        queue_pressure_ratio = 0.9
        halt_on_unhealthy = false
        "#,
    )
    .apply_to(&mut config)
    .expect("applies");

    assert_eq!(config.maintenance.checkpoint_mode, CheckpointMode::Truncate);
    assert_eq!(
        config.maintenance.retention,
        Some(Duration::from_secs(7 * 24 * 3_600))
    );
    assert_eq!(config.maintenance.vacuum_interval, None);
    // Untouched siblings keep their defaults.
    assert_eq!(
        config.maintenance.purge_interval,
        Some(Duration::from_secs(15 * 60))
    );
    assert_eq!(
        config.maintenance.checkpoint_interval,
        Some(Duration::from_secs(5 * 60))
    );
    assert!((config.watchdog.queue_pressure_ratio - 0.9).abs() < f32::EPSILON);
    assert!(!config.watchdog.halt_on_unhealthy);
    assert_eq!(config.watchdog.tick_interval, Duration::from_secs(5));
}

#[test]
fn retention_accepts_uniform_and_table_forms() {
    // Uniform: one window (also accepts bare seconds).
    let mut config = Config::default();
    file_config("[maintenance]\nretention = 60")
        .apply_to(&mut config)
        .expect("applies");
    assert_eq!(config.maintenance.retention, Some(Duration::from_secs(60)));

    // Table form with the explicit `logs` key.
    let mut config = Config::default();
    file_config("[maintenance.retention]\nlogs = \"7d\"")
        .apply_to(&mut config)
        .expect("applies");
    assert_eq!(
        config.maintenance.retention,
        Some(Duration::from_secs(7 * 86_400))
    );

    // Explicit opt-out disables purging.
    let mut config = Config::default();
    file_config("[maintenance]\nretention = \"off\"")
        .apply_to(&mut config)
        .expect("applies");
    assert_eq!(config.maintenance.retention, None);

    // Inline-table form is equivalent to the section form.
    let mut config = Config::default();
    file_config("[maintenance]\nretention = { logs = \"1d\" }")
        .apply_to(&mut config)
        .expect("applies");
    assert_eq!(
        config.maintenance.retention,
        Some(Duration::from_secs(86_400))
    );

    // The retired metrics key fails with a targeted error.
    let file = toml::from_str::<FileConfig>("[maintenance.retention]\nmetrics = \"24h\"")
        .expect("raw TOML parses into the file mirror");
    let mut config = Config::default();
    let error = file
        .apply_to(&mut config)
        .expect_err("metrics retention must be rejected");
    assert!(
        error
            .to_string()
            .contains("OTLP metrics signal was removed"),
        "error must explain the removal: {error}"
    );
}

#[test]
fn retention_rejects_unknown_signal_keys() {
    // A typo inside the per-signal table must fail applying the config
    // instead of being silently ignored (which would keep the signal
    // unpruned).
    let file = toml::from_str::<FileConfig>("[maintenance.retention]\nmetris = \"24h\"")
        .expect("raw TOML parses into the file mirror");
    let mut config = Config::default();
    let error = file
        .apply_to(&mut config)
        .expect_err("unknown per-signal retention key must be rejected");
    assert!(
        error.to_string().contains("metris"),
        "error must name the unknown key: {error}"
    );
}

#[test]
fn rebuild_fts_scheduling_is_opt_in() {
    let mut config = Config::default();
    file_config("").apply_to(&mut config).expect("applies");
    assert_eq!(config.maintenance.rebuild_fts_interval, None);

    file_config("[maintenance]\nrebuild_fts_interval = \"7d\"")
        .apply_to(&mut config)
        .expect("applies");
    assert_eq!(
        config.maintenance.rebuild_fts_interval,
        Some(Duration::from_secs(7 * 86_400))
    );
}

#[test]
fn mandatory_duration_rejects_off() {
    let mut config = Config::default();
    let result = file_config("shutdown_timeout = \"off\"").apply_to(&mut config);
    assert!(result.is_err());
}

#[test]
fn durability_section_defaults_to_commit_and_normal_sync() {
    let mut config = Config::default();
    file_config("").apply_to(&mut config).expect("applies");
    assert_eq!(config.durability_mode, DurabilityMode::Commit);
    assert_eq!(config.sqlite_synchronous, SyncMode::Normal);

    file_config(
        r#"
        [durability]
        mode = "enqueue"
        synchronous = "full"
        "#,
    )
    .apply_to(&mut config)
    .expect("applies");
    assert_eq!(config.durability_mode, DurabilityMode::Enqueue);
    assert_eq!(config.sqlite_synchronous, SyncMode::Full);
    // The knobs flow through to the per-crate configs.
    assert_eq!(
        config.ingress_config().durability_mode,
        DurabilityMode::Enqueue
    );
    assert_eq!(config.storage_config().synchronous, SyncMode::Full);
}

#[test]
fn durability_section_is_case_insensitive_and_rejects_unknown_values() {
    let mut config = Config::default();
    file_config("[durability]\nmode = \"Commit\"")
        .apply_to(&mut config)
        .expect("applies");
    assert_eq!(config.durability_mode, DurabilityMode::Commit);

    let mut config = Config::default();
    let result = file_config("[durability]\nmode = \"durable\"").apply_to(&mut config);
    assert!(result.is_err());

    let mut config = Config::default();
    let result = file_config("[durability]\nsynchronous = \"off\"").apply_to(&mut config);
    assert!(result.is_err());
}

#[test]
fn tls_and_auth_sections_parse_and_validate() {
    let mut config = Config::default();
    file_config("").apply_to(&mut config).expect("applies");
    assert!(config.tls.is_none());
    assert!(config.auth.is_none());

    // mTLS + token auth together.
    file_config(
        r#"
        [tls]
        cert = "/certs/server.pem"
        key = "/certs/server.key"
        client_ca = "/certs/ca.pem"

        [auth]
        mode = "token"
        token_file = "/etc/otel.txt"
        "#,
    )
    .apply_to(&mut config)
    .expect("applies");
    let tls = config.tls.as_ref().expect("tls set");
    assert_eq!(tls.cert, PathBuf::from("/certs/server.pem"));
    assert_eq!(tls.client_ca, Some(PathBuf::from("/certs/ca.pem")));
    let auth = config.auth.as_ref().expect("auth set");
    assert_eq!(auth.token_file, PathBuf::from("/etc/otel.txt"));

    // Server TLS without mTLS is valid too.
    let mut config = Config::default();
    file_config("[tls]\ncert = \"s.pem\"\nkey = \"s.key\"")
        .apply_to(&mut config)
        .expect("applies");
    assert!(config.tls.as_ref().expect("tls").client_ca.is_none());

    // [tls] without cert/key is a config error.
    for incomplete in ["[tls]\nkey = \"k\"", "[tls]\ncert = \"c\""] {
        let mut config = Config::default();
        assert!(
            file_config(incomplete).apply_to(&mut config).is_err(),
            "incomplete [tls] must be rejected: {incomplete}"
        );
    }

    // mode=token requires token_file; none + token_file contradicts;
    // unknown modes are rejected.
    let mut config = Config::default();
    assert!(
        file_config("[auth]\nmode = \"token\"")
            .apply_to(&mut config)
            .is_err()
    );
    let mut config = Config::default();
    assert!(
        file_config("[auth]\ntoken_file = \"t\"")
            .apply_to(&mut config)
            .is_err(),
        "\"none\" with token_file must be rejected"
    );
    let mut config = Config::default();
    assert!(
        file_config("[auth]\nmode = \"oauth\"")
            .apply_to(&mut config)
            .is_err()
    );
}

#[test]
fn metrics_address_is_disabled_by_default_and_can_be_enabled() {
    let mut config = Config::default();
    file_config("").apply_to(&mut config).expect("applies");
    assert_eq!(config.metrics_address, None, "metrics are off by default");

    file_config("metrics_address = \"0.0.0.0:9100\"\nallow_remote_metrics = true")
        .apply_to(&mut config)
        .expect("applies");
    assert_eq!(config.metrics_address.as_deref(), Some("0.0.0.0:9100"));

    // Bare ":port" is valid and binds all interfaces.
    file_config("metrics_address = \":9464\"")
        .apply_to(&mut config)
        .expect("applies");
    assert_eq!(config.metrics_address.as_deref(), Some(":9464"));

    for off in ["off", "disabled", ""] {
        let mut config = Config::default();
        let toml_text = if off.is_empty() {
            "metrics_address = \"\"".to_owned()
        } else {
            format!("metrics_address = \"{off}\"")
        };
        file_config(&toml_text)
            .apply_to(&mut config)
            .expect("applies");
        assert_eq!(config.metrics_address, None, "value {off:?} disables");
    }

    let mut config = Config::default();
    let result = file_config("metrics_address = \"not-an-address\"").apply_to(&mut config);
    assert!(result.is_err(), "invalid address must be rejected");
}

#[test]
fn validation_rejects_unsafe_or_out_of_range_values() {
    macro_rules! assert_invalid {
        ($key:literal, $config:expr) => {
            let error = $config.validate().expect_err("invalid config must fail");
            assert!(error.to_string().contains($key), "{}: {error}", $key);
        };
    }
    assert_invalid!(
        "listen_address",
        Config {
            listen_address: ":0".to_owned(),
            ..Config::default()
        }
    );
    assert_invalid!(
        "ingest_queue_capacity",
        Config {
            ingest_queue_capacity: 0,
            ..Config::default()
        }
    );
    assert_invalid!(
        "command_queue_capacity",
        Config {
            command_queue_capacity: MAX_INGEST_QUEUE_CAPACITY + 1,
            ..Config::default()
        }
    );
    assert_invalid!(
        "batcher_max_batch_records",
        Config {
            batcher_max_batch_records: 0,
            ..Config::default()
        }
    );
    assert_invalid!(
        "grpc_max_recv_msg_size",
        Config {
            grpc_max_recv_msg_size: 512,
            ..Config::default()
        }
    );
    assert_invalid!(
        "grpc_max_concurrent_streams",
        Config {
            grpc_max_concurrent_streams: 0,
            ..Config::default()
        }
    );
    assert_invalid!(
        "grpc_max_concurrent_streams",
        Config {
            grpc_max_concurrent_streams: MAX_GRPC_CONCURRENT_STREAMS + 1,
            ..Config::default()
        }
    );
    assert_invalid!(
        "max_records_per_request",
        Config {
            max_records_per_request: MAX_RECORDS_PER_REQUEST + 1,
            ..Config::default()
        }
    );
    assert_invalid!(
        "max_attributes_per_record",
        Config {
            max_attributes_per_record: MAX_ATTRIBUTES_PER_RECORD + 1,
            ..Config::default()
        }
    );
    assert_invalid!(
        "max_attribute_key_bytes",
        Config {
            max_attribute_key_bytes: MAX_ATTRIBUTE_KEY_BYTES + 1,
            ..Config::default()
        }
    );
    assert_invalid!(
        "max_attribute_value_bytes",
        Config {
            max_attribute_value_bytes: MAX_ATTRIBUTE_VALUE_BYTES + 1,
            ..Config::default()
        }
    );
    assert_invalid!(
        "max_body_bytes",
        Config {
            max_body_bytes: MAX_BODY_BYTES + 1,
            ..Config::default()
        }
    );
    assert_invalid!(
        "shutdown_timeout",
        Config {
            shutdown_timeout: Duration::ZERO,
            ..Config::default()
        }
    );

    assert_invalid!(
        "allow_remote_metrics",
        Config {
            metrics_address: Some("0.0.0.0:8888".to_owned()),
            ..Config::default()
        }
    );

    assert!(
        Config {
            watchdog: WatchdogConfig {
                queue_pressure_ratio: f32::NAN,
                ..WatchdogConfig::default()
            },
            ..Config::default()
        }
        .validate()
        .is_err()
    );

    let watchdog = WatchdogConfig::default();
    assert!(
        Config {
            watchdog: WatchdogConfig {
                degraded_after: watchdog.unhealthy_after,
                ..watchdog
            },
            ..Config::default()
        }
        .validate()
        .is_err()
    );
}

#[test]
fn validation_requires_readable_tls_and_auth_files() {
    let dir = tempfile::tempdir().expect("temp directory");
    let cert = dir.path().join("server.pem");
    let key = dir.path().join("server.key");
    let ca = dir.path().join("ca.pem");
    let token = dir.path().join("tokens.txt");
    for path in [&cert, &key, &ca, &token] {
        std::fs::write(path, "test").expect("write test file");
    }

    let mut config = Config {
        tls: Some(TlsConfig {
            cert: cert.clone(),
            key: key.clone(),
            client_ca: Some(ca),
        }),
        auth: Some(AuthConfig { token_file: token }),
        ..Config::default()
    };
    config
        .validate()
        .expect("readable security files are valid");

    config.tls.as_mut().expect("tls").key = dir.path().join("missing.key");
    let error = config.validate().expect_err("missing TLS key must fail");
    assert!(error.to_string().contains("tls.key"));
}

#[test]
fn validation_rejects_directories_as_security_files() {
    let dir = tempfile::tempdir().expect("temp directory");
    let cert = dir.path().join("server.pem");
    std::fs::write(&cert, "test").expect("write test file");
    // On unix `File::open` happily opens a directory; validation must
    // reject it because a directory is not usable certificate material.
    let config = Config {
        tls: Some(TlsConfig {
            cert,
            key: dir.path().to_path_buf(),
            client_ca: None,
        }),
        ..Config::default()
    };
    let error = config
        .validate()
        .expect_err("a directory must not pass as tls.key");
    assert!(
        error.to_string().contains("tls.key"),
        "unexpected error: {error}"
    );
}

#[test]
fn effective_dump_contains_runtime_values() {
    let config = Config {
        max_records_per_request: 321,
        metrics_address: None,
        ..Config::default()
    };
    let dump = config.dump_json().expect("serializes");
    assert!(dump.contains("\"max_records_per_request\": 321"));
    assert!(dump.contains("\"metrics_address\": null"));
    assert!(dump.contains("\"allow_remote_metrics\": false"));
    assert!(dump.contains("\"allow_insecure_remote\": false"));

    // Maintenance and watchdog are structured JSON, not Rust `Debug`
    // strings, so machine consumers can read individual knobs.
    let value: serde_json::Value = serde_json::from_str(&dump).expect("valid JSON dump");
    let maintenance = value
        .get("maintenance")
        .and_then(serde_json::Value::as_object)
        .expect("maintenance must be a JSON object");
    assert!(maintenance.contains_key("checkpoint_mode"));
    assert!(maintenance.contains_key("retention_ms"));
    let watchdog = value
        .get("watchdog")
        .and_then(serde_json::Value::as_object)
        .expect("watchdog must be a JSON object");
    assert!(watchdog.contains_key("maintenance_stall_after_ms"));
    assert!(watchdog.contains_key("halt_on_unhealthy"));
    assert!(!dump.contains("MaintenanceConfig"), "{dump}");
    assert!(!dump.contains("WatchdogConfig"), "{dump}");
}

#[test]
fn metrics_url_uses_a_usable_host() {
    assert_eq!(
        metrics_display_url(":8888"),
        "http://127.0.0.1:8888/metrics"
    );
    assert_eq!(
        metrics_display_url("0.0.0.0:9100"),
        "http://127.0.0.1:9100/metrics"
    );
    assert_eq!(
        metrics_display_url("[::]:8888"),
        "http://[::1]:8888/metrics"
    );
    assert_eq!(
        metrics_display_url("127.0.0.1:8888"),
        "http://127.0.0.1:8888/metrics"
    );
}

#[test]
fn non_loopback_listen_requires_tls_or_explicit_opt_in() {
    // Loopback binds stay usable without TLS (the default).
    Config {
        listen_address: "127.0.0.1:4317".to_owned(),
        ..Config::default()
    }
    .validate()
    .expect("loopback without TLS is valid");

    // A wildcard bind without TLS fails closed with a prescriptive error.
    let error = Config {
        listen_address: "0.0.0.0:4317".to_owned(),
        ..Config::default()
    }
    .validate()
    .expect_err("cleartext remote bind must fail");
    assert!(
        error.to_string().contains("allow_insecure_remote"),
        "unexpected error: {error}"
    );

    // The explicit isolated-network opt-in restores the old behaviour.
    Config {
        listen_address: "0.0.0.0:4317".to_owned(),
        allow_insecure_remote: true,
        ..Config::default()
    }
    .validate()
    .expect("explicit opt-in is valid");

    // A TLS identity satisfies the gate without the opt-in.
    let dir = tempfile::tempdir().expect("temp directory");
    let cert = dir.path().join("server.pem");
    let key = dir.path().join("server.key");
    std::fs::write(&cert, "test").expect("write test file");
    std::fs::write(&key, "test").expect("write test file");
    Config {
        listen_address: "0.0.0.0:4317".to_owned(),
        tls: Some(TlsConfig {
            cert,
            key,
            client_ca: None,
        }),
        ..Config::default()
    }
    .validate()
    .expect("remote bind with TLS is valid");
}

#[test]
fn loopback_classification_matches_bound_interface() {
    assert!(is_loopback_listen("127.0.0.1:4317"));
    assert!(is_loopback_listen("[::1]:4317"));
    // Bare ":port" and 0.0.0.0 bind every interface, not just loopback.
    assert!(!is_loopback_listen(":4317"));
    assert!(!is_loopback_listen("0.0.0.0:4317"));
    // Unparseable addresses never count as loopback.
    assert!(!is_loopback_listen("not-an-address"));
}

#[test]
fn unknown_keys_are_rejected() {
    assert!(toml::from_str::<FileConfig>("no_such_key = 1").is_err());
    assert!(toml::from_str::<FileConfig>("[watchdog]\nnope = true").is_err());
}

#[test]
fn malformed_values_are_rejected() {
    assert!(toml::from_str::<FileConfig>("shutdown_timeout = \"5 parsecs\"").is_err());
    assert!(toml::from_str::<FileConfig>("shutdown_timeout = \"abc\"").is_err());
    assert!(toml::from_str::<FileConfig>("shutdown_timeout = \"-30s\"").is_err());
    // Unknown checkpoint modes parse as plain strings; they are rejected
    // when the file is applied to the runtime configuration.
    let file = toml::from_str::<FileConfig>("[maintenance]\ncheckpoint_mode = \"aggressive\"")
        .expect("string field parses");
    let mut config = Config::default();
    assert!(file.apply_to(&mut config).is_err());
}

#[test]
fn from_file_overrides_defaults_and_keeps_rest() {
    let path = temp_config_path("e2e");
    std::fs::write(
        &path,
        r#"
        listen_address = ":1234"
        sqlite_path = "/tmp/otel.db"
        shutdown_timeout = "45s"
        allow_insecure_remote = true

        [maintenance]
        retention = "7d"
        "#,
    )
    .expect("write temp config");
    let config = Config::from_file(&path);
    let _ = std::fs::remove_file(&path);

    let config = config.expect("valid config file");
    assert_eq!(config.listen_address, ":1234");
    assert!(config.allow_insecure_remote);
    assert_eq!(config.sqlite_path, PathBuf::from("/tmp/otel.db"));
    assert_eq!(config.shutdown_timeout, Duration::from_secs(45));
    assert_eq!(
        config.maintenance.retention,
        Some(Duration::from_secs(7 * 86_400))
    );
    assert_eq!(config.ingest_queue_capacity, 50_000);
}

#[test]
fn from_file_missing_reports_the_path() {
    let result = Config::from_file(Path::new("definitely-missing-cfg.toml"));
    let error = result.expect_err("missing file must fail");
    assert!(error.to_string().contains("definitely-missing-cfg.toml"));
}
