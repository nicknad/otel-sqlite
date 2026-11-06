//! Process-level tests for `otel-sqlite check`.
//!
//! The preflight must answer "could this config + binary + database start
//! cleanly?" while remaining side-effect free: no listener, no workers, and no
//! database/WAL mutation. These tests drive the real binary and compare the
//! database bytes before and after the check.

// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use otel_sqlite_storage::{latest_schema_version, migrate_up_to};
use rusqlite::Connection;

fn binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_otel-sqlite"))
}

fn write_config(dir: &Path, sqlite_path: &Path, extra: &str) -> PathBuf {
    let config = dir.join("otel-sqlite.toml");
    let sqlite_path = sqlite_path.display().to_string();
    std::fs::write(&config, format!("sqlite_path = {sqlite_path:?}\n{extra}"))
        .expect("write config");
    config
}

fn run_check(config: Option<&Path>, extra: &[&str]) -> Output {
    let mut command = Command::new(binary_path());
    command
        .arg("check")
        .env_remove("OTEL_SQLITE_CFG_PATH")
        .env_remove("otel-sqlite-cfg-path");
    if let Some(config) = config {
        command.arg("--config").arg(config);
    }
    command.args(extra);
    command.output().expect("run otel-sqlite check")
}

fn json(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&stdout).unwrap_or_else(|error| {
        panic!(
            "check output is not JSON ({error}):\nstdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn assert_no_sidecars(db: &Path) {
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = db.as_os_str().to_os_string();
        sidecar.push(suffix);
        assert!(
            !Path::new(&sidecar).exists(),
            "check must not create {}",
            Path::new(&sidecar).display()
        );
    }
}

#[test]
fn check_succeeds_for_a_missing_database_without_creating_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("logs.db");
    let config = write_config(dir.path(), &db, "");

    let output = run_check(Some(&config), &["--json"]);

    assert!(
        output.status.success(),
        "a fresh database path should pass; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = json(&output);
    assert_eq!(report["ok"], true);
    assert_eq!(report["config_source"], "file");
    assert_eq!(report["database"]["state"], "missing");
    assert_eq!(report["tls"]["configured"], false);
    assert!(!db.exists(), "check must not create the database");
    assert_no_sidecars(&db);
}

#[test]
fn check_fails_for_a_newer_schema_and_leaves_the_database_untouched() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("logs.db");
    {
        let mut conn = Connection::open(&db).expect("open database");
        migrate_up_to(&mut conn, latest_schema_version()).expect("build current schema");
        conn.execute(
            "INSERT INTO schema_migrations (version, description) VALUES ('999', 'future')",
            [],
        )
        .expect("stamp future version");
    }
    let before = std::fs::read(&db).expect("read before");
    let config = write_config(dir.path(), &db, "");

    let output = run_check(Some(&config), &["--json"]);

    assert_eq!(output.status.code(), Some(1), "newer schema must fail");
    let report = json(&output);
    assert_eq!(report["ok"], false);
    assert_eq!(report["database"]["state"], "newer");
    assert!(
        report["errors"]
            .as_array()
            .expect("errors array")
            .iter()
            .any(|error| error
                .as_str()
                .unwrap_or_default()
                .contains("upgrade this binary")),
        "expected the downgrade message, got {report}"
    );

    let after = std::fs::read(&db).expect("read after");
    assert_eq!(before, after, "check must not modify the database");
    assert_no_sidecars(&db);
}

#[test]
fn check_fails_for_invalid_tls_pem() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("logs.db");
    let cert = dir.path().join("server.pem");
    let key = dir.path().join("server.key");
    std::fs::write(&cert, b"not a certificate").expect("write cert");
    std::fs::write(&key, b"not a key").expect("write key");
    let extra = format!(
        "\n[tls]\ncert = {:?}\nkey = {:?}\n",
        cert.display().to_string(),
        key.display().to_string()
    );
    let config = write_config(dir.path(), &db, &extra);

    let output = run_check(Some(&config), &["--json"]);

    assert_eq!(output.status.code(), Some(1), "invalid PEM must fail");
    let report = json(&output);
    assert_eq!(report["tls"]["configured"], true);
    assert_eq!(report["tls"]["valid"], false);
    assert!(
        report["errors"]
            .as_array()
            .expect("errors array")
            .iter()
            .any(|error| error
                .as_str()
                .unwrap_or_default()
                .contains("TLS material is invalid")),
        "expected a TLS error, got {report}"
    );
    assert!(!db.exists(), "check must not create the database");
}

#[test]
fn check_rejects_unknown_flags_with_exit_code_two() {
    let output = run_check(None, &["--unknown"]);

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unknown check flag"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn check_help_prints_usage_and_exits_zero() {
    let output = run_check(None, &["--help"]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("otel-sqlite check"), "stdout: {stdout}");
    assert!(stdout.contains("--config"), "stdout: {stdout}");
}
