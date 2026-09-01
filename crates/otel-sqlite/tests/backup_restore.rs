//! Process-level backup/restore tests.
//!
//! Proves the backup contract against the REAL `otel-sqlite` binary: an online
//! `backup` snapshot is taken while the server is actively ingesting, the
//! server is stopped, the snapshot is `restore`d into a clean directory, and
//! the server restarts on the restored database — with row counts preserved
//! and new traffic persisting afterwards. An encrypted round-trip covers the
//! `--key-file` path end to end, and a corruption probe proves `verify` exits
//! non-zero on damaged data (the hook cron/systemd/Kubernetes alert on).
//!
//! The binary used is `env!("CARGO_BIN_EXE_otel-sqlite")` (the dev build of
//! THIS package) unless `OTEL_SQLITE_BIN` or `OTEL_SQLITE_CRASH_BIN` points at
//! another binary (the release build, as in the CI release gates).

// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use otel_sqlite_core::storage::SyncMode;
use otel_sqlite_e2e::crash::{CrashServerConfig, LoadParams, run_load, write_server_config};
use otel_sqlite_e2e::generator::WorkloadSpec;
use otel_sqlite_storage::backup;

/// The binary under test: `OTEL_SQLITE_BIN` (backup tests), then the
/// `OTEL_SQLITE_CRASH_BIN` override (shared with the crash suite in CI),
/// then this package's own dev binary.
fn binary_path() -> PathBuf {
    let override_path = std::env::var_os("OTEL_SQLITE_BIN")
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var_os("OTEL_SQLITE_CRASH_BIN").filter(|value| !value.is_empty()));
    match override_path {
        Some(path) => resolve_override(PathBuf::from(path)),
        None => PathBuf::from(env!("CARGO_BIN_EXE_otel-sqlite")),
    }
}

/// Cargo runs test binaries with the working directory at the package root,
/// while CI points `OTEL_SQLITE_CRASH_BIN` at a release build whose `target/`
/// lives at the workspace root. Resolve a relative override by searching
/// upward from the current directory for the first existing match (honoring
/// the `.exe` suffix Windows executables carry).
fn resolve_override(path: PathBuf) -> PathBuf {
    if path.is_absolute() || path.exists() {
        return path;
    }
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    for ancestor in cwd.ancestors() {
        let candidate = ancestor.join(&path);
        if candidate.exists() {
            return candidate;
        }
        #[cfg(windows)]
        {
            let exe = candidate.with_extension("exe");
            if exe.exists() {
                return exe;
            }
        }
    }
    path
}

/// A spawned `otel-sqlite` server under test. Kills the child on drop so a
/// failed assertion never leaks a lingering server process.
struct ServerProc {
    child: std::process::Child,
    endpoint: String,
    db_path: PathBuf,
    _stdout_path: PathBuf,
    _stderr_path: PathBuf,
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Deterministic workload shape shared by every backup scenario.
fn workload_spec(run_id: &str) -> WorkloadSpec {
    WorkloadSpec {
        run_id: run_id.to_owned(),
        seed: 7,
        body_size: 64,
        attributes_per_record: 2,
        resource_count: 2,
    }
}

/// Finds a free loopback port by binding and releasing a listener.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local address").port()
}

/// Polls until the endpoint accepts TCP connections, or fails after `timeout`.
fn wait_serving(endpoint: &str, timeout: Duration) {
    let host_port = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let started = std::time::Instant::now();
    loop {
        if std::net::TcpStream::connect(host_port).is_ok() {
            return;
        }
        assert!(
            started.elapsed() < timeout,
            "server at {endpoint} did not start listening within {timeout:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Spawns the real binary with the given config and waits until it serves.
fn spawn_server(config: &CrashServerConfig) -> ServerProc {
    let config_path = write_server_config(config);
    let stdout_path = config.dir.join("server.out.log");
    let stderr_path = config.dir.join("server.err.log");
    let stdout = std::fs::File::create(&stdout_path).expect("create server stdout log");
    let stderr = std::fs::File::create(&stderr_path).expect("create server stderr log");

    let child = std::process::Command::new(binary_path())
        .env("OTEL_SQLITE_CFG_PATH", &config_path)
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .expect("spawn otel-sqlite server");

    let endpoint = format!("http://127.0.0.1:{}", config.port);
    wait_serving(&endpoint, Duration::from_secs(30));
    ServerProc {
        child,
        endpoint,
        db_path: config.dir.join("otel-logs.db"),
        _stdout_path: stdout_path,
        _stderr_path: stderr_path,
    }
}

/// Abrupt kill (SIGKILL / TerminateProcess): matches the crash suite and
/// proves the restore does not depend on graceful shutdown.
fn kill_process(proc: &mut ServerProc) -> std::process::ExitStatus {
    let _ = proc.child.kill();
    let status = proc.child.wait().expect("reap killed server");
    assert!(!status.success(), "expected an abrupt exit, got {status:?}");
    status
}

/// Runs a `backup`/`restore`/`verify` subcommand against the real binary and
/// returns its output.
fn run_cli(args: &[&str]) -> std::process::Output {
    std::process::Command::new(binary_path())
        .args(args)
        .output()
        .expect("run otel-sqlite subcommand")
}

/// Full runbook flow against the external binary: online backup while load is
/// active, stop, restore into a clean directory, restart, validate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_restore_round_trip_against_the_external_binary() {
    let dir = tempfile::tempdir().expect("temp data dir");
    let base = dir.path();

    // Phase 1: boot the server and start closed-loop load.
    let config = CrashServerConfig::new(base.to_path_buf(), free_port(), SyncMode::Normal);
    let mut server = spawn_server(&config);

    let mut params = LoadParams::new("backup-a");
    params.duration = Duration::from_secs(6);
    let endpoint = server.endpoint.clone();
    let spec = workload_spec("backup-a");
    let load = tokio::spawn(async move { run_load(&endpoint, spec, &params).await });

    // Phase 2: take an online backup WHILE the pipeline is ingesting.
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let backup_dir = base.join("backups");
    let backup_out = run_cli(&[
        "backup",
        "--db",
        &server.db_path.display().to_string(),
        "--out",
        &backup_dir.display().to_string(),
        "--json",
    ]);
    assert!(
        backup_out.status.success(),
        "backup subcommand failed: {}",
        String::from_utf8_lossy(&backup_out.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&backup_out.stdout).expect("backup emits a JSON report");
    let snapshot_rows = report["verify"]["row_counts"]["log_events"]
        .as_i64()
        .expect("row count in backup report");
    assert!(
        snapshot_rows > 0,
        "online backup must contain rows committed before it started"
    );
    let snapshot_path = report["backup"].as_str().expect("backup path in report");
    assert!(
        Path::new(snapshot_path).exists(),
        "backup artifact must exist"
    );
    assert_eq!(report["verify"]["integrity"], "ok");

    // Phase 3: the load completes while the backup is on disk.
    let outcome = load
        .await
        .expect("load task joined")
        .expect("load completes");
    assert!(
        outcome.counters.records_accepted > 0,
        "load must accept records during the backup window"
    );

    // The live database only grew after the snapshot.
    let live = backup::verify(&server.db_path).expect("verify live database");
    assert!(
        live.row_counts.log_events >= snapshot_rows,
        "the live database must hold at least the snapshot row count"
    );

    // Phase 4: stop the server, restore into a clean directory.
    let _ = kill_process(&mut server);
    let clean_dir = base.join("restored");
    let restore_out = run_cli(&[
        "restore",
        "--backup",
        snapshot_path,
        "--dir",
        &clean_dir.display().to_string(),
        "--json",
    ]);
    assert!(
        restore_out.status.success(),
        "restore subcommand failed: {}",
        String::from_utf8_lossy(&restore_out.stderr)
    );
    let restore_report: serde_json::Value =
        serde_json::from_slice(&restore_out.stdout).expect("restore emits a JSON report");
    let restored_rows = restore_report["verified_after"]["row_counts"]["log_events"]
        .as_i64()
        .expect("row count in restore report");
    assert_eq!(
        restored_rows, snapshot_rows,
        "restore must contain exactly the snapshot rows"
    );
    assert_eq!(
        restore_report["verified_after"]["schema_version"], "003",
        "restored database must carry the current schema"
    );

    // Phase 5: `verify` agrees, then restart the server on the restored
    // database and prove new traffic persists.
    let verify_out = run_cli(&[
        "verify",
        "--db",
        &clean_dir.join("otel-logs.db").display().to_string(),
    ]);
    assert!(
        verify_out.status.success(),
        "verify subcommand must exit 0 on a healthy restored database: {}",
        String::from_utf8_lossy(&verify_out.stderr)
    );

    let restart = CrashServerConfig::new(clean_dir.clone(), free_port(), SyncMode::Normal);
    let mut server2 = spawn_server(&restart);
    let mut params_b = LoadParams::new("backup-b");
    params_b.duration = Duration::from_secs(3);
    let outcome_b = run_load(&server2.endpoint, workload_spec("backup-b"), &params_b)
        .await
        .expect("post-restore load");
    assert!(
        outcome_b.counters.records_accepted > 0,
        "the restored server must ingest new traffic"
    );
    let after_restart = backup::verify(&server2.db_path).expect("verify restarted database");
    assert_eq!(
        after_restart.row_counts.log_events,
        restored_rows + outcome_b.counters.records_accepted as i64,
        "rows written after restore must add to the restored base"
    );
    assert_eq!(after_restart.schema_version.as_deref(), Some("003"));

    let _ = kill_process(&mut server2);
}

/// Encrypted round-trip through the CLI: backup with `--key-file`, restore
/// with `--key-file`, then verify the restored database.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encrypted_backup_restore_against_the_external_binary() {
    let dir = tempfile::tempdir().expect("temp data dir");
    let base = dir.path();

    let config = CrashServerConfig::new(base.to_path_buf(), free_port(), SyncMode::Normal);
    let mut server = spawn_server(&config);

    let mut params = LoadParams::new("enc-backup");
    params.duration = Duration::from_secs(3);
    let outcome = run_load(&server.endpoint, workload_spec("enc-backup"), &params)
        .await
        .expect("load");
    assert!(outcome.counters.records_accepted > 0);

    let key_path = base.join("backup.key");
    std::fs::write(&key_path, [42u8; 32]).expect("write backup key");
    let backup_dir = base.join("backups");
    let backup_out = run_cli(&[
        "backup",
        "--db",
        &server.db_path.display().to_string(),
        "--out",
        &backup_dir.display().to_string(),
        "--key-file",
        &key_path.display().to_string(),
        "--json",
    ]);
    assert!(
        backup_out.status.success(),
        "encrypted backup failed: {}",
        String::from_utf8_lossy(&backup_out.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&backup_out.stdout).expect("backup JSON");
    assert_eq!(report["encrypted"], true);
    let snapshot_path = report["backup"].as_str().expect("backup path");
    assert!(
        otel_sqlite_storage::backup::looks_encrypted(Path::new(snapshot_path)).expect("read magic"),
        "encrypted artifact must carry the backup magic"
    );

    // Restore with the key into a clean directory.
    let _ = kill_process(&mut server);
    let clean_dir = base.join("restored");
    let restore_out = run_cli(&[
        "restore",
        "--backup",
        snapshot_path,
        "--dir",
        &clean_dir.display().to_string(),
        "--key-file",
        &key_path.display().to_string(),
        "--json",
    ]);
    assert!(
        restore_out.status.success(),
        "encrypted restore failed: {}",
        String::from_utf8_lossy(&restore_out.stderr)
    );
    let restore_report: serde_json::Value =
        serde_json::from_slice(&restore_out.stdout).expect("restore JSON");
    let snapshot_rows = report["verify"]["row_counts"]["log_events"]
        .as_i64()
        .expect("snapshot row count");
    assert_eq!(
        restore_report["verified_after"]["row_counts"]["log_events"]
            .as_i64()
            .expect("restored row count"),
        snapshot_rows
    );
    assert_eq!(restore_report["verified_after"]["integrity"], "ok");
    assert_eq!(restore_report["verified_after"]["schema_version"], "003");
}

/// `verify` must exit non-zero on a corrupted database — the exit code is the
/// alerting hook for cron/systemd/Kubernetes.
#[test]
fn verify_subcommand_exits_nonzero_on_corruption() {
    let dir = tempfile::tempdir().expect("temp data dir");
    let db = dir.path().join("broken.db");
    // A file that is not a SQLite database must be rejected.
    std::fs::write(&db, b"garbage, definitely not sqlite").expect("write garbage db");

    let output = run_cli(&["verify", "--db", &db.display().to_string()]);
    assert!(
        !output.status.success(),
        "verify must fail on a non-database"
    );

    // A truncated database (page count mismatch) must also fail.
    let dir2 = tempfile::tempdir().expect("temp data dir");
    let valid = dir2.path().join("otel-logs.db");
    {
        let mut conn = rusqlite::Connection::open(&valid).expect("open db");
        otel_sqlite_storage::migrate_up_to(&mut conn, "003").expect("migrate to current schema");
    }
    let truncated = dir.path().join("truncated.db");
    let bytes = std::fs::read(&valid).expect("read valid db");
    std::fs::write(&truncated, &bytes[..bytes.len() / 2]).expect("write truncated db");

    let output = run_cli(&["verify", "--db", &truncated.display().to_string()]);
    assert!(
        !output.status.success(),
        "verify must fail on a truncated database"
    );
}
