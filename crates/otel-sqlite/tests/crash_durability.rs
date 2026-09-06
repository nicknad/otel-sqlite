//! Process-level crash durability and restart tests (P0-2).
//!
//! Proves the durable-ack contract under REAL process death: the production
//! `otel-sqlite` binary is spawned against a temporary database, killed
//! (SIGKILL / `TerminateProcess` — no cleanup) while actively ingesting, then
//! restarted against the same database. Validation asserts:
//!
//! * every response acknowledged in `durability.mode = "commit"` has a
//!   corresponding persisted record after restart — **no acknowledged loss**;
//! * requests whose response was interrupted are treated as **ambiguous**,
//!   never as proven loss;
//! * the database reopens, WAL recovery replays committed frames, and new
//!   traffic succeeds;
//! * writer death and batcher death (via the env-gated fault knobs in
//!   `otel-sqlite-storage`) fail pending durable acks with `UNAVAILABLE`
//!   within a bounded timeout, and the watchdog halts ingestion;
//! * clean shutdown (SIGTERM) drains, flips health to `NOT_SERVING`, and
//!   truncates the WAL.
//!
//! The binary used is `env!("CARGO_BIN_EXE_otel-sqlite")` (the dev build of
//! THIS package) unless `OTEL_SQLITE_CRASH_BIN` points at another binary
//! (the release build, required for the P0-2 release gate).
//!
//! Load execution, outcome accounting and post-restart validation come from
//! `otel-sqlite-e2e::crash`, the same code the container crash driver uses,
//! so both paths cannot drift.

// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use otel_sqlite_core::storage::SyncMode;
use otel_sqlite_e2e::crash::{
    CrashServerConfig, LoadParams, assert_valid, assert_valid_recovery, run_load,
    write_server_config,
};
use otel_sqlite_e2e::generator::WorkloadSpec;

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

/// The binary under test: the `OTEL_SQLITE_CRASH_BIN` override when set (the
/// release build in CI), otherwise this package's own dev binary.
fn binary_path() -> PathBuf {
    match std::env::var_os("OTEL_SQLITE_CRASH_BIN").filter(|value| !value.is_empty()) {
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

/// Deterministic workload shape shared by every crash scenario.
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

/// Polls until the endpoint accepts TCP connections (the gRPC listener is
/// up), or fails after `timeout`.
fn wait_serving(endpoint: &str, timeout: Duration) {
    let host_port = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let started = Instant::now();
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

/// Spawns the real binary with the crash config and waits until it serves.
fn spawn_server(config: &CrashServerConfig, extra_env: &[(&str, &str)]) -> ServerProc {
    let config_path = write_server_config(config);
    let stdout_path = config.dir.join("server.out.log");
    let stderr_path = config.dir.join("server.err.log");
    let stdout = std::fs::File::create(&stdout_path).expect("create server stdout log");
    let stderr = std::fs::File::create(&stderr_path).expect("create server stderr log");

    let mut command = std::process::Command::new(binary_path());
    command
        .env("OTEL_SQLITE_CFG_PATH", &config_path)
        .stdout(stdout)
        .stderr(stderr);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let child = command.spawn().expect("spawn otel-sqlite server");

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

/// Abrupt kill (SIGKILL / TerminateProcess): exactly the "process death with
/// no cleanup" the WAL recovery path must survive. Returns the status.
fn kill_process(proc: &mut ServerProc) -> std::process::ExitStatus {
    let _ = proc.child.kill();
    let status = proc.child.wait().expect("reap killed server");
    assert!(
        !status.success(),
        "expected an abrupt (non-graceful) exit, got {status:?}"
    );
    status
}

/// Waits for the child to exit on its own within `timeout` (used by the
/// fault-death tests, where the watchdog is expected to halt the process).
/// Kills it if the deadline passes.
fn wait_for_exit(proc: &mut ServerProc, timeout: Duration) -> bool {
    let started = Instant::now();
    loop {
        if let Some(_status) = proc.child.try_wait().expect("poll server exit") {
            return true;
        }
        if started.elapsed() >= timeout {
            let _ = proc.child.kill();
            let _ = proc.child.wait();
            return false;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Waits until the WAL holds at least one page of un-checkpointed frames —
/// proof that commits are sitting in the WAL at the moment of the kill.
fn wait_wal_active(db_path: &Path, timeout: Duration) {
    let wal_path = db_path.with_extension("db-wal");
    let started = Instant::now();
    loop {
        let size = std::fs::metadata(&wal_path).map_or(0, |meta| meta.len());
        if size >= 4096 {
            return;
        }
        assert!(
            started.elapsed() < timeout,
            "WAL did not accumulate a full page of committed frames within {timeout:?} \
             (size {size}); the crash would not exercise WAL recovery"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Runs the built-in `healthcheck` subcommand and returns whether it reported
/// SERVING (exit 0).
#[cfg(unix)]
fn healthcheck_reports_serving(endpoint: &str) -> bool {
    let status = std::process::Command::new(binary_path())
        .arg("healthcheck")
        .env("OTEL_SQLITE_HEALTHCHECK_ENDPOINT", endpoint)
        .status()
        .expect("run healthcheck probe");
    status.success()
}

/// Core crash-recovery scenario, run for `synchronous = normal` and `full`:
/// boot, load, kill mid-WAL-write with requests in flight, restart on the
/// same database, assert no acknowledged loss, then prove live traffic.
async fn crash_recovery_acknowledges_are_durable(sync: SyncMode) {
    let dir = tempfile::tempdir().expect("temp data dir");
    let base = dir.path();

    // Phase 0: boot the real binary.
    let config = CrashServerConfig::new(base.to_path_buf(), free_port(), sync);
    let mut server = spawn_server(&config, &[]);

    // Phase 1: closed-loop load while we kill the process mid-run. The
    // pipeline is kept saturated, so requests are genuinely in flight
    // (durable-ack waits) at the kill point.
    let mut params = LoadParams::new("crash-a");
    params.duration = Duration::from_secs(6);
    let endpoint = server.endpoint.clone();
    let spec = workload_spec("crash-a");
    let load = tokio::spawn(async move { run_load(&endpoint, spec, &params).await });

    // Kill during active WAL writes, with requests in flight.
    wait_wal_active(&server.db_path, Duration::from_secs(15));
    std::thread::sleep(Duration::from_millis(300));
    let _ = kill_process(&mut server);

    let outcome_a = load
        .await
        .expect("load task joined")
        .expect("pre-crash load completes");
    println!(
        "crash-a: generated={} accepted={} rejected={} ambiguous={}",
        outcome_a.counters.records_generated,
        outcome_a.counters.records_accepted,
        outcome_a.counters.records_rejected,
        outcome_a.counters.records_ambiguous,
    );
    assert!(
        outcome_a.counters.records_accepted > 0,
        "pre-crash load must accept and acknowledge records"
    );

    // Phase 2: restart against the SAME database (fresh port, same db files).
    let restart = CrashServerConfig::new(base.to_path_buf(), free_port(), sync);
    let mut server2 = spawn_server(&restart, &[]);

    // Phase 3: every acknowledged record survived; DB is intact and WAL-mode.
    assert_valid(&server2.db_path, "crash-a", &outcome_a)
        .expect("every acknowledged record must survive the crash and WAL replay");

    // Phase 4: new traffic succeeds after recovery.
    let mut params_b = LoadParams::new("crash-b");
    params_b.duration = Duration::from_secs(3);
    let outcome_b = run_load(&server2.endpoint, workload_spec("crash-b"), &params_b)
        .await
        .expect("post-restart load");
    assert_valid(&server2.db_path, "crash-b", &outcome_b)
        .expect("post-restart traffic must persist normally");

    // Phase 5: teardown. Graceful shutdown is covered separately (unix).
    let _ = kill_process(&mut server2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_recovery_acknowledges_are_durable_sync_normal() {
    crash_recovery_acknowledges_are_durable(SyncMode::Normal).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_recovery_acknowledges_are_durable_sync_full() {
    crash_recovery_acknowledges_are_durable(SyncMode::Full).await;
}

/// Writer death via the fault knob: the writer exits through its fatal-error
/// path after `N` commands, closing the ledger so pending durable acks fail
/// with `UNAVAILABLE`; the watchdog observes the dead thread, halts
/// ingestion, and the process exits within a bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_death_fails_pending_acks_with_unavailable() {
    let dir = tempfile::tempdir().expect("temp data dir");
    let mut config =
        CrashServerConfig::new(dir.path().to_path_buf(), free_port(), SyncMode::Normal);
    config.fast_watchdog = true;
    config.shutdown_timeout = Duration::from_secs(10);
    let mut server = spawn_server(&config, &[("OTEL_SQLITE_FAULT_WRITER_AFTER_N", "15")]);

    let mut params = LoadParams::new("fault-writer");
    params.duration = Duration::from_secs(10);
    params.request_timeout = Duration::from_secs(15);
    let outcome = run_load(&server.endpoint, workload_spec("fault-writer"), &params)
        .await
        .expect("writer-fault load");

    // The watchdog must detect the dead writer and halt ingestion; the
    // process then drains and exits on its own.
    assert!(
        wait_for_exit(&mut server, Duration::from_secs(40)),
        "process must exit after the writer dies (watchdog halt)"
    );

    // Records acknowledged before the writer died are durable; the request
    // that was mid-commit when the ledger closed may be partially applied
    // (rejected-present is expected on component death).
    assert_valid_recovery(&server.db_path, "fault-writer", &outcome)
        .expect("records acknowledged before the writer died must be durable");

    // Pending/interrupted handlers failed over: we saw UNAVAILABLE or
    // ambiguous outcomes, never a silent hang.
    let failed_over = outcome.counters.records_rejected + outcome.counters.records_ambiguous;
    assert!(
        failed_over > 0,
        "a dying writer must surface as bounded failures, not silent hangs"
    );
    println!(
        "writer-death: generated={} accepted={} rejected={} ambiguous={}",
        outcome.counters.records_generated,
        outcome.counters.records_accepted,
        outcome.counters.records_rejected,
        outcome.counters.records_ambiguous,
    );
}

/// Batcher death via the fault knob: the batcher stops consuming, the input
/// channel closes (new requests fail with `UNAVAILABLE`), the watchdog halts,
/// and the shutdown path closes the ledger so pending durable acks resolve
/// instead of hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batcher_death_fails_pending_acks_with_unavailable() {
    let dir = tempfile::tempdir().expect("temp data dir");
    let mut config =
        CrashServerConfig::new(dir.path().to_path_buf(), free_port(), SyncMode::Normal);
    config.fast_watchdog = true;
    config.shutdown_timeout = Duration::from_secs(5);
    let mut server = spawn_server(&config, &[("OTEL_SQLITE_FAULT_BATCHER_AFTER_N", "300")]);

    let mut params = LoadParams::new("fault-batcher");
    params.duration = Duration::from_secs(10);
    params.request_timeout = Duration::from_secs(15);
    let outcome = run_load(&server.endpoint, workload_spec("fault-batcher"), &params)
        .await
        .expect("batcher-fault load");

    // The watchdog halts and the process exits (drain deadline + shutdown).
    assert!(
        wait_for_exit(&mut server, Duration::from_secs(40)),
        "process must exit after the batcher dies (watchdog halt)"
    );

    // Records acknowledged before the batcher died are durable; rejected-present
    // is expected for requests stranded in the ingest queue when it closed.
    assert_valid_recovery(&server.db_path, "fault-batcher", &outcome)
        .expect("records acknowledged before the batcher died must be durable");

    // New/queued requests failed over instead of hanging.
    let failed_over = outcome.counters.records_rejected + outcome.counters.records_ambiguous;
    assert!(
        failed_over > 0,
        "a dying batcher must surface as bounded failures, not silent hangs"
    );
    println!(
        "batcher-death: generated={} accepted={} rejected={} ambiguous={}",
        outcome.counters.records_generated,
        outcome.counters.records_accepted,
        outcome.counters.records_rejected,
        outcome.counters.records_ambiguous,
    );
}

/// Clean shutdown: SIGTERM triggers a graceful drain (health flips
/// `NOT_SERVING`, accepted records persist, the WAL is truncated) and the
/// process exits 0 within the shutdown budget. SIGTERM is sent MID-LOAD so
/// the drain deadline (`shutdown_timeout`) guarantees a multi-second window
/// in which the process is still alive but already `NOT_SERVING`.
/// Unix-only: Windows has no SIGTERM; graceful shutdown there stays covered
/// by the embedded harness.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(unsafe_code)] // SAFETY: justified inline where the signal is sent.
async fn clean_shutdown_is_graceful_and_serving_flips() {
    let dir = tempfile::tempdir().expect("temp data dir");
    let mut config =
        CrashServerConfig::new(dir.path().to_path_buf(), free_port(), SyncMode::Normal);
    config.shutdown_timeout = Duration::from_secs(10);
    let mut server = spawn_server(&config, &[]);

    let mut params = LoadParams::new("clean-shutdown");
    params.duration = Duration::from_secs(8);
    let endpoint = server.endpoint.clone();
    let spec = workload_spec("clean-shutdown");
    let load = tokio::spawn(async move { run_load(&endpoint, spec, &params).await });

    // SIGTERM mid-load: the server flips NOT_SERVING immediately and drains
    // in-flight requests up to shutdown_timeout before exiting.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let pid = server.child.id();
    // SAFETY: `libc::kill` takes a valid process id (positive) and a valid
    // signal constant; the pid came from the live child we spawned.
    let result = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    assert_eq!(result, 0, "SIGTERM delivery failed");

    // After SIGTERM the health service must never report SERVING again, and
    // the NOT_SERVING state must be observable while the drain is running.
    let started = Instant::now();
    let mut observed_not_serving = false;
    let status = loop {
        if let Some(status) = server.child.try_wait().expect("poll exit") {
            break status;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "server did not exit within 30s of SIGTERM"
        );
        let serving = healthcheck_reports_serving(&server.endpoint);
        assert!(!serving, "health must never report SERVING after SIGTERM");
        observed_not_serving = true;
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(
        observed_not_serving,
        "must observe NOT_SERVING during the graceful drain window"
    );

    // SIGTERM is a clean shutdown: exit 0, not a signal.
    assert!(
        status.success(),
        "SIGTERM must exit 0 after a graceful drain, got {status:?}"
    );

    let outcome = load
        .await
        .expect("load task joined")
        .expect("clean-shutdown load");
    // Everything acknowledged is persisted (the drain completed) and the
    // final checkpoint truncated the WAL.
    assert_valid(&server.db_path, "clean-shutdown", &outcome)
        .expect("graceful shutdown must not lose acknowledged records");
    let wal_size =
        std::fs::metadata(server.db_path.with_extension("db-wal")).map_or(0, |meta| meta.len());
    assert_eq!(wal_size, 0, "graceful shutdown truncates the WAL");
}
