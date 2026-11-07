//! `otel-sqlite check`: non-mutating preflight for a server rollout.
//!
//! Answers "could this config + binary + existing database start cleanly?"
//! without binding a listener, installing metrics, spawning workers, or
//! mutating the database. Each section reports independently so an operator
//! sees every blocker in one run:
//!
//! * the effective config is loaded and validated exactly like the server
//!   (`Config::from_env` / `Config::from_file`),
//! * TLS material is parsed (not merely checked for readability),
//! * the bearer-token file is loaded and validated,
//! * the SQLite schema is inspected read-only, including the downgrade guard
//!   that normally only runs inside the mutating migration path.
//!
//! Exit codes: `0` no blocking problem, `1` failed check, `2` usage error.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use otel_sqlite_ingress::{TokenFileVault, validate_tls_material};
use otel_sqlite_storage::{backup, latest_schema_version};
use serde::Serialize;

use crate::backup_cli::{set_once, value};
use crate::config::Config;

/// Parsed `check` flags.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct CheckArgs {
    /// Explicit config file; overrides `OTEL_SQLITE_CFG_PATH`.
    pub config: Option<PathBuf>,
    /// Emit the machine-readable report.
    pub json: bool,
    /// `-h`/`--help`; the caller prints usage instead of checking.
    pub help: bool,
}

/// Parses `check` arguments. Flags: `--config PATH`, `--json`, `-h`/`--help`.
pub fn parse_args(args: &[String]) -> Result<CheckArgs> {
    let mut config: Option<PathBuf> = None;
    let mut json = false;
    let mut help = false;
    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "-h" | "--help" => help = true,
            "--json" => {
                if json {
                    bail!("flag --json was given more than once");
                }
                json = true;
            }
            "--config" => set_once(
                &mut config,
                PathBuf::from(value(flag, &mut iter)?),
                "--config",
            )?,
            other => bail!("unknown check flag `{other}` (expected --config, --json)"),
        }
    }
    Ok(CheckArgs { config, json, help })
}

/// Runs the preflight and prints the report. Returns `Ok(true)` when no
/// blocking problem was found.
pub fn run(args: &CheckArgs) -> Result<bool> {
    let source = config_source(args);
    let path = args.config.clone().or_else(Config::env_config_path);

    let report = match load_config(args) {
        Ok(config) => evaluate(&config, source, path),
        Err(error) => CheckReport::config_failure(source, path, format!("{error:#}")),
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_human(&report);
    }
    Ok(report.ok)
}

/// Usage for `otel-sqlite check`.
pub fn print_usage() {
    println!(
        "otel-sqlite check - non-mutating preflight for a server rollout

Usage:
  otel-sqlite check [--config PATH] [--json]

Loads and validates the effective configuration exactly like the server,
parses TLS certificates/keys, validates the bearer-token file, and inspects
the SQLite schema read-only. It never binds a listener or mutates the
database.

Options:
  --config PATH  TOML config file (overrides {config_env})
  --json         print the machine-readable report
  -h, --help     print this help

Exit status: 0 = no blocking problem, 1 = failed check, 2 = usage error.",
        config_env = crate::config::CONFIG_PATH_ENV_VAR
    );
}

fn config_source(args: &CheckArgs) -> &'static str {
    if args.config.is_some() {
        "file"
    } else if Config::env_config_path().is_some() {
        "env"
    } else {
        "defaults"
    }
}

/// Loads validation-identical config. Explicit `--config` replaces (rather
/// than layers on top of) the environment-selected file.
fn load_config(args: &CheckArgs) -> Result<Config> {
    match &args.config {
        Some(path) => Config::from_file(path)
            .with_context(|| format!("failed to load config file {}", path.display())),
        None => Config::from_env().context("failed to load configuration"),
    }
}

/// The outcome of one optional security material check.
#[derive(Debug, Serialize)]
struct MaterialCheck {
    configured: bool,
    /// `None` when the check could not run (config load failed) or the
    /// material is not configured.
    valid: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl MaterialCheck {
    fn not_configured() -> Self {
        Self {
            configured: false,
            valid: None,
            error: None,
        }
    }

    fn valid() -> Self {
        Self {
            configured: true,
            valid: Some(true),
            error: None,
        }
    }

    fn invalid(error: String) -> Self {
        Self {
            configured: true,
            valid: Some(false),
            error: Some(error),
        }
    }
}

/// Read-only schema state of the configured database.
#[derive(Debug, Serialize)]
struct DatabaseCheck {
    path: String,
    /// `missing`, `in_memory`, `current`, `pending`, `newer`, `not_otel`, or
    /// `error`.
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    applied_version: Option<String>,
    binary_version: &'static str,
    applied: Vec<String>,
    pending: Vec<&'static str>,
}

/// Stable report emitted by [`run`].
#[derive(Debug, Serialize)]
struct CheckReport {
    ok: bool,
    config_source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    config_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_config: Option<serde_json::Value>,
    tls: MaterialCheck,
    auth: MaterialCheck,
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<DatabaseCheck>,
    warnings: Vec<String>,
    errors: Vec<String>,
}

impl CheckReport {
    fn config_failure(source: &'static str, path: Option<PathBuf>, error: String) -> Self {
        Self {
            ok: false,
            config_source: source,
            config_path: path.map(|path| path.display().to_string()),
            effective_config: None,
            tls: MaterialCheck::not_configured(),
            auth: MaterialCheck::not_configured(),
            database: None,
            warnings: Vec::new(),
            errors: vec![error],
        }
    }
}

/// Evaluates a loaded config. Pure apart from read-only filesystem access, so
/// tests can exercise every section without a subprocess.
fn evaluate(config: &Config, source: &'static str, path: Option<PathBuf>) -> CheckReport {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let effective_config = match config.dump_json() {
        Ok(dump) => serde_json::from_str(&dump).ok(),
        Err(error) => {
            errors.push(format!(
                "cannot serialize effective configuration: {error:#}"
            ));
            None
        }
    };

    let tls = match &config.tls {
        None => MaterialCheck::not_configured(),
        Some(tls) => match validate_tls_material(tls) {
            Ok(()) => MaterialCheck::valid(),
            Err(error) => {
                let message = format!("TLS material is invalid: {error}");
                errors.push(message.clone());
                MaterialCheck::invalid(message)
            }
        },
    };

    let auth = match &config.auth {
        None => MaterialCheck::not_configured(),
        Some(auth) => match TokenFileVault::open(&auth.token_file) {
            Ok(_) => MaterialCheck::valid(),
            Err(error) => {
                let message = format!("bearer-token file is invalid: {error}");
                errors.push(message.clone());
                MaterialCheck::invalid(message)
            }
        },
    };

    let database = check_database(&config.sqlite_path, &mut warnings, &mut errors);

    CheckReport {
        ok: errors.is_empty(),
        config_source: source,
        config_path: path.map(|path| path.display().to_string()),
        effective_config,
        tls,
        auth,
        database: Some(database),
        warnings,
        errors,
    }
}

/// Inspects the configured database path without opening it read-write.
fn check_database(
    path: &Path,
    warnings: &mut Vec<String>,
    errors: &mut Vec<String>,
) -> DatabaseCheck {
    let binary_version = latest_schema_version();
    let display = path.display().to_string();

    if is_memory_path(path) {
        return DatabaseCheck {
            path: display,
            state: "in_memory",
            applied_version: None,
            binary_version,
            applied: Vec::new(),
            pending: Vec::new(),
        };
    }

    check_parent_directory(path, warnings);

    if !path.exists() {
        warnings.push(format!(
            "database {display} does not exist yet; the server will create it"
        ));
        return DatabaseCheck {
            path: display,
            state: "missing",
            applied_version: None,
            binary_version,
            applied: Vec::new(),
            pending: Vec::new(),
        };
    }

    let conn = match backup::open_immutable(path) {
        Ok(conn) => conn,
        Err(error) => {
            let state = if matches!(error, backup::BackupError::NotADatabase(_)) {
                "not_otel"
            } else {
                "error"
            };
            errors.push(format!("cannot open database {display}: {error}"));
            return DatabaseCheck {
                path: display,
                state,
                applied_version: None,
                binary_version,
                applied: Vec::new(),
                pending: Vec::new(),
            };
        }
    };

    let status = match otel_sqlite_storage::inspect_schema(&conn) {
        Ok(status) => status,
        Err(error) => {
            let state = if error.is_not_a_database() {
                "not_otel"
            } else {
                "error"
            };
            errors.push(format!("cannot inspect database {display}: {error}"));
            return DatabaseCheck {
                path: display,
                state,
                applied_version: None,
                binary_version,
                applied: Vec::new(),
                pending: Vec::new(),
            };
        }
    };

    let base = DatabaseCheck {
        path: display,
        state: "current",
        applied_version: status.applied.last().cloned(),
        binary_version,
        applied: status.applied,
        pending: status.pending,
    };

    if !status.is_otel_schema {
        errors.push(format!(
            "{} is a SQLite database but not otel-sqlite storage (no log_event table)",
            base.path
        ));
        return DatabaseCheck {
            state: "not_otel",
            ..base
        };
    }

    if status.newer_than_binary {
        errors.push(format!(
            "database schema version {} is newer than the newest supported version {}; \
             upgrade this binary before opening it",
            base.applied_version.as_deref().unwrap_or("(unknown)"),
            status.latest_known
        ));
        return DatabaseCheck {
            state: "newer",
            ..base
        };
    }

    if base.pending.is_empty() {
        base
    } else {
        warnings.push(format!(
            "database will apply pending migration(s) on startup: {}",
            base.pending.join(", ")
        ));
        DatabaseCheck {
            state: "pending",
            ..base
        }
    }
}

fn is_memory_path(path: &Path) -> bool {
    path == Path::new(":memory:") || path.to_string_lossy().starts_with("file::memory:")
}

/// Warns when the database directory does not exist yet or looks read-only.
/// Storage creates missing parents itself, so neither is fatal here; this is
/// a heads-up for permission/volume mistakes that would fail at startup.
fn check_parent_directory(path: &Path, warnings: &mut Vec<String>) {
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return;
    };

    if !parent.exists() {
        match nearest_existing(parent).filter(|ancestor| is_readonly(ancestor)) {
            Some(ancestor) => warnings.push(format!(
                "database directory {} does not exist and {} is read-only; creating it may fail",
                parent.display(),
                ancestor.display()
            )),
            None => warnings.push(format!(
                "database directory {} does not exist yet; the server will create it",
                parent.display()
            )),
        }
        return;
    }

    if is_readonly(parent) {
        warnings.push(format!(
            "database directory {} is not writable; verify the server user can write there",
            parent.display()
        ));
    }
}

fn nearest_existing(path: &Path) -> Option<&Path> {
    path.ancestors()
        .find(|candidate| !candidate.as_os_str().is_empty() && candidate.exists())
}

/// Rough write-bit test. Permission models do not let a non-mutating check
/// prove writability, so callers treat this as advisory.
fn is_readonly(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|metadata| metadata.permissions().readonly())
}

fn print_human(report: &CheckReport) {
    println!("otel-sqlite check (config: {})", report.config_source);
    if let Some(path) = &report.config_path {
        println!("  config file: {path}");
    }
    println!("  tls:         {}", material_line("TLS", &report.tls));
    println!(
        "  auth:        {}",
        material_line("bearer token", &report.auth)
    );
    if let Some(database) = &report.database {
        println!(
            "  database:    {} (state: {}, binary schema: {})",
            database.path, database.state, database.binary_version
        );
        if let Some(version) = &database.applied_version {
            println!("  applied:     {version}");
        }
        if !database.pending.is_empty() {
            println!("  pending:     {}", database.pending.join(", "));
        }
    }
    for warning in &report.warnings {
        println!("  warning:     {warning}");
    }
    for error in &report.errors {
        println!("  error:       {error}");
    }
    println!("  result:      {}", if report.ok { "ok" } else { "failed" });
}

fn material_line(label: &str, check: &MaterialCheck) -> String {
    match (check.configured, check.valid, &check.error) {
        (false, _, _) => format!("{label} not configured"),
        (true, Some(true), _) => format!("{label} configured, valid"),
        (true, _, Some(error)) => error.clone(),
        (true, _, None) => format!("{label} configured"),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use otel_sqlite_ingress::{AuthConfig, TlsConfig};
    use otel_sqlite_storage::migrate_up_to;
    use rusqlite::Connection;

    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn config_for(sqlite_path: PathBuf) -> Config {
        Config {
            sqlite_path,
            ..Config::default()
        }
    }

    #[test]
    fn parse_defaults() {
        let parsed = parse_args(&[]).expect("empty args parse");
        assert_eq!(parsed, CheckArgs::default());
    }

    #[test]
    fn parse_overrides() {
        let parsed = parse_args(&strings(&["--config", "/etc/otel-sqlite.toml", "--json"]))
            .expect("valid args parse");
        assert_eq!(parsed.config, Some(PathBuf::from("/etc/otel-sqlite.toml")));
        assert!(parsed.json);
        assert!(!parsed.help);
    }

    #[test]
    fn parse_rejects_unknown_and_duplicate_flags() {
        let unknown = parse_args(&strings(&["--nope"])).expect_err("unknown flag rejected");
        assert!(unknown.to_string().contains("unknown check flag"));

        let duplicated =
            parse_args(&strings(&["--json", "--json"])).expect_err("duplicate --json rejected");
        assert!(duplicated.to_string().contains("more than once"));

        let missing = parse_args(&strings(&["--config"])).expect_err("missing value rejected");
        assert!(missing.to_string().contains("missing its value"));
    }

    #[test]
    fn missing_database_is_not_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = config_for(dir.path().join("not-created.db"));
        let report = evaluate(&config, "defaults", None);

        assert!(report.ok, "a fresh database path is expected: {report:?}");
        let database = report.database.expect("database section");
        assert_eq!(database.state, "missing");
        assert!(!report.warnings.is_empty());
    }

    #[test]
    fn pending_migration_is_a_warning_not_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("historic.db");
        let mut conn = Connection::open(&path).expect("open db");
        migrate_up_to(&mut conn, "004").expect("build historic schema");
        drop(conn);

        let config = config_for(path);
        let report = evaluate(&config, "defaults", None);

        assert!(report.ok, "pending upgrade is allowed: {report:?}");
        let database = report.database.expect("database section");
        assert_eq!(database.state, "pending");
        assert_eq!(database.applied_version.as_deref(), Some("004"));
        assert!(database.pending.contains(&"005"));
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("pending migration"))
        );
    }

    #[test]
    fn current_schema_is_clean() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("current.db");
        let mut conn = Connection::open(&path).expect("open db");
        migrate_up_to(&mut conn, latest_schema_version()).expect("build current schema");
        drop(conn);

        let config = config_for(path);
        let report = evaluate(&config, "defaults", None);

        assert!(report.ok, "current schema passes: {report:?}");
        let database = report.database.expect("database section");
        assert_eq!(database.state, "current");
        assert!(database.pending.is_empty());
    }

    #[test]
    fn newer_schema_fails_with_the_downgrade_message() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("newer.db");
        let mut conn = Connection::open(&path).expect("open db");
        migrate_up_to(&mut conn, latest_schema_version()).expect("build current schema");
        conn.execute(
            "INSERT INTO schema_migrations (version, description) VALUES ('999', 'future')",
            [],
        )
        .expect("stamp future version");
        drop(conn);

        let config = config_for(path);
        let report = evaluate(&config, "defaults", None);

        assert!(!report.ok);
        let database = report.database.expect("database section");
        assert_eq!(database.state, "newer");
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("upgrade this binary"))
        );
    }

    #[test]
    fn non_otel_sqlite_fails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("foreign.db");
        let conn = Connection::open(&path).expect("open db");
        conn.execute_batch("CREATE TABLE unrelated (id INTEGER PRIMARY KEY);")
            .expect("foreign schema");
        drop(conn);

        let config = config_for(path);
        let report = evaluate(&config, "defaults", None);

        assert!(!report.ok);
        let database = report.database.expect("database section");
        assert_eq!(database.state, "not_otel");
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("not otel-sqlite storage"))
        );
    }

    #[test]
    fn garbage_file_is_reported_as_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("garbage.db");
        fs::write(&path, b"this is not sqlite").expect("write garbage");

        let config = config_for(path);
        let report = evaluate(&config, "defaults", None);

        assert!(!report.ok);
        let database = report.database.expect("database section");
        assert_eq!(database.state, "not_otel");
    }

    #[test]
    fn invalid_tls_material_fails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cert = dir.path().join("server.pem");
        let key = dir.path().join("server.key");
        fs::write(&cert, b"not a certificate").expect("write cert");
        fs::write(&key, b"not a key").expect("write key");

        let mut config = config_for(dir.path().join("missing.db"));
        config.tls = Some(TlsConfig {
            cert,
            key,
            client_ca: None,
        });
        let report = evaluate(&config, "defaults", None);

        assert!(!report.ok);
        assert_eq!(report.tls.valid, Some(false));
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("TLS material is invalid"))
        );
    }

    #[test]
    fn valid_tls_material_passes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let key = rcgen::KeyPair::generate().expect("key");
        let params = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).expect("params");
        let cert = params.self_signed(&key).expect("self-signed cert");

        let cert_path = dir.path().join("server.pem");
        let key_path = dir.path().join("server.key");
        fs::write(&cert_path, cert.pem()).expect("write cert");
        fs::write(&key_path, key.serialize_pem()).expect("write key");

        let mut config = config_for(dir.path().join("missing.db"));
        config.tls = Some(TlsConfig {
            cert: cert_path,
            key: key_path,
            client_ca: None,
        });
        let report = evaluate(&config, "defaults", None);

        assert_eq!(report.tls.valid, Some(true), "report: {report:?}");
        assert!(report.ok, "valid TLS with a missing DB is fine: {report:?}");
    }

    #[test]
    fn invalid_token_file_fails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let token_file = dir.path().join("tokens.txt");
        fs::write(&token_file, b"").expect("empty token file");

        let mut config = config_for(dir.path().join("missing.db"));
        config.auth = Some(AuthConfig { token_file });
        let report = evaluate(&config, "defaults", None);

        assert!(!report.ok);
        assert_eq!(report.auth.valid, Some(false));
    }

    #[test]
    fn valid_token_file_passes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let token_file = dir.path().join("tokens.txt");
        fs::write(&token_file, b"token-a\ntoken-b\n").expect("token file");

        let mut config = config_for(dir.path().join("missing.db"));
        config.auth = Some(AuthConfig { token_file });
        let report = evaluate(&config, "defaults", None);

        assert!(
            report.ok,
            "valid tokens with a missing DB is fine: {report:?}"
        );
        assert_eq!(report.auth.valid, Some(true));
    }
}
