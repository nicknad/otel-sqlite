//! `otel-sqlite backup | restore | verify`: operator-driven backup, restore
//! and verification using the SQLite Online Backup API — the single consistent
//! snapshot method.
//!
//! These subcommands run against a **live** database without pausing it: the
//! backup opens the source read-only, so a running server keeps ingesting
//! while the snapshot is taken. Automation (cron, systemd timer, Kubernetes
//! CronJob) schedules the `backup` command; every command exits non-zero on
//! failure, which is what cron/systemd/Kubernetes alerting hooks into.
//!
//! ```sh
//! otel-sqlite backup   --db /data/otel-logs.db --out /backups --keep 30
//! otel-sqlite backup   --db /data/otel-logs.db --out /backups --key-file /keys/backup.key
//! otel-sqlite restore  --backup /backups/otel-logs.db.<UTC>.otsb --dir /data/restored --key-file /keys/backup.key
//! otel-sqlite verify   --db /data/otel-logs.db
//! ```

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use otel_sqlite_storage::backup::{self, BackupReport, RestoreReport, VerifyReport};

/// Default live-database path, matching the server's default `otel-logs.db`.
const DEFAULT_DB_PATH: &str = "otel-logs.db";
/// Default backup destination directory.
const DEFAULT_BACKUP_DIR: &str = "backups";
/// Default number of backups retained per directory after a successful run.
const DEFAULT_KEEP: usize = 30;

#[derive(Debug)]
pub struct BackupArgs {
    pub db: PathBuf,
    pub out_dir: PathBuf,
    pub keep: usize,
    pub key_file: Option<PathBuf>,
    pub json: bool,
}

#[derive(Debug)]
pub struct RestoreArgs {
    pub backup: PathBuf,
    pub dir: PathBuf,
    pub key_file: Option<PathBuf>,
    pub json: bool,
}

#[derive(Debug)]
pub struct VerifyArgs {
    pub db: PathBuf,
    pub json: bool,
}

/// Parses `backup` arguments. Flags: `--db PATH`, `--out DIR`, `--keep N`,
/// `--key-file PATH`, `--json`.
pub fn parse_backup_args(args: &[String]) -> Result<BackupArgs> {
    let mut parsed = BackupArgs {
        db: PathBuf::from(DEFAULT_DB_PATH),
        out_dir: PathBuf::from(DEFAULT_BACKUP_DIR),
        keep: DEFAULT_KEEP,
        key_file: None,
        json: false,
    };
    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--json" => parsed.json = true,
            "--db" => parsed.db = PathBuf::from(value(flag, &mut iter)?),
            "--out" => parsed.out_dir = PathBuf::from(value(flag, &mut iter)?),
            "--key-file" => parsed.key_file = Some(PathBuf::from(value(flag, &mut iter)?)),
            "--keep" => {
                let raw = value(flag, &mut iter)?;
                parsed.keep = raw.parse().with_context(|| {
                    format!("--keep must be a non-negative integer, got `{raw}`")
                })?;
            }
            other => bail!(
                "unknown backup flag `{other}` (expected --db, --out, --keep, --key-file, --json)"
            ),
        }
    }
    Ok(parsed)
}

/// Parses `restore` arguments. Flags: `--backup PATH`, `--dir DIR`,
/// `--key-file PATH`, `--json`.
pub fn parse_restore_args(args: &[String]) -> Result<RestoreArgs> {
    let mut parsed = RestoreArgs {
        backup: PathBuf::new(),
        dir: PathBuf::new(),
        key_file: None,
        json: false,
    };
    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--json" => parsed.json = true,
            "--backup" => parsed.backup = PathBuf::from(value(flag, &mut iter)?),
            "--dir" => parsed.dir = PathBuf::from(value(flag, &mut iter)?),
            "--key-file" => parsed.key_file = Some(PathBuf::from(value(flag, &mut iter)?)),
            other => {
                bail!(
                    "unknown restore flag `{other}` (expected --backup, --dir, --key-file, --json)"
                )
            }
        }
    }
    if parsed.backup.as_os_str().is_empty() {
        bail!("restore requires --backup PATH");
    }
    if parsed.dir.as_os_str().is_empty() {
        bail!("restore requires --dir DIR (must be empty or absent)");
    }
    Ok(parsed)
}

/// Parses `verify` arguments. Flags: `--db PATH`, `--json`.
pub fn parse_verify_args(args: &[String]) -> Result<VerifyArgs> {
    let mut parsed = VerifyArgs {
        db: PathBuf::from(DEFAULT_DB_PATH),
        json: false,
    };
    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--json" => parsed.json = true,
            "--db" => parsed.db = PathBuf::from(value(flag, &mut iter)?),
            other => bail!("unknown verify flag `{other}` (expected --db, --json)"),
        }
    }
    Ok(parsed)
}

fn value<'a>(flag: &str, iter: &mut impl Iterator<Item = &'a String>) -> Result<String> {
    iter.next()
        .cloned()
        .with_context(|| format!("flag {flag} is missing its value"))
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

/// Runs `backup`: take an online snapshot, optionally encrypt it, then prune
/// the retention window. Prints a machine-readable JSON report with `--json`,
/// or a human summary otherwise. Never fails silently: any verification
/// problem aborts before the artifact is trusted.
pub fn run_backup(args: &BackupArgs) -> Result<()> {
    std::fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("cannot create {}", args.out_dir.display()))?;

    let epoch = now_epoch_secs();
    let plain = backup::unique_backup_path(&args.out_dir, epoch, false);
    let mut report = backup::backup_to(&args.db, &plain)?;

    if let Some(key_file) = &args.key_file {
        let encrypted = backup::unique_backup_path(&args.out_dir, epoch, true);
        backup::encrypt_file(&plain, &encrypted, key_file)?;
        std::fs::remove_file(&plain)
            .with_context(|| format!("failed to remove plaintext snapshot {}", plain.display()))?;
        report.backup = encrypted;
        report.encrypted = true;
        report.sha256 = backup::sha256_hex(&report.backup)?;
    }

    let pruned = backup::prune_old_backups(&args.out_dir, args.keep)?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_backup_summary(&report);
        if pruned > 0 {
            println!(
                "retained the newest {keep} backups; pruned {pruned} older file(s)",
                keep = args.keep
            );
        }
    }
    Ok(())
}

fn print_backup_summary(report: &BackupReport) {
    println!(
        "backup written to {} (encrypted: {})",
        report.backup.display(),
        report.encrypted
    );
    println!("  sha256:      {}", report.sha256);
    println!("  integrity:   {}", report.verify.integrity);
    println!(
        "  schema:      {}",
        report
            .verify
            .schema_version
            .as_deref()
            .unwrap_or("(not an otel-sqlite database)")
    );
    println!(
        "  rows:        log_events={} log_resources={} metric_points={} metric_series={} metrics={} scopes={} fts={}",
        report.verify.row_counts.log_events,
        report.verify.row_counts.log_resources,
        report.verify.row_counts.metric_points,
        report.verify.row_counts.metric_series,
        report.verify.row_counts.metrics,
        report.verify.row_counts.scopes,
        report.verify.row_counts.fts_rows,
    );
}

/// Runs `restore`: install a (possibly encrypted) backup into a clean
/// directory. The backup is verified before and after the copy; the directory
/// must be empty or absent. Restore is offline by nature — stop the server
/// first (documented in the runbook).
pub fn run_restore(args: &RestoreArgs) -> Result<()> {
    if !args.backup.exists() {
        bail!("backup file does not exist: {}", args.backup.display());
    }
    let encrypted = backup::looks_encrypted(&args.backup)?;
    if encrypted && args.key_file.is_none() {
        bail!(
            "backup {} is encrypted; pass --key-file to decrypt it",
            args.backup.display()
        );
    }

    let report = backup::restore_into(&args.backup, &args.dir, args.key_file.as_deref())?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_restore_summary(&report);
    }
    Ok(())
}

fn print_restore_summary(report: &RestoreReport) {
    println!(
        "restored {} -> {}",
        report.backup.display(),
        report.target.display()
    );
    println!(
        "  verified before: integrity={} schema={}",
        report.verified_before.integrity,
        report
            .verified_before
            .schema_version
            .as_deref()
            .unwrap_or("(none)")
    );
    println!(
        "  verified after:  integrity={} schema={}",
        report.verified_after.integrity,
        report
            .verified_after
            .schema_version
            .as_deref()
            .unwrap_or("(none)")
    );
    println!(
        "  rows:            log_events={} metric_points={}",
        report.verified_after.row_counts.log_events, report.verified_after.row_counts.metric_points
    );
}

/// Runs `verify`: run integrity/foreign-key/row-count checks on any SQLite
/// file (live database, backup, or restored database). Exits non-zero when
/// verification fails, which is the hook cron/systemd/Kubernetes alert on.
pub fn run_verify(args: &VerifyArgs) -> Result<()> {
    let report = backup::verify(&args.db)?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_verify_summary(&report);
    }

    if report.integrity != "ok" {
        bail!(
            "verification failed: integrity_check = {}",
            report.integrity
        );
    }
    if report.foreign_key_violations > 0 {
        bail!(
            "verification failed: foreign_key_check found {} violation(s)",
            report.foreign_key_violations
        );
    }
    Ok(())
}

fn print_verify_summary(report: &VerifyReport) {
    println!("verify {}:", report.path.display());
    println!("  integrity:   {}", report.integrity);
    println!(
        "  foreign keys: {} violation(s)",
        report.foreign_key_violations
    );
    println!("  journal mode: {}", report.journal_mode);
    println!(
        "  schema:      {}",
        report
            .schema_version
            .as_deref()
            .unwrap_or("(not an otel-sqlite database)")
    );
    println!(
        "  rows:        log_events={} log_resources={} metric_points={} metric_series={} metrics={} scopes={} fts={}",
        report.row_counts.log_events,
        report.row_counts.log_resources,
        report.row_counts.metric_points,
        report.row_counts.metric_series,
        report.row_counts.metrics,
        report.row_counts.scopes,
        report.row_counts.fts_rows,
    );
    println!("  sha256:      {}", report.sha256);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_backup_defaults_and_overrides() {
        let parsed = parse_backup_args(&[]).expect("empty args parse");
        assert_eq!(parsed.db, PathBuf::from(DEFAULT_DB_PATH));
        assert_eq!(parsed.out_dir, PathBuf::from(DEFAULT_BACKUP_DIR));
        assert_eq!(parsed.keep, DEFAULT_KEEP);
        assert!(parsed.key_file.is_none());
        assert!(!parsed.json);

        let parsed = parse_backup_args(&[
            "--db".into(),
            "/data/otel-logs.db".into(),
            "--out".into(),
            "/backups".into(),
            "--keep".into(),
            "7".into(),
            "--key-file".into(),
            "/keys/k".into(),
            "--json".into(),
        ])
        .expect("full args parse");
        assert_eq!(parsed.db, PathBuf::from("/data/otel-logs.db"));
        assert_eq!(parsed.out_dir, PathBuf::from("/backups"));
        assert_eq!(parsed.keep, 7);
        assert_eq!(parsed.key_file, Some(PathBuf::from("/keys/k")));
        assert!(parsed.json);
    }

    #[test]
    fn parse_backup_rejects_bad_keep() {
        assert!(parse_backup_args(&["--keep".into(), "lots".into()]).is_err());
    }

    #[test]
    fn parse_restore_requires_backup_and_dir() {
        assert!(parse_restore_args(&[]).is_err());
        assert!(parse_restore_args(&["--backup".into(), "b.bak".into()]).is_err());
        assert!(parse_restore_args(&["--dir".into(), "d".into()]).is_err());
        let parsed = parse_restore_args(&[
            "--backup".into(),
            "b.otsb".into(),
            "--dir".into(),
            "restored".into(),
            "--key-file".into(),
            "k".into(),
        ])
        .expect("valid restore args");
        assert_eq!(parsed.backup, PathBuf::from("b.otsb"));
        assert_eq!(parsed.dir, PathBuf::from("restored"));
        assert_eq!(parsed.key_file, Some(PathBuf::from("k")));
    }

    #[test]
    fn parse_verify_defaults_to_the_server_database() {
        let parsed = parse_verify_args(&[]).expect("empty verify args");
        assert_eq!(parsed.db, PathBuf::from(DEFAULT_DB_PATH));
    }
}
