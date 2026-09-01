//! Online backup, restore and verification for the SQLite database.
//!
//! The **SQLite Online Backup API** is the single consistent snapshot method:
//! it copies the database page by page through the source connection's WAL,
//! so a snapshot taken while the server is ingesting is exactly consistent at
//! the moment the copy finishes — without ever pausing the writer or copying
//! the `-wal`/`-shm` sidecars. The snapshot is normalized to a single
//! self-contained rollback-journal file (`PRAGMA journal_mode = DELETE`), so a
//! backup can be opened, verified and restored anywhere without sidecars.
//!
//! Every function here operates on paths and opens its own connections; the
//! storage pipeline is never touched. The source is opened **read-only**, so a
//! backup can run while the production writer keeps ingesting (the intended
//! mode for `otel-sqlite backup`). Mutations on the live database remain
//! exclusively the writer's.
//!
//! Backup files may be optionally encrypted with AES-256-GCM (32-byte key
//! file). An encrypted backup is a self-describing container:
//!
//! ```text
//! "OTSQBAK1" (8 bytes) || version (1 byte) || nonce (12 bytes) || ciphertext
//! ```

use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use rand::TryRngCore;
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

/// Magic bytes identifying an otel-sqlite encrypted backup file.
pub const BACKUP_FILE_MAGIC: &[u8; 8] = b"OTSQBAK1";
/// Current on-disk format version of an encrypted backup.
pub const BACKUP_FILE_VERSION: u8 = 1;
/// AES-256 requires a 32-byte key.
pub const ENCRYPTION_KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;

/// A backup engine failure. Kept separate from [`crate::StorageError`] because
/// backups run outside the pipeline (CLI subcommands, cron/systemd timers) and
/// must never be classified by the writer's retry/salvage policy.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("sqlite failure: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("backup io failure: {0}")]
    Io(#[from] io::Error),
    #[error("integrity check failed on {path}: {detail}")]
    Integrity { path: PathBuf, detail: String },
    #[error(
        "foreign key check found {violations} violation(s) in {path}",
        violations = .violations
    )]
    ForeignKeyViolations { path: PathBuf, violations: i64 },
    #[error("backup source {0} is not a sqlite database")]
    NotADatabase(PathBuf),
    #[error("destination database {0} already exists")]
    DestinationExists(PathBuf),
    #[error("restore directory {0} is not empty")]
    DestinationNotEmpty(PathBuf),
    #[error("encryption key file must contain exactly 32 bytes, got {0}")]
    InvalidKeyLen(usize),
    #[error("unsupported or corrupted backup file: {0}")]
    CorruptBackup(String),
}

/// Row counts for the canonical tables plus the derived search index. Used to
/// compare a backup/restore against its source, which is the content
/// validation half of "integrity checks and row-count/content validation".
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct RowCounts {
    pub log_events: i64,
    pub log_resources: i64,
    pub metric_points: i64,
    pub metric_series: i64,
    pub metrics: i64,
    pub scopes: i64,
    pub fts_rows: i64,
}

/// The result of verifying one SQLite file: integrity, foreign keys, journal
/// mode, schema migration stamp, row counts and a SHA-256 of the file.
#[derive(Debug, Clone, Serialize)]
pub struct VerifyReport {
    pub path: PathBuf,
    /// `PRAGMA integrity_check` output — `"ok"` on success, otherwise the
    /// diagnostic lines joined together.
    pub integrity: String,
    /// Number of rows returned by `PRAGMA foreign_key_check`.
    pub foreign_key_violations: i64,
    /// `PRAGMA journal_mode` of the file at open time.
    pub journal_mode: String,
    /// Highest applied schema migration (the `schema_migrations` stamp), if
    /// the database is schema-migrated otel-sqlite storage.
    pub schema_version: Option<String>,
    pub row_counts: RowCounts,
    /// SHA-256 of the file bytes as they sit on disk.
    pub sha256: String,
}

/// What one `backup_to` call produced. `verify` describes the snapshot
/// *contents*; `sha256` is the hash of the artifact on disk (after optional
/// encryption), so a stored backup can be checked byte-for-byte later.
#[derive(Debug, Clone, Serialize)]
pub struct BackupReport {
    pub source: PathBuf,
    pub backup: PathBuf,
    pub encrypted: bool,
    pub sha256: String,
    pub verify: VerifyReport,
}

/// What one `restore_into` call produced. The backup is verified both before
/// and after it is copied into the clean directory.
#[derive(Debug, Clone, Serialize)]
pub struct RestoreReport {
    pub backup: PathBuf,
    pub target: PathBuf,
    pub verified_before: VerifyReport,
    pub verified_after: VerifyReport,
}

/// Opens a database read-only. Backups and verifications never mutate the
/// source; the single writer remains the only mutator.
pub fn open_readonly(db_path: &Path) -> Result<Connection, BackupError> {
    Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| map_sqlite_error(db_path, error))
}

/// Maps a SQLite failure to a meaningful [`BackupError`]: a file that is not
/// a SQLite database or does not exist deserves a specific message, not a raw
/// result code.
fn map_sqlite_error(path: &Path, error: rusqlite::Error) -> BackupError {
    match error {
        rusqlite::Error::SqliteFailure(ref failure, _)
            if failure.code == rusqlite::ffi::ErrorCode::NotADatabase =>
        {
            BackupError::NotADatabase(path.to_path_buf())
        }
        rusqlite::Error::SqliteFailure(ref failure, _)
            if failure.code == rusqlite::ffi::ErrorCode::CannotOpen =>
        {
            BackupError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("cannot open {}", path.display()),
            ))
        }
        other => BackupError::Sqlite(other),
    }
}

/// Reads the canonical table row counts from an open connection.
fn row_counts(conn: &Connection) -> Result<RowCounts, BackupError> {
    let count =
        |sql: &str| -> Result<i64, BackupError> { Ok(conn.query_row(sql, [], |row| row.get(0))?) };
    Ok(RowCounts {
        log_events: count("SELECT COUNT(*) FROM log_event")?,
        log_resources: count("SELECT COUNT(*) FROM log_resource")?,
        metric_points: count("SELECT COUNT(*) FROM metric_data_point")?,
        metric_series: count("SELECT COUNT(*) FROM metric_series")?,
        metrics: count("SELECT COUNT(*) FROM metric")?,
        scopes: count("SELECT COUNT(*) FROM scope")?,
        fts_rows: count("SELECT COUNT(*) FROM logs_fts")?,
    })
}

/// Verifies a SQLite file: integrity check, foreign-key check, journal mode,
/// schema migration stamp, row counts and a SHA-256 of the file bytes.
///
/// Works on the live database (read-only, safe while the server ingests), on a
/// backup file, or on a restored database — the runbook uses the same command
/// for all three.
pub fn verify(db_path: &Path) -> Result<VerifyReport, BackupError> {
    let conn = open_readonly(db_path)?;

    let integrity: String = {
        let mut statement = conn
            .prepare("PRAGMA integrity_check")
            .map_err(|error| map_sqlite_error(db_path, error))?;
        let rows: Vec<String> = statement
            .query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        if rows.len() == 1 {
            rows[0].clone()
        } else {
            rows.join("; ")
        }
    };

    let foreign_key_violations: i64 = {
        let mut statement = conn.prepare("PRAGMA foreign_key_check")?;
        statement
            .query_map([], |_| Ok(()))
            .map(|rows| rows.count() as i64)?
    };

    let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;

    let schema_version: Option<String> = conn
        .query_row(
            "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .ok();

    let counts = row_counts(&conn)?;
    drop(conn);

    Ok(VerifyReport {
        path: db_path.to_path_buf(),
        integrity,
        foreign_key_violations,
        journal_mode,
        schema_version,
        row_counts: counts,
        sha256: sha256_hex(db_path)?,
    })
}

/// Backs up `source` into `dest` using the SQLite Online Backup API.
///
/// * The source is opened **read-only**: a running server keeps ingesting
///   while the snapshot is taken (the backup reads through the WAL).
/// * The destination must not already exist.
/// * After the copy the snapshot is normalized to a single self-contained
///   rollback-journal file (checkpoint + `journal_mode = DELETE`), so it can
///   be opened, verified and restored anywhere without `-wal`/`-shm` sidecars.
/// * The snapshot is verified and its contents returned for row-count/content
///   comparison against the source.
///
/// The snapshot itself is **not** encrypted; pass it through
/// [`encrypt_file`] when the artifact must be encrypted at rest.
pub fn backup_to(source: &Path, dest: &Path) -> Result<BackupReport, BackupError> {
    if dest.exists() {
        return Err(BackupError::DestinationExists(dest.to_path_buf()));
    }
    if !source.exists() {
        return Err(BackupError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("backup source does not exist: {}", source.display()),
        )));
    }

    // Read-only source connection: the online backup API copies a consistent
    // snapshot through the WAL while the live writer keeps ingesting. The
    // destination connection is a plain file we own; the API copies page by
    // page into it without ever touching the source's `-wal`/`-shm` files.
    let source_conn = open_readonly(source)?;
    {
        let mut dest_conn = Connection::open(dest)?;
        dest_conn.busy_timeout(Duration::from_secs(5))?;
        let backup = rusqlite::backup::Backup::new(&source_conn, &mut dest_conn)
            .map_err(|error| map_sqlite_error(source, error))?;
        backup
            .run_to_completion(100, Duration::from_millis(10), None)
            .map_err(|error| map_sqlite_error(source, error))?;
    }
    drop(source_conn);

    // Normalize into a self-contained single file: checkpoint any WAL frames
    // the snapshot copied, then switch to rollback-journal mode so the backup
    // needs no sidecars anywhere.
    {
        let conn = Connection::open(dest)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode = DELETE;")?;
    }
    restrict_permissions(dest)?;

    let verify = verify(dest)?;
    if verify.integrity != "ok" {
        return Err(BackupError::Integrity {
            path: dest.to_path_buf(),
            detail: verify.integrity.clone(),
        });
    }
    if verify.foreign_key_violations > 0 {
        return Err(BackupError::ForeignKeyViolations {
            path: dest.to_path_buf(),
            violations: verify.foreign_key_violations,
        });
    }

    Ok(BackupReport {
        source: source.to_path_buf(),
        backup: dest.to_path_buf(),
        encrypted: false,
        sha256: verify.sha256.clone(),
        verify,
    })
}

/// Restores a backup into a **clean** directory.
///
/// The destination directory must be empty or absent, and the target
/// `otel-logs.db` must not exist. The backup is verified before and after the
/// copy. Restoring while a server holds the target open is an operator error:
/// the runbook stops the server first (restore is an offline operation).
pub fn restore_into(
    backup_path: &Path,
    dest_dir: &Path,
    key_path: Option<&Path>,
) -> Result<RestoreReport, BackupError> {
    let target = dest_dir.join("otel-logs.db");
    if target.exists() {
        return Err(BackupError::DestinationExists(target));
    }
    if dest_dir.exists() && fs::read_dir(dest_dir)?.next().is_some() {
        return Err(BackupError::DestinationNotEmpty(dest_dir.to_path_buf()));
    }
    fs::create_dir_all(dest_dir)?;

    // Decrypt on demand into a temporary sibling, cleaned up on every path.
    let (plain, _guard) = if let Some(key_path) = key_path {
        let temp = dest_dir.join("otel-logs.db.decrypting");
        let guard = TempGuard(Some(temp.clone()));
        decrypt_to(backup_path, key_path, &temp)?;
        (temp, guard)
    } else {
        (backup_path.to_path_buf(), TempGuard(None))
    };
    let verified_before = verify(&plain)?;
    if verified_before.integrity != "ok" {
        return Err(BackupError::Integrity {
            path: plain.clone(),
            detail: verified_before.integrity.clone(),
        });
    }
    if verified_before.foreign_key_violations > 0 {
        return Err(BackupError::ForeignKeyViolations {
            path: plain.clone(),
            violations: verified_before.foreign_key_violations,
        });
    }

    fs::copy(&plain, &target)?;
    restrict_permissions(&target)?;
    let verified_after = verify(&target)?;

    Ok(RestoreReport {
        backup: backup_path.to_path_buf(),
        target,
        verified_before,
        verified_after,
    })
}

/// Deletes all backups in `dir` beyond the newest `keep`, returning how many
/// were removed. Only files matching the `otel-logs.db.` naming prefix are
/// considered; the live database itself (`otel-logs.db`) is never touched.
/// `keep == 0` disables pruning.
pub fn prune_old_backups(dir: &Path, keep: usize) -> Result<usize, BackupError> {
    if keep == 0 {
        return Ok(0);
    }
    let mut backups: Vec<(PathBuf, io::Result<fs::Metadata>)> = fs::read_dir(dir)?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(OTEL_DB_PREFIX)
        })
        .map(|entry| (entry.path(), entry.metadata()))
        .collect();

    // Newest first by modification time; ties break by path for determinism.
    backups.sort_by(|a, b| {
        let (ta, tb) = (modified(a.1.as_ref().ok()), modified(b.1.as_ref().ok()));
        tb.cmp(&ta)
            .then_with(|| b.0.as_os_str().cmp(a.0.as_os_str()))
    });

    let mut removed = 0;
    for (path, _) in backups.into_iter().skip(keep) {
        fs::remove_file(&path)?;
        removed += 1;
    }
    Ok(removed)
}

fn modified(metadata: Option<&fs::Metadata>) -> Option<std::time::SystemTime> {
    metadata.and_then(|metadata| metadata.modified().ok())
}

/// Prefix of every backup artifact written by `otel-sqlite backup`. Includes
/// the trailing dot so the live database file (`otel-logs.db`) never matches.
const OTEL_DB_PREFIX: &str = "otel-logs.db.";

/// Artifact filename for a backup taken at `epoch_secs` (UTC):
/// `otel-logs.db.<YYYYMMDDTHHMMSSZ>.otsb` when encrypted, `.bak` otherwise.
/// The prefix is shared with [`prune_old_backups`] so retention only ever
/// touches files this tool wrote.
pub fn backup_filename(epoch_secs: u64, encrypted: bool) -> String {
    let extension = if encrypted { "otsb" } else { "bak" };
    format!("{OTEL_DB_PREFIX}{}.{extension}", utc_timestamp(epoch_secs))
}

/// Artifact path for a backup taken at `epoch_secs` that does not collide with
/// an existing file. Two backups within the same second (a cron retry, a
/// tight schedule) must never overwrite each other: the timestamped name when
/// free, otherwise `-<n>` suffixes. All variants keep the retention prefix.
pub fn unique_backup_path(dir: &Path, epoch_secs: u64, encrypted: bool) -> PathBuf {
    let base = backup_filename(epoch_secs, encrypted);
    let candidate = dir.join(&base);
    if !candidate.exists() {
        return candidate;
    }
    let extension = if encrypted { "otsb" } else { "bak" };
    let prefix = base
        .strip_suffix(&format!(".{extension}"))
        .expect("backup filename ends in its extension");
    for counter in 1u64.. {
        let candidate = dir.join(format!("{prefix}-{counter}.{extension}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!("a free filename always exists")
}

/// SHA-256 of a file's bytes, hex-encoded.
pub fn sha256_hex(path: &Path) -> Result<String, BackupError> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Whether `path` starts with the encrypted-backup magic bytes.
pub fn looks_encrypted(path: &Path) -> io::Result<bool> {
    let mut magic = [0u8; BACKUP_FILE_MAGIC.len()];
    let mut file = fs::File::open(path)?;
    let read = file.read(&mut magic)?;
    Ok(read == BACKUP_FILE_MAGIC.len() && &magic == BACKUP_FILE_MAGIC)
}

/// Reads and validates a backup encryption key (exactly 32 bytes).
fn read_key(path: &Path) -> Result<Zeroizing<Vec<u8>>, BackupError> {
    let bytes = fs::read(path)?;
    if bytes.len() != ENCRYPTION_KEY_LEN {
        return Err(BackupError::InvalidKeyLen(bytes.len()));
    }
    Ok(Zeroizing::new(bytes))
}

fn random_nonce() -> [u8; NONCE_LEN] {
    let mut nonce = [0u8; NONCE_LEN];
    rand::rngs::OsRng
        .try_fill_bytes(&mut nonce)
        .expect("operating system random source must be available");
    nonce
}

/// Encrypts a plaintext backup file into `out` with AES-256-GCM under the key
/// in `key_path`. The output is self-describing:
/// `magic || version || nonce || ciphertext`. `out` must not exist.
pub fn encrypt_file(plain: &Path, out: &Path, key_path: &Path) -> Result<(), BackupError> {
    if out.exists() {
        return Err(BackupError::DestinationExists(out.to_path_buf()));
    }
    let key = read_key(key_path)?;
    let nonce_bytes = random_nonce();
    let cipher =
        Aes256Gcm::new_from_slice(key.as_slice()).expect("key length validated to 32 bytes");
    let plaintext = fs::read(plain)?;
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), plaintext.as_ref())
        .map_err(|_| BackupError::CorruptBackup("AES-GCM encryption failed".to_owned()))?;

    let mut file = fs::File::create(out)?;
    file.write_all(BACKUP_FILE_MAGIC)?;
    file.write_all(&[BACKUP_FILE_VERSION])?;
    file.write_all(&nonce_bytes)?;
    file.write_all(&ciphertext)?;
    file.sync_all()?;
    drop(file);
    restrict_permissions(out)?;
    Ok(())
}

/// Decrypts an encrypted backup into `out` with the key in `key_path`.
/// Authenticates the ciphertext: a wrong key or a corrupted file fails here,
/// never after an operator already trusted the bytes.
pub fn decrypt_to(encrypted: &Path, key_path: &Path, out: &Path) -> Result<(), BackupError> {
    let key = read_key(key_path)?;
    let bytes = fs::read(encrypted)?;
    if bytes.len() < BACKUP_FILE_MAGIC.len() + 1 + NONCE_LEN
        || &bytes[..BACKUP_FILE_MAGIC.len()] != BACKUP_FILE_MAGIC
    {
        return Err(BackupError::CorruptBackup(
            "not an otel-sqlite encrypted backup (bad magic)".to_owned(),
        ));
    }
    if bytes[BACKUP_FILE_MAGIC.len()] != BACKUP_FILE_VERSION {
        return Err(BackupError::CorruptBackup(format!(
            "unsupported backup file version {}",
            bytes[BACKUP_FILE_MAGIC.len()]
        )));
    }
    let (nonce, ciphertext) = bytes[BACKUP_FILE_MAGIC.len() + 1..].split_at(NONCE_LEN);
    let cipher =
        Aes256Gcm::new_from_slice(key.as_slice()).expect("key length validated to 32 bytes");
    let plaintext = cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| {
            BackupError::CorruptBackup(
                "authentication failed: wrong key or corrupted backup".to_owned(),
            )
        })?;

    let mut file = fs::File::create(out)?;
    file.write_all(&plaintext)?;
    file.sync_all()?;
    drop(file);
    restrict_permissions(out)?;
    Ok(())
}

/// Restricts a backup artifact to owner-only read/write on unix. Windows
/// relies on directory ACLs instead (documented in the runbook).
#[cfg(unix)]
fn restrict_permissions(path: &Path) -> Result<(), BackupError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
#[allow(clippy::unnecessary_wraps)] // mirrors the unix arm's signature for uniform call sites
fn restrict_permissions(_path: &Path) -> Result<(), BackupError> {
    Ok(())
}

/// Best-effort cleanup of a temporary decrypted file on every exit path.
struct TempGuard(Option<PathBuf>);

impl Drop for TempGuard {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(&path);
        }
    }
}

/// Renders a unix epoch in seconds as `YYYYMMDDTHHMMSSZ` in UTC.
///
/// Civil-from-days conversion (Howard Hinnant's public-domain algorithm):
/// deterministic, dependency-free and testable.
fn utc_timestamp(epoch_secs: u64) -> String {
    let days = (epoch_secs / 86_400) as i64;
    let seconds_in_day = (epoch_secs % 86_400) as i64;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };

    let hour = seconds_in_day / 3600;
    let minute = (seconds_in_day % 3600) / 60;
    let second = seconds_in_day % 60;

    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_is_utc_zero_padded() {
        // 2026-08-30T12:34:56Z = 1788093296 epoch seconds.
        let text = utc_timestamp(1_788_093_296);
        assert_eq!(text, "20260830T123456Z");
        assert_eq!(text.len(), 16);
    }
}
