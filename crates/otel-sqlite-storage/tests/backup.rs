// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

//! Backup engine integration tests: the SQLite Online Backup
//! API snapshot, restore into a clean directory, integrity/content
//! verification, corruption rejection, encryption round-trips and the online
//! backup-while-ingesting property.

use std::time::Duration;

use crossbeam_channel::unbounded;
use otel_sqlite_core::model::{Attribute, LogBatch, LogRecord, Resource, Severity};
use otel_sqlite_core::storage::{BatchOrigin, IngestMessage, LogChunk};
use otel_sqlite_storage::backup::{self, BackupError};
use otel_sqlite_storage::{Storage, StorageConfig};

fn logs_message(batch: LogBatch) -> IngestMessage {
    IngestMessage::Logs(LogChunk {
        origin: BatchOrigin {
            resource: batch.resource,
            schema_url: batch.schema_url,
        },
        records: batch.records,
        commit_seq: 0,
    })
}

fn sample_batch() -> LogBatch {
    let mut batch = LogBatch::with_capacity(2);
    batch.resource = Some(Resource::new(vec![
        ("service.name", "checkout").into(),
        ("retries", 3_i64).into(),
    ]));
    "https://example.test/schemas".clone_into(&mut batch.schema_url);

    batch.push(LogRecord {
        time_unix_nano: 1_000,
        observed_time_unix_nano: 1_500,
        severity_number: Severity::Error,
        severity_text: "ERROR".to_owned(),
        body: "payment failed".to_owned(),
        event_name: "order.failed".to_owned(),
        scope_name: "scope-a".to_owned(),
        attributes: vec![Attribute {
            key: "attempt".to_owned(),
            value: otel_sqlite_core::model::AttributeValue::Int(2),
        }],
        ..LogRecord::default()
    });
    batch.push(LogRecord {
        time_unix_nano: 2_000,
        severity_number: Severity::Info,
        body: "ok".to_owned(),
        ..LogRecord::default()
    });
    batch
}

/// Builds a migrated, populated database through the real pipeline. Returns
/// the database path (still on disk after the pipeline joined).
fn populated_db(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let db_path = dir.join(name);
    let (sender, receiver) = unbounded();
    let mut storage = Storage::open(
        receiver,
        StorageConfig {
            sqlite_path: db_path.clone(),
            insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
                1_000,
                Duration::from_millis(20),
            ),
            ..StorageConfig::default()
        },
    )
    .expect("storage opens");
    sender.send(logs_message(sample_batch())).unwrap();
    sender.send(logs_message(sample_batch())).unwrap();
    sender.send(IngestMessage::Flush).unwrap();
    drop(sender);
    storage.join().expect("storage joins cleanly");
    db_path
}

/// The backup snapshot must be a consistent, self-contained copy of the
/// source: integrity ok, foreign keys clean, same row counts, journal mode
/// normalized to a single file, and restorable into a clean directory.
#[test]
fn backup_matches_source_and_restores_into_a_clean_directory() {
    let dir = tempfile::tempdir().unwrap();
    let db = populated_db(dir.path(), "otel-logs.db");

    // Row counts of the live (source) database, read-only.
    let source = backup::verify(&db).unwrap();
    assert_eq!(source.integrity, "ok");
    assert_eq!(source.row_counts.log_events, 4);

    // Online backup while nothing else holds a write lock — the read-only
    // source is the same code path the CLI uses against a live server.
    let snapshot = dir.path().join("snapshot.bak");
    let report = backup::backup_to(&db, &snapshot).unwrap();
    assert!(!report.encrypted);
    assert_eq!(report.verify.integrity, "ok");
    assert_eq!(report.verify.foreign_key_violations, 0);
    assert_eq!(report.verify.schema_version.as_deref(), Some("003"));
    assert_eq!(
        report.verify.row_counts, source.row_counts,
        "snapshot row counts must match the source exactly"
    );
    // Normalized to a self-contained rollback-journal file: no sidecars.
    assert_eq!(report.verify.journal_mode, "delete");
    assert!(!dir.path().join("snapshot.bak-wal").exists());
    assert!(!dir.path().join("snapshot.bak-shm").exists());

    // Restore into a clean directory.
    let clean = dir.path().join("restored");
    let restored = backup::restore_into(&snapshot, &clean, None).unwrap();
    assert_eq!(restored.target, clean.join("otel-logs.db"));
    assert_eq!(
        restored.verified_after.row_counts, source.row_counts,
        "restored database must contain every snapshot row"
    );
    assert_eq!(
        restored.verified_after.schema_version.as_deref(),
        Some("003")
    );
    assert_eq!(restored.verified_after.integrity, "ok");
    assert_eq!(restored.verified_after.foreign_key_violations, 0);
}

/// A backup taken while the writer is actively ingesting must be consistent:
/// this is the "online backup while ingestion is active" contract. The test
/// opens a read-only source connection from this thread while the writer
/// keeps committing on another thread.
#[test]
fn backup_is_consistent_while_ingestion_is_active() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("otel-logs.db");

    let (sender, receiver) = unbounded();
    let mut storage = Storage::open(
        receiver,
        StorageConfig {
            sqlite_path: db_path.clone(),
            insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
                1_000,
                Duration::from_millis(10),
            ),
            ..StorageConfig::default()
        },
    )
    .expect("storage opens");

    // Keep the pipeline busy for the whole test; commits land while the
    // backup below runs on this thread.
    let feeder_sender = sender.clone();
    let feeder = std::thread::spawn(move || {
        let started = std::time::Instant::now();
        while started.elapsed() < Duration::from_millis(800) {
            feeder_sender.send(logs_message(sample_batch())).unwrap();
            std::thread::sleep(Duration::from_millis(5));
        }
    });

    // Give the writer some committed pages, then snapshot mid-flight.
    std::thread::sleep(Duration::from_millis(150));
    let snapshot = dir.path().join("online.bak");
    let report = backup::backup_to(&db_path, &snapshot).expect("online backup succeeds");
    assert_eq!(
        report.verify.integrity, "ok",
        "snapshot taken during active ingestion must pass integrity_check"
    );
    assert_eq!(report.verify.foreign_key_violations, 0);
    assert!(report.verify.row_counts.log_events > 0);

    feeder.join().unwrap();
    drop(sender);
    storage.join().expect("storage joins cleanly");

    // The snapshot is a prefix of the final database, never a torn copy.
    let final_counts = backup::verify(&db_path).unwrap().row_counts;
    assert!(
        final_counts.log_events >= report.verify.row_counts.log_events,
        "source may only grow after the snapshot"
    );
    assert_eq!(
        report.verify.row_counts,
        backup::verify(&snapshot).unwrap().row_counts,
        "re-verifying the snapshot must be stable"
    );
}

/// Restore must refuse to overwrite anything: a non-empty destination
/// directory is rejected, and an already-existing target database is too.
#[test]
fn restore_refuses_non_empty_or_occupied_destinations() {
    let dir = tempfile::tempdir().unwrap();
    let db = populated_db(dir.path(), "otel-logs.db");
    let snapshot = dir.path().join("snapshot.bak");
    backup::backup_to(&db, &snapshot).unwrap();

    let occupied = dir.path().join("occupied");
    std::fs::create_dir_all(&occupied).unwrap();
    std::fs::write(occupied.join("stray.txt"), "x").unwrap();
    assert!(
        matches!(
            backup::restore_into(&snapshot, &occupied, None),
            Err(BackupError::DestinationNotEmpty(_))
        ),
        "restore must refuse a non-empty directory"
    );

    let target_exists = dir.path().join("target");
    std::fs::create_dir_all(&target_exists).unwrap();
    std::fs::write(target_exists.join("otel-logs.db"), "already there").unwrap();
    assert!(
        matches!(
            backup::restore_into(&snapshot, &target_exists, None),
            Err(BackupError::DestinationExists(_))
        ),
        "restore must refuse when the target database already exists"
    );
}

/// Verification must catch a corrupted backup before it is trusted: restore
/// rejects a snapshot whose bytes were damaged.
#[test]
fn restore_rejects_a_corrupted_backup() {
    let dir = tempfile::tempdir().unwrap();
    let db = populated_db(dir.path(), "otel-logs.db");
    let snapshot = dir.path().join("snapshot.bak");
    backup::backup_to(&db, &snapshot).unwrap();

    // Truncate the snapshot: page_count no longer matches the file size, so
    // integrity_check must report a malformed database.
    let truncated = dir.path().join("corrupt.bak");
    let bytes = std::fs::read(&snapshot).unwrap();
    std::fs::write(&truncated, &bytes[..bytes.len() / 2]).unwrap();

    let clean = dir.path().join("restored");
    let outcome = backup::restore_into(&truncated, &clean, None);
    assert!(outcome.is_err(), "corrupted backup must not restore");
    assert!(!clean.join("otel-logs.db").exists());

    // And verify() must report the damage (either an error or a non-ok
    // integrity result).
    if let Ok(report) = backup::verify(&truncated) {
        assert_ne!(report.integrity, "ok");
    }
}

/// Encryption round-trip: an encrypted artifact is self-describing, decrypts
/// back to the identical database, rejects a wrong key, and rejects a
/// tampered artifact via AEAD authentication.
#[test]
fn encryption_round_trip_rejects_wrong_key_and_tampering() {
    let dir = tempfile::tempdir().unwrap();
    let db = populated_db(dir.path(), "otel-logs.db");
    let snapshot = dir.path().join("snapshot.bak");
    backup::backup_to(&db, &snapshot).unwrap();

    let key_path = dir.path().join("backup.key");
    std::fs::write(&key_path, [7u8; 32]).unwrap();

    let encrypted = dir.path().join("snapshot.otsb");
    backup::encrypt_file(&snapshot, &encrypted, &key_path).unwrap();
    assert!(backup::looks_encrypted(&encrypted).unwrap());

    let decrypted = dir.path().join("decrypted.bak");
    backup::decrypt_to(&encrypted, &key_path, &decrypted).unwrap();
    let before = backup::verify(&snapshot).unwrap();
    let after = backup::verify(&decrypted).unwrap();
    assert_eq!(before.row_counts, after.row_counts);
    assert_eq!(before.sha256, after.sha256);

    // Wrong key: authentication fails, nothing is written to `out`.
    let wrong_key = dir.path().join("wrong.key");
    std::fs::write(&wrong_key, [9u8; 32]).unwrap();
    let wrong_out = dir.path().join("wrong-out.bak");
    assert!(
        backup::decrypt_to(&encrypted, &wrong_key, &wrong_out).is_err(),
        "a wrong key must fail authentication"
    );
    assert!(!wrong_out.exists());

    // Tampering with a single ciphertext byte must fail authentication.
    let tampered = dir.path().join("tampered.otsb");
    let mut bytes = std::fs::read(&encrypted).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    std::fs::write(&tampered, &bytes).unwrap();
    assert!(
        backup::decrypt_to(&tampered, &key_path, &wrong_out).is_err(),
        "a tampered artifact must fail AEAD authentication"
    );
    assert!(!wrong_out.exists());

    // Restore path decrypts transparently with the key.
    let clean = dir.path().join("restored");
    let report = backup::restore_into(&encrypted, &clean, Some(&key_path)).unwrap();
    assert_eq!(report.verified_after.row_counts.log_events, 4);
    assert_eq!(report.verified_after.integrity, "ok");
}

/// A key file that is not exactly 32 bytes is rejected.
#[test]
fn encryption_rejects_bad_key_lengths() {
    let dir = tempfile::tempdir().unwrap();
    let db = populated_db(dir.path(), "otel-logs.db");
    let snapshot = dir.path().join("snapshot.bak");
    backup::backup_to(&db, &snapshot).unwrap();

    let short = dir.path().join("short.key");
    std::fs::write(&short, [1u8; 8]).unwrap();
    assert!(
        matches!(
            backup::encrypt_file(&snapshot, &dir.path().join("x.otsb"), &short),
            Err(BackupError::InvalidKeyLen(8))
        ),
        "8-byte keys must be rejected"
    );
}

/// Retention pruning removes only backup artifacts, newest-first, and never
/// the live database or unrelated files. Files carry distinct modification
/// times so ordering matches the realistic "newest backup wins" rule.
#[test]
fn prune_keeps_newest_and_leaves_live_db_and_unrelated_files_alone() {
    let dir = tempfile::tempdir().unwrap();
    let names = [
        "otel-logs.db.20260801T000000Z.bak",
        "otel-logs.db.20260802T000000Z.bak",
        "otel-logs.db.20260803T000000Z.bak",
    ];
    for (i, name) in names.iter().enumerate() {
        let path = dir.path().join(name);
        std::fs::write(&path, "x").unwrap();
        // Distinct mtimes: day 1 < day 2 < day 3 (relative to a fixed anchor).
        let mtime = std::fs::FileTimes::new()
            .set_modified(std::time::UNIX_EPOCH + Duration::from_secs(86_400 * (i as u64 + 1)));
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(mtime)
            .unwrap();
    }
    // The live database and an unrelated file must survive pruning.
    std::fs::write(dir.path().join("otel-logs.db"), "live").unwrap();
    std::fs::write(dir.path().join("notes.txt"), "notes").unwrap();

    // keep = 2 retains the newest two (day 2, day 3) and prunes day 1.
    let removed = backup::prune_old_backups(dir.path(), 2).unwrap();
    assert_eq!(removed, 1);
    assert!(!dir.path().join(names[0]).exists(), "oldest backup pruned");
    assert!(dir.path().join(names[1]).exists());
    assert!(dir.path().join(names[2]).exists());
    assert!(dir.path().join("otel-logs.db").exists());
    assert!(dir.path().join("notes.txt").exists());

    // keep == 0 disables pruning entirely: a full set survives untouched.
    std::fs::write(dir.path().join(names[0]), "x").unwrap();
    assert_eq!(backup::prune_old_backups(dir.path(), 0).unwrap(), 0);
    assert!(dir.path().join(names[0]).exists());
    assert!(dir.path().join(names[1]).exists());
    assert!(dir.path().join(names[2]).exists());
}

/// Backup artifact names are unique per second and match the retention prefix.
#[test]
fn backup_filenames_are_unique_and_retention_friendly() {
    // 2026-08-30T12:34:56Z = 1788093296 epoch seconds.
    let plain = backup::backup_filename(1_788_093_296, false);
    assert_eq!(plain, "otel-logs.db.20260830T123456Z.bak");
    let encrypted = backup::backup_filename(1_788_093_296, true);
    assert_eq!(encrypted, "otel-logs.db.20260830T123456Z.otsb");
    // Never collides with the live database file.
    assert_ne!(plain, "otel-logs.db");
}

/// Two backups within the same second must not overwrite each other: the
/// collision-aware path suffixes with `-<n>` and stays retention-friendly.
#[test]
fn backup_paths_never_collide_within_the_same_second() {
    let dir = tempfile::tempdir().unwrap();
    let epoch = 1_788_093_296;

    let first = backup::unique_backup_path(dir.path(), epoch, false);
    std::fs::write(&first, "x").unwrap();
    let second = backup::unique_backup_path(dir.path(), epoch, false);
    assert_ne!(first, second, "a taken name must not be reused");
    assert!(second.file_name().unwrap().to_string_lossy().contains("-1"));
    std::fs::write(&second, "x").unwrap();
    let third = backup::unique_backup_path(dir.path(), epoch, false);
    assert!(third.file_name().unwrap().to_string_lossy().contains("-2"));
    // Every variant stays inside the retention prefix.
    for name in [&first, &second, &third] {
        assert!(
            name.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("otel-logs.db.")
        );
    }
}

/// Not-a-database and missing files produce specific errors, not raw codes.
#[test]
fn verify_reports_not_a_database_and_missing_files_clearly() {
    let dir = tempfile::tempdir().unwrap();
    let garbage = dir.path().join("garbage.db");
    std::fs::write(&garbage, b"definitely not a sqlite file").unwrap();
    match backup::verify(&garbage) {
        Err(BackupError::NotADatabase(_)) => {}
        other => panic!("expected NotADatabase, got {other:?}"),
    }

    let missing = dir.path().join("missing.db");
    match backup::verify(&missing) {
        Err(BackupError::Io(_)) => {}
        other => panic!("expected an Io error for a missing file, got {other:?}"),
    }
}

/// The backup API requires an existing source; a missing source is a clear
/// error instead of an obscure sqlite code.
#[test]
fn backup_missing_source_is_a_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let outcome = backup::backup_to(&dir.path().join("nope.db"), &dir.path().join("out.bak"));
    assert!(matches!(outcome, Err(BackupError::Io(_))));
}

/// The snapshot bytes are what `restore` installs: after copying, re-verifying
/// the restored database yields the identical sha256 as the snapshot.
#[test]
fn restore_copies_the_snapshot_byte_for_byte() {
    let dir = tempfile::tempdir().unwrap();
    let db = populated_db(dir.path(), "otel-logs.db");
    let snapshot = dir.path().join("snapshot.bak");
    backup::backup_to(&db, &snapshot).unwrap();

    let clean = dir.path().join("restored");
    let report = backup::restore_into(&snapshot, &clean, None).unwrap();
    assert_eq!(report.verified_before.sha256, report.verified_after.sha256);
}
