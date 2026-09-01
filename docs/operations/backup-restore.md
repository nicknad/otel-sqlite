# Backup & restore runbook

**Contract:** the SQLite Online Backup API is the single, consistent snapshot
method. It is exercised by the `otel-sqlite backup | restore | verify`
subcommands, which work on the live database **without pausing ingestion** and
need no external `sqlite3` binary (the slim runtime image ships none).

```sh
# Take an online, verified snapshot while the server keeps ingesting.
otel-sqlite backup --db /data/otel-logs.db --out /backups --keep 30

# Same, encrypted at rest with AES-256-GCM.
otel-sqlite backup --db /data/otel-logs.db --out /backups \
    --key-file /keys/backup.key --keep 30

# Validate any SQLite file (live database, backup, or restored database).
otel-sqlite verify --db /data/otel-logs.db

# Restore a (possibly encrypted) backup into a CLEAN directory.
otel-sqlite restore --backup /backups/otel-logs.db.20260830T123456Z.otsb \
    --dir /data/restored --key-file /keys/backup.key
```

Every subcommand exits non-zero on failure and prints a machine-readable JSON
report with `--json` — the hook cron/systemd/Kubernetes alert on.

---

## 1. Online backup while ingestion is active

`backup` opens the source database **read-only** and copies it page by page
through the Online Backup API. The live writer never pauses: the snapshot is
exactly consistent at the moment the final copy pass completes. This is the
same mechanism as `sqlite3 source.db ".backup dest.db"`.

**Prerequisites**

* The server must have opened the database at least once so the WAL `-shm`
  sidecar exists (it always does — migrations run at first startup).
* The destination directory must be writable and on a volume with enough free
  space (see [Disk capacity](#6-disk-capacity-and-backup-retention)).

**Output**

* Artifact: `otel-logs.db.<YYYYMMDDTHHMMSSZ>.bak` (plaintext) or
  `… .otsb` (encrypted).
* The snapshot is normalized to a single, self-contained rollback-journal
  file: `-wal`/`-shm` sidecars are folded in and never copied.
* Verified before it is reported successful: `PRAGMA integrity_check` = `ok`,
  `PRAGMA foreign_key_check` = 0 violations, per-table row counts, schema
  version, and a SHA-256 of the artifact.

## 2. WAL and sidecar handling

* `backup` reads through the WAL, so committed-but-uncheckpointed frames are
  included in the snapshot. **Never copy `otel-logs.db-wal` / `otel-logs.db-shm`
  directly** — copying sidecars without the coordinating database produces a
  corrupt backup.
* After the copy, the snapshot is checkpointed and switched to rollback-journal
  mode, so it is a **single self-contained file** that can be opened, verified
  and restored anywhere without sidecars.
* On restore the database is copied back as a single file; the server's own
  startup re-enables WAL mode automatically.

## 3. Restore into a clean directory

`restore` is an **offline** operation on the target:

1. Stop the server (`systemctl stop otel-sqlite` / SIGTERM; Kubernetes handles
   this through `terminationGracePeriodSeconds`).
2. Choose a destination directory that is **absent or empty** — the restore
   refuses a non-empty directory and an already-existing `otel-logs.db`.
3. `otel-sqlite restore --backup … --dir /data/restored`.
4. The backup is verified **before** and **after** the copy; the report prints
   both.
5. Point the server at the restored database (e.g. move it into place or set
   `sqlite_path`) and start it. Verify with `otel-sqlite verify`.

## 4. Integrity checks and row-count/content validation

`verify` runs on a live database, a backup, or a restored database:

* `PRAGMA integrity_check` — must read `ok`.
* `PRAGMA foreign_key_check` — must read 0 violations.
* Row counts for every canonical table plus the search index
  (`log_event`, `log_resource`, `metric_data_point`, `metric_series`,
  `metric`, `scope`, `logs_fts`).
* The highest `schema_migrations` stamp.
* A SHA-256 of the file bytes (compare against the `backup` report to confirm
  the artifact was not altered in storage).

Recommended restore validation:

```sh
otel-sqlite verify --db /data/restored/otel-logs.db
# Compare "rows:" against the values printed by the matching `backup` report.
```

## 5. Encryption and access control

**Encryption.** Optional AES-256-GCM at rest. Generate a key (exactly 32 bytes)
and keep it out of the data directory:

```sh
openssl rand -out /keys/backup.key 32     # or: head -c32 /dev/urandom > /keys/backup.key
chmod 0600 /keys/backup.key
```

Pass `--key-file` to `backup` and to `restore`. The encrypted artifact is
self-describing (`"OTSQBAK1"` magic + version + nonce + ciphertext), and
`restore`/`decrypt` authenticate the ciphertext — a wrong key or a tampered
file is rejected, never silently trusted. Backup files are created with
owner-only permissions (`0600`) on unix.

**Access control.**

* Keep backup keys out of the data directory and readable only by the backup
  user; the live database user should not need the key.
* Backups inherit the `0600` of the artifact plus whatever the destination
  directory's ACLs allow. On Windows, restrict the backup directory with
  filesystem ACLs (the binary cannot set POSIX modes there).
* Consider volume/disk-level encryption (e.g. LUKS, BitLocker) so backups are
  encrypted at rest even when the artifact itself is plaintext.
* **Memory note:** encryption reads the snapshot into memory (≈ the database
  size). For multi-GB databases prefer volume-level encryption or an external
  streamed tool (`openssl enc`, `age`) piped from a filesystem copy.

**Alternatives.** Any tool that produces a consistent SQLite snapshot can be
used instead: `sqlite3 source.db ".backup dest.db"` (the same API), or
`VACUUM INTO 'dest.db'` when the server is stopped. If you wrap the backup with
external encryption, verify with `otel-sqlite verify` on the decrypted file
before trusting it.

## 6. Disk capacity and backup retention

* **Sizing:** a snapshot is ≈ the live database file size (freelist pages are
  copied). Budget at least **2× the database size** of free space on the backup
  volume, plus headroom for retention growth between prunes.
* **Failure behavior:** on disk-full the backup aborts with a clear error and
  a non-zero exit — it never produces a truncated artifact that passes
  verification (the artifact is verified before success is reported). No
  automatic capacity pre-check exists; monitor the backup volume with your
  regular disk alerts.
* **Retention:** pass `--keep N` to `backup`. After a successful run it prunes
  everything in the output directory beyond the newest `N` artifacts (matching
  the `otel-logs.db.` prefix only — the live database and unrelated files are
  untouched). `--keep 0` disables pruning.
  * systemd timer: see `deploy/systemd/otel-sqlite-backup.*`.
  * Kubernetes: see `deploy/kubernetes/otel-sqlite-backup-cronjob.yaml`.
* **Alerting:** alert on the backup job failing (systemd `OnFailure`/timer
  failure, cron exit code, CronJob failure) and on `verify` exiting non-zero.
  The release gate in [`CONTRIBUTING.md`](../../CONTRIBUTING.md) requires a
  successful restore-and-validate as part of every release.

## 7. Schema migration and rollback limits

Upgrades run automatically at startup (`Storage::open` applies pending
migrations). See [docs/operations/upgrades.md](upgrades.md) for:

* the per-version compatibility table and incompatible changes;
* the recommended upgrade procedure (backup → stop → replace → start → verify);
* rollback limits: migrations are forward-only; the only rollback is restoring
  a pre-upgrade backup with the pre-upgrade binary.

## 8. Scheduled examples

Step-by-step platform guides:

* **systemd**: [docs/operations/backup-systemd.md](backup-systemd.md) — install
  the oneshot service + timer, verify runs, enable encryption, wire `OnFailure`
  alerting, and restore.
* **Kubernetes**: [docs/operations/backup-kubernetes.md](backup-kubernetes.md) —
  deploy the backup PVC + CronJob, trigger/verify runs, enable the key Secret,
  alert on failed/stale jobs, and restore.

**systemd timer** (files in `deploy/systemd/`):

```sh
# /etc/systemd/system/otel-sqlite-backup.timer
[Unit]
Description=Nightly otel-sqlite backup

[Timer]
OnCalendar=daily
RandomizedDelaySec=30min
Persistent=true

[Install]
WantedBy=timers.target
```

**Kubernetes CronJob** (file in `deploy/kubernetes/`): a daily job that mounts
the server's persistent volume and the backup volume, runs `backup` online
while the server pod ingests, and prunes with `--keep`.

## Verification of this contract

* `crates/otel-sqlite-storage/tests/backup.rs` — engine: snapshot equals
  source, restore into a clean directory, corruption rejection, encryption
  round-trip, retention pruning, online-while-ingesting consistency.
* `crates/otel-sqlite/tests/backup_restore.rs` — the real binary: backup taken
  while load is running, restore, restart on the restored database, new
  traffic persists; encrypted round-trip; `verify` exits non-zero on
  corruption.