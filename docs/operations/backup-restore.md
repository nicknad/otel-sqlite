# Backup & restore runbook

**Contract:** the SQLite Online Backup API is the single snapshot method,
via `otel-sqlite backup | restore | verify` — works live **without pausing
ingestion**, needs no external `sqlite3` (slim image ships none).

```sh
otel-sqlite backup --db /data/otel-logs.db --out /backups --keep 30
otel-sqlite backup --db /data/otel-logs.db --out /backups --key-file /keys/backup.key --keep 30
otel-sqlite verify --db /data/otel-logs.db
otel-sqlite restore --backup /backups/otel-logs.db.<UTC>.otsb --dir /data/restored --key-file /keys/backup.key
```

All exit non-zero on failure; `--json` prints a machine-readable report for
alerting. See [upgrades](upgrades.md) for migration/rollback limits.

## 1. Online backup

`backup` opens the source **read-only**, copies page-by-page through the
Backup API (same as `sqlite3 src ".backup dst"`). Writer never pauses;
snapshot is consistent at final-pass completion. Requires the `-shm`
sidecar to exist (always true after first startup) and a writable dest
volume (see §4).

Output: `otel-logs.db.<UTC>.bak` (plaintext) or `.otsb` (encrypted) —
single self-contained rollback-journal file (WAL folded in, no sidecars).
Verified before success: `integrity_check = ok`, 0 FK violations, row
counts, schema version, SHA-256.

## 2. WAL and sidecar handling

Backup reads through the WAL — committed-but-uncheckpointed frames are
included. **Never copy `-wal`/`-shm` directly** — detached sidecars
corrupt. Snapshots are checkpointed to rollback-journal mode, restorable
anywhere; on restore the server re-enables WAL at startup.

## 3. Restore (offline)

1. Stop server (graceful drain + WAL truncate).
2. Restore into an **absent or empty** dir (refuses non-empty / existing DB):
   `otel-sqlite restore --backup … --dir /data/restored`.
3. Verified **before and after** copy. Point server at restored DB
   (`sqlite_path` or move into place), start, then
   `otel-sqlite verify --db /data/restored/otel-logs.db` (compare rows to
   the `backup` report).

## 4. Disk, retention, alerting

- **Sizing:** snapshot ≈ live DB size. Budget **2× DB size** free plus
  `keep × avg artifact`. Disk-full aborts non-zero, never reports a
  truncated artifact as success. No pre-check — monitor the volume.
- **Retention:** `--keep N` prunes beyond newest `N` (prefix
  `otel-logs.db.` only; `--keep 0` disables).
- **Alerting:** alert on job failure (systemd `OnFailure`, CronJob failure,
  non-zero exit) and on `verify` non-zero. Every release requires a
  restore-and-validate.

Platform guides: [systemd](backup-systemd.md),
[Kubernetes](backup-kubernetes.md) (`deploy/systemd/`,
`deploy/kubernetes/`).

## 5. Integrity, encryption, access

`verify` checks `integrity_check`, `foreign_key_check`, row counts
(`log_event`, `log_resource`, `logs_fts`), schema stamp, SHA-256. For
encrypted backups the hash covers the `.otsb` as stored; row/integrity
fields describe decrypted contents.

Encryption: optional AES-256-GCM. `openssl rand -out /keys/backup.key 32`
(`0600`, outside data dir); pass `--key-file` to `backup` + `restore`.
Artifacts carry `OTSQBAK1` magic + nonce; wrong key/tamper fails
authentication, never silently trusted. Artifacts created `0600` (on
Windows restrict the dir via ACLs); streams in 64 KiB frames (constant
memory). Consider volume-level encryption for plaintext artifacts.

Alternatives producing consistent snapshots: `sqlite3 src ".backup dst"`
(same API) or `VACUUM INTO` with server stopped; verify decrypted output
with `otel-sqlite verify` before trusting.

Tests: `storage/tests/backup.rs` (engine, corruption, encryption,
pruning, online consistency); `otel-sqlite/tests/backup_restore.rs`
(real binary under load, restart, encrypted round-trip).
