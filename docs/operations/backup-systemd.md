# Backups on systemd (runbook)

This guide configures **automated daily backups** of an `otel-sqlite` server
managed by systemd, using the oneshot service + timer shipped in
`deploy/systemd/`. It covers install, verification, encryption, retention,
alerting and restore.

The backup itself is the standard contract described in
[docs/operations/backup-restore.md](backup-restore.md): the binary opens the
live database **read-only** and copies a consistent snapshot through the
SQLite Online Backup API, so the running server is never paused.

## 1. Prerequisites

* The `otel-sqlite` binary at `/usr/local/bin/otel-sqlite`.
* A dedicated unprivileged user `otel-sqlite` (the runtime image creates
  `uid 10001`; for a host install: `useradd --system --uid 10001 --create-home
  --shell /usr/sbin/nologin otel-sqlite`).
* A data directory `/data` (database + WAL) owned by `otel-sqlite`, and a
  **separate** backup directory `/backups` also owned by `otel-sqlite` on a
  volume with at least 2× the database size of free space.
* The server configured via `OTEL_SQLITE_CFG_PATH` (see
  `deploy/systemd/otel-sqlite.service`).

## 2. Install the units

```sh
sudo cp deploy/systemd/otel-sqlite-backup.service \
        deploy/systemd/otel-sqlite-backup.timer \
        /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now otel-sqlite-backup.timer
```

`enable --now` starts the timer immediately; because of `Persistent=true` a
missed run (host was down at the scheduled time) fires on the next boot.

## 3. Verify it works

```sh
# Timer is active and armed.
systemctl list-timers otel-sqlite-backup.timer

# Run a backup right now (oneshot) and inspect the result.
sudo systemctl start otel-sqlite-backup.service
systemctl status otel-sqlite-backup.service
systemctl show otel-sqlite-backup.service -p Result -p ExecMainStatus

# Artifacts appear under /backups; each is verified before success.
ls -l /backups/otel-logs.db.*.bak
```

A healthy run prints a summary (backup path, sha256, `integrity: ok`, schema
version, row counts) to the journal:

```sh
journalctl -u otel-sqlite-backup.service -n 20
```

## 4. Encryption (optional but recommended)

1. Generate a 32-byte key and place it **outside** the data directory, readable
   only by the backup user:

   ```sh
   sudo install -o otel-sqlite -g otel-sqlite -m 0600 /dev/null \
       /etc/otel-sqlite/backup.key
   openssl rand -out /etc/otel-sqlite/backup.key 32
   ```

2. Uncomment the encrypted `ExecStart` in
   `/etc/systemd/system/otel-sqlite-backup.service`:

   ```ini
   ExecStart=/usr/local/bin/otel-sqlite backup \
       --db /data/otel-logs.db \
       --out /backups \
       --key-file /etc/otel-sqlite/backup.key \
       --keep 30
   ```

3. Reload and re-verify:

   ```sh
   sudo systemctl daemon-reload
   sudo systemctl start otel-sqlite-backup.service
   ls -l /backups/otel-logs.db.*.otsb     # .otsb = encrypted artifact
   ```

Backups then carry the `"OTSQBAK1"` magic; a wrong key or a tampered artifact
is rejected by the AEAD authentication on restore.

## 5. Retention and disk capacity

`--keep 30` keeps the newest 30 artifacts per run and prunes the rest. Budget
the backup volume as:

```text
backup volume ≥ (2 × database size) + (keep × average artifact size)
```

The backup fails loudly (non-zero exit) on disk-full and never reports success
for an unverified artifact. Monitor the volume with your regular disk alerts.

## 6. Alerting on failure

A failed backup exits non-zero. Wire an `OnFailure=` unit to page:

```ini
# /etc/systemd/system/otel-sqlite-backup-notify.service
[Unit]
Description=Alert on failed otel-sqlite backup
OnFailure=   # (see note)

[Service]
Type=oneshot
ExecStart=/usr/local/bin/otel-backup-alert.sh  # your paging/email/script hook
```

```ini
# /etc/systemd/system/otel-sqlite-backup.service  (add)
[Unit]
OnFailure=otel-sqlite-backup-notify.service
```

Alternatively scrape `systemctl show otel-sqlite-backup.service -p Result`
from your monitoring agent (e.g. node_exporter textfile / a systemd exporter)
and alert when `Result != success`.

## 7. Restore

Restore is **offline**: it writes the database into a clean directory while the
server is stopped.

```sh
# 1. Stop the server (graceful drain + final WAL truncate).
sudo systemctl stop otel-sqlite.service

# 2. Pick a backup (plaintext .bak or encrypted .otsb) and restore into a
#    clean directory. For encrypted backups pass --key-file.
sudo -u otel-sqlite otel-sqlite restore \
    --backup /backups/otel-logs.db.20260830T121426Z-1.otsb \
    --dir /data/restored \
    --key-file /etc/otel-sqlite/backup.key

# 3. Verify the restored database before trusting it.
otel-sqlite verify --db /data/restored/otel-logs.db
#    Confirm: integrity=ok, foreign keys=0, schema=003 (current), rows match
#    the values printed by the matching `backup` run.

# 4. Move it into place and start the server.
sudo -u otel-sqlite mv /data/otel-logs.db /data/otel-logs.db.pre-restore
sudo -u otel-sqlite mv /data/restored/otel-logs.db /data/otel-logs.db
sudo systemctl start otel-sqlite.service
```

Keep the failed database (`otel-logs.db.pre-restore`) and the backup artifact
until the next successful backup.

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `backup` exits "cannot open …" | Backup user cannot read `/data/otel-logs.db` (or the `-shm` sidecar is missing because the server never started) | Start the server once; check `/data` ownership (`chown otel-sqlite`); ensure `ReadOnlyPaths=/data` does not block a second process reading it (it does not — read is allowed). |
| `backup` exits "backup source does not exist" | Wrong `--db` path or `WorkingDirectory` | Confirm `ExecStart` `--db /data/otel-logs.db`; `ls -l /data/otel-logs.db`. |
| Artifact is `-1.bak` suffixed | Two runs in the same second | Harmless; the tool never overwrites. |
| Timer never fires | Timer not enabled, or `OnCalendar` misread | `systemctl list-timers --all`; `systemctl status otel-sqlite-backup.timer`. |
| Restore refuses the directory | Directory not empty | Use a fresh empty dir; the tool never overwrites. |
| Restore fails with "wrong key or corrupted" | Key mismatch or damaged artifact | Re-check `--key-file` bytes (`openssl rand -out … 32`, length 32); take a new backup. |

## 9. Reference

* Runbook & contract: `docs/operations/backup-restore.md`
* Units: `deploy/systemd/otel-sqlite-backup.{service,timer}`,
  `deploy/systemd/otel-sqlite.service`
* Schema migration/rollback: `docs/operations/upgrades.md`