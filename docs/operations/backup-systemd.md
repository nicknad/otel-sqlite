# Backups on systemd (runbook)

Daily automated backups via the oneshot service + timer in
`deploy/systemd/`. Contract in [backup-restore](backup-restore.md): binary
opens the live DB **read-only** through the Online Backup API — server
never pauses.

## 1. Install

Prereqs: binary at `/usr/local/bin/otel-sqlite`, user `otel-sqlite`,
`/data` (DB + WAL) and separate `/backups` (≥2× DB + `keep × avg`)
both owned by the user, server configured via `OTEL_SQLITE_CFG_PATH`.

```sh
sudo cp deploy/systemd/otel-sqlite-backup.service \
        deploy/systemd/otel-sqlite-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now otel-sqlite-backup.timer  # Persistent=true catches up after downtime
```

Verify: `systemctl list-timers otel-sqlite-backup.timer`,
`sudo systemctl start otel-sqlite-backup.service`,
`systemctl show … -p Result -p ExecMainStatus`,
`ls -l /backups/otel-logs.db.*.bak`, `journalctl -u otel-sqlite-backup.service -n 20`
(summary: path, sha256, `integrity: ok`, schema, row counts).

## 2. Encryption (recommended)

```sh
sudo install -o otel-sqlite -g otel-sqlite -m 0600 /dev/null /etc/otel-sqlite/backup.key
openssl rand -out /etc/otel-sqlite/backup.key 32
```

Uncomment the encrypted `ExecStart` (`--key-file /etc/otel-sqlite/backup.key`),
`daemon-reload`, `start`, confirm `.otsb` artifacts (`OTSQBAK1` magic;
wrong key/tamper rejected on restore).

`--keep 30` prunes beyond newest 30. Backup fails non-zero on disk-full —
monitor the volume.

## 3. Alerting

Failed backup exits non-zero. Add `OnFailure=` to the **backup** unit:

```ini
# otel-sqlite-backup-notify.service
[Service]
Type=oneshot
ExecStart=/usr/local/bin/otel-backup-alert.sh
# otel-sqlite-backup.service [Unit]: OnFailure=otel-sqlite-backup-notify.service
```

Or scrape `systemctl show … -p Result` (node_exporter/systemd exporter),
alert on `Result != success`.

## 4. Restore (offline)

```sh
sudo systemctl stop otel-sqlite.service
sudo -u otel-sqlite otel-sqlite restore --backup /backups/otel-logs.db.<UTC>.otsb \
  --dir /data/restored --key-file /etc/otel-sqlite/backup.key  # omit key if .bak
otel-sqlite verify --db /data/restored/otel-logs.db  # integrity=ok, FK=0, schema=005, rows match backup
sudo -u otel-sqlite mv /data/otel-logs.db /data/otel-logs.db.pre-restore
sudo -u otel-sqlite mv /data/restored/otel-logs.db /data/otel-logs.db
sudo systemctl start otel-sqlite.service
```

Keep failed DB + artifact until next successful backup.

## 5. Troubleshooting

| Symptom | Fix |
|---|---|
| `cannot open …` | Start server once (creates `-shm`); `chown otel-sqlite /data` |
| `backup source does not exist` | Check `--db` path / `WorkingDirectory`; `ls -l /data/otel-logs.db` |
| Timer never fires | `list-timers --all`; `status otel-sqlite-backup.timer` |
| Restore refuses dir | Use fresh empty dir (never overwrites) |
| `wrong key or corrupted` | Re-check 32-byte key; take new backup |

Refs: [backup-restore](backup-restore.md),
`deploy/systemd/otel-sqlite-backup.{service,timer}`,
[upgrades](upgrades.md).
