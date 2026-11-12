# Schema upgrades, compatibility and rollback

Applies to the SQLite schema in `crates/otel-sqlite-storage/migrations/*`.

## How migrations run

Forward-only, additive snapshots applied by the SQLite writer at startup.
Each is stamped in `schema_migrations` (`version`, `applied_at`,
`description`). `Storage::open` blocks until open, migration, the startup
`wal_checkpoint(TRUNCATE)` and owner-only permission hardening all succeed —
schema or checkpoint problems fail startup, not first traffic or shutdown.

There are two paths, both ending at the same schema:

- **Fresh database:** applies the squashed logs-only baseline
  (`migrations/baseline_006.sql`) once. It never creates the metric signal
  that the old `000`-`004` chain used to build and `005` used to drop. The
  baseline is then stamped as satisfying every migration through `006`, with
  description `Satisfied by squashed logs-only baseline` — those historical
  DDL statements did not run.
- **Existing database:** replays the missing files in
  `migrations/upgrades/` (the frozen `000`-`006` chain), then any newer
  migrations. Databases stamped `005` apply `006`, which reduces the search
  index and starts the bounded background rebuild.

Once stamped, never re-run. **Never edit an applied migration or the
baseline** — SQLite can't tell an edited `001` from the original; evolution
always adds a new `NNN_*.sql` under `migrations/upgrades/`. Squashing again
means a new baseline plus a deliberate version bump, not an edit to the
current one.

## Supported versions

Binary is compatible when its `MIGRATIONS` contains every stamped version
(i.e. binary **at or above** the DB's schema). Current: **006**.

| Version | Contents |
|---|---|
| `000` | Legacy baseline (frozen, `migrations/upgrades/`): resources, log records, FTS, scope/metric tables, views |
| `001` | FTS rebuild as contentless FTS5 (`contentless_delete=1`) + triggers + backfill (**breaking** FTS shape) |
| `003` | New log/scope columns (`scope_attributes_json`, `scope_schema_url`, …) with defaults; views gain columns (old `SELECT`s keep working) |
| `005` | **Drops OTLP metrics signal** (views `metrics`/`metric_buckets`, tables `scope`/`metric`/`metric_series`/`metric_data_point). **Destructive, irreversible.** Logs, `logs` view, FTS untouched. |
| `006` | Search index becomes optional and reconciled; the default drops to body-only columns, with a background rebuild after a mode change |

Fresh databases do not execute `000`-`006`; they apply the logs-only baseline
(`migrations/baseline_006.sql`) and stamp the versions it satisfies. The prior
frozen baseline is `migrations/baseline_005.sql`; it is kept for reference, not
executed. (`002`/`004` were metrics-only and are gone with `005`. Full upgrade
history lives in `migrations/upgrades/` + git.)

Incompatible changes:

- `005` deletes stored metric rows; metrics exports return `UNIMPLEMENTED`.
  Back up first if that data matters.
- `006` drops the existing search index. On a large database the one-time
  `DROP TABLE` can take a while; run `otel-sqlite check` first and keep the
  backup. After startup the index is `rebuilding`; `logs_fts` row counts are
  lower than `log_event` counts until the background rebuild finishes. Search
  covers new rows immediately; historical rows become searchable as the
  rebuild advances.
- Body-only search (the default) no longer matches metadata column filters
  such as `MATCH 'service_name:checkout'`. Set `[search] mode = "full"` before
  the first start to keep the six-column index.
- Downgrading the **binary** below the DB schema is unsupported — old binary
  fails at startup. Only rollback is **restore from pre-upgrade backup**.
- Never open a newer-schema DB with an older binary.

## Upgrade procedure

Before replacing the binary, run the non-mutating preflight:

```sh
otel-sqlite check --config /etc/otel-sqlite/otel-sqlite.toml
```

It loads the effective config exactly like the server, parses the TLS
certificate/key (and client CA), loads the token file, and inspects the
database schema **read-only** — no listener, no workers, no `-wal`/`-shm`
sidecars, no schema change. Exit status is `0` when nothing blocks startup,
`1` when config/TLS/auth/downgrade checks fail, and `2` for usage errors.
`--json` emits the same report machine-readably; pending migrations are
reported as warnings (startup will apply them), while a database stamped by a
newer binary is a hard failure.

1. Backup + record version:
   ```sh
   otel-sqlite backup --db /data/otel-logs.db --out /backups --keep 30
   otel-sqlite verify --db /data/otel-logs.db   # note schema stamp
   ```
2. Stop gracefully (systemd/K8s/SIGTERM) — drains, commits acked records,
   truncates WAL.
3. Replace binary / image.
4. Run `otel-sqlite check --config ...` against the new binary before
   starting; then start — `Storage::open` applies pending migrations in order,
   one txn each, then folds the startup WAL and hardens the files. Readiness
   is published only after all of those succeed.
5. Verify: `otel-sqlite verify --db /data/otel-logs.db` shows `schema: 006`,
   `integrity: ok`, 0 FK violations, row counts matching pre-upgrade (+
   restart-window traffic).
6. Keep the pre-upgrade backup until the *next* verified backup succeeds.

## Rollback limits

No `downgrade` path — DDL is not rolled back. Only mechanism is restore:

1. Stop server. 2. `otel-sqlite restore --backup /backups/otel-logs.db.<UTC>.bak
   --dir /data/restored` (carries backup-time schema). 3. Move into place,
   start the **old** binary, verify. Restore replaces the DB; it never
   downgrades a schema in place.

Coverage: `storage/tests/migrations.rs` creates fresh databases from the
logs-only baseline (no metric objects, FTS triggers and scope columns present)
and replays `000`–`004` → `005` through `Storage::open`: order + stamp, log
rows survive, metric objects gone, FTS backfilled/searchable, `integrity_check`
ok, FK clean, idempotent reopen.
