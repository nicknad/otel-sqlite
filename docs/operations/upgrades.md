# Schema upgrades, compatibility and rollback

Applies to the SQLite schema in `crates/otel-sqlite-storage/migrations/*`.

## How migrations run

Forward-only, additive snapshots applied by the SQLite writer at startup.
Each is stamped in `schema_migrations` (`version`, `applied_at`,
`description`). `Storage::open` blocks until open **and** migrated — schema
problems fail startup, not first traffic.

Once stamped, never re-run. **Never edit an applied migration** — SQLite
can't tell an edited `001` from the original; evolution always adds a new
`NNN_*.sql`.

## Supported versions

Binary is compatible when its `MIGRATIONS` contains every stamped version
(i.e. binary **at or above** the DB's schema). Current: **005**.

| Version | Contents |
|---|---|
| `000` | Baseline: resources, log events, FTS, scope/metric tables, views |
| `001` | FTS rebuild as contentless FTS5 (`contentless_delete=1`) + triggers + backfill (**breaking** FTS shape) |
| `003` | New log/scope columns (`scope_attributes_json`, `scope_schema_url`, …) with defaults; views gain columns (old `SELECT`s keep working) |
| `005` | **Drops OTLP metrics signal** (views `metrics`/`metric_buckets`, tables `scope`/`metric`/`metric_series`/`metric_data_point). **Destructive, irreversible.** Logs, `logs` view, FTS untouched. |

(`002`/`004` were metrics-only and are gone with `005`. Full DDL history
in `migrations/` + git.)

Incompatible changes:

- `005` deletes stored metric rows; metrics exports return `UNIMPLEMENTED`.
  Back up first if that data matters.
- Downgrading the **binary** below the DB schema is unsupported — old binary
  fails at startup. Only rollback is **restore from pre-upgrade backup**.
- Never open a newer-schema DB with an older binary.

## Upgrade procedure

1. Backup + record version:
   ```sh
   otel-sqlite backup --db /data/otel-logs.db --out /backups --keep 30
   otel-sqlite verify --db /data/otel-logs.db   # note schema stamp
   ```
2. Stop gracefully (systemd/K8s/SIGTERM) — drains, commits acked records,
   truncates WAL.
3. Replace binary / image.
4. Start — `Storage::open` applies pending migrations in order, one txn
   each; readiness waits for current schema.
5. Verify: `otel-sqlite verify --db /data/otel-logs.db` shows `schema: 005`,
   `integrity: ok`, 0 FK violations, row counts matching pre-upgrade (+
   restart-window traffic).
6. Keep the pre-upgrade backup until the *next* verified backup succeeds.

## Rollback limits

No `downgrade` path — DDL is not rolled back. Only mechanism is restore:

1. Stop server. 2. `otel-sqlite restore --backup /backups/otel-logs.db.<UTC>.bak
   --dir /data/restored` (carries backup-time schema). 3. Move into place,
   start the **old** binary, verify. Restore replaces the DB; it never
   downgrades a schema in place.

Coverage: `storage/tests/migrations.rs` replays `000`–`004` → `005` through
`Storage::open`: order + stamp, log rows survive, metric objects gone, FTS
backfilled/searchable, `integrity_check` ok, FK clean, idempotent reopen.
