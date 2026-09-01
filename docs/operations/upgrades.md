# Schema upgrades, compatibility and rollback

**Applies to:** the SQLite schema managed by `otel-sqlite-storage` migrations
(`crates/otel-sqlite-storage/migrations/*`).

## How migrations run

Migrations are **forward-only, additive snapshots** applied by the SQLite
writer at startup. Every applied migration is stamped in `schema_migrations`
(`version`, `applied_at`, `description`). `Storage::open` refuses to become
ready until the database is open **and** migrated, so a schema problem fails
process startup instead of surfacing on first traffic.

Once a migration is stamped, it is never re-run. **Never edit an applied
migration file** — SQLite cannot tell an edited `001` from the original, and
re-running edited DDL over migrated data is the fastest way to corrupt a
deployment. Schema evolution always adds a new `NNN_*.sql` file.

## Supported schema versions

Every database is stamped with the *highest* applied migration. A binary is
compatible with a database when the binary's `MIGRATIONS` contains every
stamped version — i.e. the binary is **at or above** the database's schema
version. Current schema version: **003**.

| Version | Contents | Additive? | Notes |
|---|---|---|---|
| `000` | Baseline canonical schema: resources, log events, contentless `logs_fts`, scope/metric/series/data-point tables, read-side views | — | The squashed starting point. |
| `001` | Recreates `logs_fts` as contentless FTS5 with `contentless_delete = 1` and installs incremental INSERT/DELETE triggers; backfills existing rows | **No** | Replaces the old FTS table shape. A database stamped `000` cannot be read by a binary that only knows `000` after this migration exists — the FTS table and trigger shape differ. |
| `002` | Adds `idx_metric_dp_timestamp` (metric retention access path) | Yes | Pure index addition; no data or view change. |
| `003` | Adds `log_event.scope_attributes_json`, `log_event.scope_schema_url`, `scope.attributes_json`, `metric.metadata_json`; recreates `logs`/`metrics` views to expose them | Yes (tables) / view shape changes | New columns carry defaults (`'{}'`/NULL) so existing rows stay valid; the recreated views only add columns, so old `SELECT` queries against them keep working. |

**Incompatible changes to be aware of:**

* Downgrading the **binary** below the database's schema version is not
  supported: the older binary does not know the newer columns/views and will
  fail at startup. The only rollback for a too-new database is **restore from a
  backup taken before the upgrade**.
* A database whose schema is *newer* than the binary must never be opened by
  that binary — treat a schema-version mismatch as a halt, not a best-effort
  read.

## Upgrade procedure (recommended)

1. **Take a backup** of the current database:
   ```sh
   otel-sqlite backup --db /data/otel-logs.db --out /backups --keep 30
   otel-sqlite verify --db /backups/otel-logs.db.<UTC>.bak
   ```
   Record the pre-upgrade schema version: `otel-sqlite verify --db /data/otel-logs.db`.
2. **Stop the server gracefully** (systemd `systemctl stop`, Kubernetes
   `kubectl rollout restart`, or SIGTERM). Graceful shutdown drains, commits
   acknowledged records and truncates the WAL.
3. **Replace the binary / image** with the new version.
4. **Start the server.** `Storage::open` applies any pending migrations in
   order, in one transaction each, and the process only becomes ready after
   the schema is current.
5. **Verify post-upgrade:**
   ```sh
   otel-sqlite verify --db /data/otel-logs.db
   ```
   Confirm the report shows `schema: 003`, `integrity: ok`, `foreign keys: 0
   violation(s)`, and row counts that match the pre-upgrade numbers (plus any
   traffic that arrived during the restart window).
6. **Keep the pre-upgrade backup** until the *next* backup succeeds and
   verifies. Only then is the old snapshot safe to prune.

## Rollback limits

* Migrations are **irreversible by design** (SQLite DDL in this project is not
  rolled back by later migrations). There is no `downgrade` path.
* The only rollback mechanism is **restore from a pre-upgrade backup**:
  1. Stop the server.
  2. `otel-sqlite restore --backup /backups/otel-logs.db.<UTC>.bak --dir /data/restored`
     (the database it produces is schema version `000`–`002`, matching the
     pre-upgrade backup).
  3. Move the restored database into place, start the older binary, verify.
* Because the restored database is older than the new binary's schema, the
  restore procedure must be paired with the **old binary** — restore does not
  downgrade a schema, it replaces the database with one the old binary can
  read.

## Upgrade test coverage

`crates/otel-sqlite-storage/tests/migrations.rs` builds a database stamped at
every historical version (`000`, `001`, `002`), seeds era-appropriate data,
and reopens it through the production `Storage::open` path. It asserts:

* migrations apply in order and land on the current stamp (`003`);
* seeded log and metric rows survive with identical counts and content;
* the search index is backfilled and searchable without a manual rebuild;
* `PRAGMA integrity_check` is `ok` and `PRAGMA foreign_key_check` is clean;
* the `003` columns are present and visible on the read-side views;
* reopening a current-schema database is a no-op (idempotent).