# Rewrite plan: inline event attributes and native SQLite

## Purpose and starting point

This plan is the implementation checklist for replacing the event-attribute EAV
writes with one JSON value on `log_event`, then switching the SQL driver to
`github.com/mattn/go-sqlite3`.

The repository was inspected before writing this plan. The important facts are:

- `go test ./...` currently passes.
- The writer is already single-consumer/single-connection and uses prepared
  statements in `internal/storage/sqlite`.
- `log_event` currently has no `attributes_json`; `log_attr` is still created by
  migrations 001 and the embedded bootstrap SQL.
- `internal/storage/sqlite/write_batch_command.go` currently inserts one event
  and then one `log_attr` row per attribute. Resource attributes already use the
  existing verbose JSON representation and are not part of this rewrite.
- Migration SQL is duplicated: `migrations/*.sql` is the human/audit copy and
  `internal/storage/sqlite/migrator.go` contains the SQL actually executed.
  Keep those copies synchronized.
- Migration 003 owns the current `logs` view and the contentless `logs_fts`
  table. FTS is rebuilt by maintenance; it is not trigger-maintained anymore.
- `observed_timestamp_ns` is currently `NOT NULL`; do not bind `NULL` to it
  without a table-rebuild migration. `severity_text` is already nullable.
- The current Docker build explicitly uses `CGO_ENABLED=0`. There is no CI
  configuration in the repository; the Makefile and Dockerfile are the local
  build/test contract.
- `go.sum` already contains a `go-sqlite3` checksum, but it is not a direct
  dependency in `go.mod` and the vendored tree currently contains modernc,
  not go-sqlite3.

### Scope locked for this rewrite

In scope:

1. Add `log_event.attributes_json` with a compact JSON-object encoding.
2. Migrate existing `log_attr` rows into that column and remove `log_attr` from
   the final schema.
3. Make event writes one event insert per record, with no attribute inserts.
4. Remove the attribute delete path from retention.
5. Drop redundant severity text values where safe; preserve custom text.
6. Switch every runtime/test/tool SQL open to the native CGO driver.
7. Preserve resource deduplication, WAL/pragmas, single-writer behavior, the
   `logs` view contract, FTS maintenance, purge behavior, and queue behavior.

Explicitly out of scope:

- Turso, multi-writer/sharded storage, or a query API redesign.
- Attribute-key SQL indexes or a return to EAV. Future filtering can use
  `json_extract`/expression indexes for a small number of deliberately chosen
  keys.
- Resource JSON compaction, integer resource IDs, scope tables, body
  compression, or changing the resource attribute wire format.
- Making `observed_timestamp_ns` nullable. It is a separate schema-rebuild
  decision and the expected space saving is small.
- Replacing contentless FTS or adding attributes to FTS.

---

## Locked data-format decisions

### Event attributes

Store a single JSON object in `log_event.attributes_json`:

```json
{"http.method":"GET","http.status_code":500,"ok":true,"payload":{"$b":"AQI="}}
```

Encoding rules:

| Internal kind | JSON representation |
|---|---|
| `ValueString` | JSON string |
| `ValueInt` | JSON integer number (`int64`) |
| `ValueDouble` | JSON number |
| `ValueBool` | JSON boolean |
| `ValueBytes` | `{"$b":"<standard-base64>"}` |
| `ValueNull` | JSON `null` |
| no attributes | `{}` |

Additional rules:

- Use the existing `[]model.Attribute` representation; do not add a map to the
  hot-path model.
- Use `encoding/json` on a `map[string]any` (Go's JSON encoder sorts string map
  keys, making tests and migration output deterministic).
- Duplicate keys are last-write-wins, matching the order of the input attribute
  slice and the legacy rows' `id` order.
- Invalid legacy `value_type` values fail migration rather than silently
  dropping data.
- Non-finite doubles cannot be JSON numbers. Reject them as a write error and
  cover that behavior with a unit test; do not silently turn them into strings.
- Bytes use an explicit wrapper so they cannot be confused with ordinary JSON
  strings. The wrapper is reserved by this storage format.
- Keep resource `attributes_json` unchanged for this rewrite. Its current
  verbose shape is covered by existing E2E tests.

### Schema and migration policy

- Do not edit migrations 001–004. Existing database directories need those
  versions to remain historical and replayable.
- Add migration 005. A fresh database may create `log_attr` transiently while
  replaying the immutable 001–004 history, but after all migrations the final
  schema must not contain it.
- Apply migration 005 transactionally: add the column, backfill, update the
  view, remove attribute indexes/table, and record version 005 atomically.
- Make the migration safe for both a legacy database with rows and a fresh
  database with no rows.

### Native SQLite driver

- Driver import: `_ "github.com/mattn/go-sqlite3"`.
- Driver name: `sqlite3` at every `sql.Open` call.
- Use the driver's default bundled SQLite build; do not add `-tags libsqlite3`
  or require a system SQLite development library unless that choice is made
  explicitly later.
- Keep the existing explicit PRAGMAs and `SetMaxOpenConns(1)`. Do not replace
  tested behavior with undocumented DSN-only pragmas.
- CGO is a build prerequisite. The Docker builder must use `CGO_ENABLED=1`
  and `build-base`; verify the resulting Alpine binary's runtime libraries
  rather than assuming it is static.

---

## Phase 0 — Baseline and inventory

### Checklist

- [x] Confirm a clean baseline with `go test ./...`; record the command, Go
  version, OS/container image, and commit in the change notes.
- [x] Reproduce the existing sustained load profile from
  `docs/loadtest-baseline.md` and record process records/sec, ingest records/sec,
  queue depth, errors, elapsed time, database size, page count, page size, and
  `COUNT(*)` for `log_event` and `log_attr`.
- [ ] Capture a burst/drain run as well as the flat sustained run. Do not use a
  burst-only number as the SQLite throughput baseline.
- [x] Before changing the schema, checkpoint/close the database and collect
  `PRAGMA page_count`, `PRAGMA page_size`, `PRAGMA freelist_count`, and table
  sizes from `dbstat` when available. Record whether WAL/shm files are included.
- [x] Add `scripts/db_density_report.go` (or an equivalent checked-in script)
  that accepts a database path and reports the above values, table/index
  `dbstat` bytes, event count, attribute count, and bytes/event. It must be
  usable both before and after the rewrite; update only its driver import during
  the driver phase.
- [x] Inventory every runtime, test, benchmark, and script reference with:
  `rg -n 'modernc.org/sqlite|sql.Open\("sqlite"|log_attr|attributes_json' --glob '!vendor/**' .`
- [ ] Treat historical migration references to `log_attr` as intentional. The
  final runtime code, final schema assertions, writer SQL, purge SQL, and
  current docs must not depend on it.

### Exit criteria

- [x] Baseline is reproducible from a checked-in command and its numbers are
  recorded in `docs/loadtest-baseline.md` or a new clearly labeled pre-rewrite
  section.
- [x] The inventory lists at least: writer, migrator, write command, purge
  command, all SQLite tests/benchmarks, batcher integration test,
  `scripts/check_indexes.go`, Dockerfile, Makefile, README, architecture docs,
  and the vendor/module files.

---

## Phase 1 — Migration 005 and schema contract

### 1. Define the migration

- [ ] Add `migrations/005_inline_event_attributes.sql` with the canonical DDL
  for adding `attributes_json TEXT NOT NULL DEFAULT '{}'` to `log_event`.
- [ ] Add the matching `migration005SQL` to `migrator.go` and add version 005 to
  `allMigrations()`.
- [ ] Refactor migration application so each migration runs in a transaction
  and its `schema_migrations` row is inserted in that same transaction. Preserve
  current behavior for 001–004 while making failure/retry safe.
- [ ] Give migration 005 a Go hook because typed EAV values cannot be safely
  aggregated by a simple SQLite expression. The hook must run in one transaction
  and in this order:
  1. inspect `PRAGMA table_info(log_event)` and execute `migration005SQL` only
     when the new column is absent;
  2. read legacy rows ordered by `event_id, id`, group by event;
  3. encode and update `log_event.attributes_json`;
  4. drop all legacy attribute indexes and `log_attr` if it exists;
  5. drop and recreate `logs` with an explicit column list including
     `attributes_json`;
  6. insert the version-005 tracking row and commit the same transaction.
- [ ] Define `migration005SQL` and the checked-in 005 SQL file as the guarded
  column-add DDL contract. The Go hook owns the conditional execution,
  backfill, table removal, and view recreation; this avoids an `ALTER TABLE`
  duplicate-column failure on a retry while keeping the two SQL copies clear.
- [ ] Detect whether `log_attr` exists before querying it. A fresh final schema
  and a legacy schema with no attribute table must both migrate successfully.
### 2. Backfill implementation

- [ ] Read `event_id, id, key, value_type, string_value, int_value,
  double_value, bool_value, bytes_value` with nullable scan types.
- [ ] Convert each legacy row using the locked encoding rules. Bind one prepared
  update statement and flush each completed event; do not build a whole large
  database in memory.
- [ ] Preserve empty byte slices, null values, integer range, booleans, and
  special characters. Use standard base64 for bytes.
- [ ] Use deterministic duplicate-key behavior (last row by legacy `id`).
- [ ] Drop `idx_log_attr_event_id`, `idx_log_attr_key`, all legacy value indexes,
  and any other `idx_log_attr_*` indexes with `IF EXISTS` before dropping the
  table. Do not drop event timestamp/resource indexes used by purge.
- [ ] Recreate the `logs` view explicitly; preserve every existing column and
  append/name `attributes_json` without using `le.*`. Keep FTS as the existing
  separate `logs_fts` object.
- [ ] Keep `migrations/005_inline_event_attributes.sql` and the embedded SQL
  comments clear about the Go backfill hook; the file must not imply that a
  typed EAV backfill happens in SQL alone. The file is the auditable DDL
  contract, while `RunMigrations` is the only supported complete application
  path.

### 3. Migration tests

- [ ] Add a fresh-database test asserting `RunMigrations` is idempotent and the
  final `sqlite_master` has no `log_attr` table or `idx_log_attr_*` indexes.
- [ ] Add a legacy-database test that creates the 001–004 shape, inserts one
  event with string/int/double/bool/bytes/null attributes (including a special
  key and duplicate key), marks 001–004 applied, and runs `RunMigrations`.
- [ ] Assert the backfilled JSON values, duplicate-key rule, empty case `{}`,
  absence of `log_attr`, migration version 005, and `logs.attributes_json`.
- [ ] Assert a failed backfill rolls back the schema/data change and can be
  retried, if the migration hook has an injectable invalid-value path.
- [ ] Keep the existing FTS rebuild test and add an assertion that migration
  005 does not drop or repopulate `logs_fts` unexpectedly.

### Exit criteria

- [ ] Both fresh and pre-existing databases migrate successfully.
- [ ] Final schema has one event row per event, no attribute table/indexes, and a
  queryable `logs.attributes_json` column.
- [ ] `go test ./internal/storage/sqlite ./internal/batcher` passes before any
  driver change is made.

---

## Phase 2 — Inline write path and retention

### 1. Write SQL and prepared statements

- [ ] Add `attributes_json` to `sqlInsertEvent` and its values list.
- [ ] Remove `sqlInsertAttr`, `InsertAttr`, `insertAttributeRow`, and
  `attrValues` from the production write path.
- [ ] Reduce `PreparedStatements` and `preparedStatementsSQL` to resource/event
  statements only; update close/error cleanup and all tests that inspect them.
- [ ] Implement `marshalEventAttrs(attrs []model.Attribute) (string, error)` in
  the SQLite storage package. Marshal once per record before the event insert.
- [ ] Bind `{}` for nil/empty attributes. Bind the compact scalar values and
  bytes wrapper exactly as specified above.
- [ ] Decide and test command error ownership: if JSON encoding or event insert
  fails, return an error that causes the writer transaction to roll back and
  return every record to its pool exactly once. Do not log-and-continue while
  silently losing a record.

### 2. Safe row slimming

- [ ] Add a small helper for the stored severity text: bind `NULL` when
  `SeverityText == ""` or exactly equals `record.SeverityNumber.String()`;
  retain non-empty custom OTLP text.
- [ ] Add tests for standard severity, unspecified severity, custom text, and
  empty text. Confirm `severity_text` remains nullable in the existing schema.
- [ ] Continue binding `observed_timestamp_ns` as an integer, including zero;
  do not bind `NULL` because migration 001 declares it `NOT NULL`.
- [ ] Leave flags, dropped-attribute count, scope, IDs, body, and resource ID
  semantics unchanged. Do not remove columns based only on an unmeasured guess.

### 3. Retention and query-side checks

- [ ] Remove the `DELETE FROM log_attr` statement and its error path from
  `PurgeLogsCommand`. Delete expired `log_event` rows and rely on the declared
  event/resource relationship plus the existing orphan-resource cleanup.
- [ ] Keep the purge batching/iteration limit and timestamp index behavior.
- [ ] Rewrite purge SQL syntax tests so they insert attributes in
  `attributes_json`, never into `log_attr`.
- [ ] Confirm deleting events does not leave FTS behavior/regression issues;
  `logs_fts` is rebuilt by the existing maintenance command, not incrementally.

### 4. Write-path tests and benchmarks

- [ ] Update `writer_test.go` to query and unmarshal `log_event.attributes_json`
  for all supported scalar kinds and `{}` for empty attributes.
- [ ] Update `e2e_test.go` to assert event attribute keys/values in JSON. Keep
  the existing resource-attribute assertions in their current verbose format.
- [ ] Update `sql_syntax_test.go` DDL expectations: `log_attr` must be absent;
  surviving indexes are the timestamp/resource indexes plus the resource
  service-name index as appropriate. Remove direct EAV insert/delete cases.
- [ ] Update batcher integration assertions to read event JSON where relevant.
- [ ] Update all SQLite benchmarks to measure the new event statement and add
  a focused marshal benchmark. Remove benchmark assumptions that count one
  attribute insert per attribute.
- [ ] Add table tests for key escaping, Unicode, quotes, all scalar types,
  empty attributes, duplicate keys, and invalid non-finite doubles.

### Exit criteria

- [ ] The production path executes one event insert per record and no
  `log_attr` SQL exists outside intentional historical migration/backfill tests.
- [ ] Attribute round-trip tests pass, purge tests pass, and
  `go test ./internal/storage/sqlite ./internal/batcher ./internal/otlp` passes.
- [ ] A modernc-backed Phase 2 benchmark is captured so the schema change can
  be compared independently from the driver change.

---

## Phase 3 — Native `go-sqlite3` cutover

### Checklist

- [ ] Add `github.com/mattn/go-sqlite3` as a direct `go.mod` dependency.
- [ ] Replace the writer's modernc blank import with go-sqlite3 and change
  `sql.Open("sqlite", ...)` to `sql.Open("sqlite3", ...)`.
- [ ] Update every test/benchmark/tool call site found by the inventory,
  including `writer_bench_test.go`, `writer_perf_bench_test.go`,
  `writer_pool_bench_test.go` if applicable, `batcher_test.go`, `e2e_test.go`,
  and `scripts/check_indexes.go`.
- [ ] Remove modernc from direct/transitive module requirements with
  `go mod tidy`; run `go mod vendor` so `vendor/` and `vendor/modules.txt`
  contain go-sqlite3 and no modernc package.
- [ ] Re-run `rg` and ensure modernc is absent from active Go code, tests, tools,
  `go.mod`, `go.sum`, and `vendor/modules.txt`. Historical plan/load-result
  text may be retained only if it is explicitly labeled historical.
- [ ] Keep `SetMaxOpenConns(1)` and `SetMaxIdleConns(1)` and all existing
  foreign-key, WAL, busy-timeout, cache, mmap, temp-store, checkpoint, and
  journal-size pragmas.
- [ ] Add/retain an `openDatabase` test checking `journal_mode`, foreign keys,
  and the key performance pragmas under the native driver.
- [ ] Verify `LastInsertId`, BLOB scan/bind behavior, `RETURN`/error behavior,
  FTS5, `VACUUM`, and `PRAGMA wal_checkpoint` under go-sqlite3. Do not assume
  modernc and native driver error strings are identical in assertions.
- [ ] Run `CGO_ENABLED=1 go test ./...` and `CGO_ENABLED=1 go test -race ./...`.
  A deliberate `CGO_ENABLED=0` build is expected to fail; document that as a
  prerequisite rather than retaining a second driver/build tag.

### Docker and developer build

- [ ] In `Dockerfile`, install `build-base` in the builder, set/use
  `CGO_ENABLED=1`, and build the collector with the existing target.
- [ ] Keep the runtime Alpine image compatible with the generated binary;
  inspect `ldd /usr/local/bin/otel-collector` (or the equivalent image command)
  and add only the required runtime libraries. The default bundled driver does
  not require `sqlite-dev` in the runtime image.
- [ ] Update `Makefile` targets/comments so build, test, and loadtest use CGO
  intentionally. Do not add a fake static CGO claim.
- [ ] Update README build/development instructions with GCC/build-base
  prerequisites for local Linux/macOS builds and the cross-compilation caveat.
- [ ] Rebuild `docker-compose.loadtest.yml` and verify the collector starts,
  creates its database, passes health checks, and accepts OTLP traffic.

### Exit criteria

- [ ] No active modernc import, module, vendor entry, or `sqlite` driver-name
  call site remains.
- [ ] The native-driver test suite and Docker health check pass.
- [ ] The single-writer architecture and observed pragmas are unchanged.

---

## Phase 4 — Validation, density, and documentation

### Functional validation

- [ ] Run `go test ./...` and `go test -race ./...` with CGO enabled.
- [ ] Run the migration tests against: fresh DB, a legacy DB with attributes,
  an empty legacy DB, an already-migrated DB, and a DB reopened after migration.
- [ ] Run FTS rebuild/search, retention, vacuum, checkpoint, resource dedup,
  foreign-key, shutdown-drain, and batcher integration tests.
- [ ] Inspect `sqlite_master` after a real ingestion run. Assert final tables,
  indexes, view columns, and absence of `log_attr`.
- [ ] Use a SQLite trace or a controlled test/statement counter where practical
  to verify no per-attribute insert is issued. Source inspection alone is not
  the only proof for this success criterion.

### Performance and storage validation

- [ ] Generate a fixed synthetic workload (same number of events, same four
  scalar attributes, same resources, same batch/queue settings) before and
  after. Checkpoint both databases before measuring file/page totals.
- [ ] Run the sustained profile and record process rate, ingest rate, queue
  depth, errors, commit latency, RSS, binary size, database bytes/event, and
  table/index bytes.
- [ ] Run the burst profile and record drain time separately from intake rate.
- [ ] Compare Phase 2 (old driver/new schema) and Phase 3 (native driver/new
  schema) where the environment permits. Do not attribute a schema gain to
  CGO or a driver gain to removed EAV rows.
- [ ] Treat the existing ~4.7k records/sec sustained figure as a baseline, not
  a guaranteed target. Report the measured result and investigate regressions.
- [ ] Run `VACUUM` only as a separately labeled post-migration size check;
  distinguish logical freed pages from file size reclaimed by VACUUM.

### Documentation

- [ ] Update `docs/architecture.md`: final schema has compact event
  `attributes_json`, no `log_attr`, migration 005 compatibility/backfill,
  native CGO SQLite, and the unchanged single-writer/FTS model.
- [ ] Update `README.md`: remove `log_attr` from the current schema description,
  document the JSON encoding and the lost direct EAV SQL contract, and document
  CGO/native-driver build prerequisites.
- [ ] Update `docs/project_structure.md` migration list and storage notes.
- [ ] Update `docs/loadtest-baseline.md` with the exact pre/post commands and
  results; retain the old baseline as historical context rather than silently
  overwriting it.
- [ ] Add an operations note that migration 005 removes the EAV table and that
  a VACUUM may be needed to shrink an existing file after the migration.
- [ ] Document the compatibility impact: external readers that query
  `log_attr` must migrate to `log_event.attributes_json`; this is an on-disk
  migration, not a query API redesign.

### Final acceptance checklist

- [ ] `go test ./...` passes with CGO enabled.
- [ ] Fresh and legacy migrations pass and leave no `log_attr` table/index.
- [ ] All event attribute kinds round-trip through JSON, including bytes and
  null; resource attributes remain intact.
- [ ] `logs.attributes_json` is available by name and FTS rebuild/search still
  works.
- [ ] Purge removes expired events without any EAV delete statement and cleans
  orphaned resources.
- [ ] Docker loadtest image builds/runs with the native driver.
- [ ] Density and throughput measurements are recorded with their workload and
  environment, and no unexplained regression is accepted.
- [ ] `git diff --check`, `go vet ./...`, and the repository linter pass.

---

## Recommended commit order

Use small commits so failures are attributable:

1. **Baseline/instrumentation:** density report and recorded pre-rewrite run.
2. **Migration 005:** transactional backfill, final schema/view, migration tests.
3. **Inline writes:** event JSON encoder, prepared statements, purge, tests and
   modernc-backed comparison benchmark.
4. **Driver cutover:** go-sqlite3 imports/names, module/vendor cleanup, native
   driver tests.
5. **Build/docs/validation:** Docker/Makefile/README/architecture/loadtest
   updates and final measurements.

Start with the unchecked Phase 0 items. Do not begin the CGO cutover until the
fresh/legacy migration tests and the Phase 2 schema benchmark are green.
