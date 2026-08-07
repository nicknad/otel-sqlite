# Rewrite plan: unlock and raise real write throughput

## Why the previous plan looked like a no-op on write rate

The inline-attributes + native-driver work **did what it claimed for density and
driver microbenchmarks**, but the post-rewrite load number was measured with a
**rate-capped** workload that cannot show a write-path improvement.

### What actually landed (and is fine)

| Result | Evidence |
|---|---|
| One event insert per record; no `log_attr` on the write path | `write_batch_command.go` |
| Density ~1.41× (437 → 310 logical bytes/event) | `docs/loadtest-baseline.md` post-rewrite capture |
| Native driver ~3× faster than modernc on insert microbench | same doc, `BenchmarkWriterInsert` |
| Direct writer throughput on this machine | **~45–100k rec/s** empty DB, **~18–20k rec/s** on 500k-row DB (`BenchmarkWriter_*`) |
| Full queue+writer E2E (short run, empty DB) | **~110k rec/s** (`BenchmarkWriter_E2E_Pipeline`) |

### Why “write rate is the same” is the wrong conclusion

1. **The sustained loadtest is capped at ~4.72k rec/s.**
   Command used pre/post:

   ```bash
   make loadtest-run LOADTEST_CLIENTS=32 LOADTEST_RECORDS=150 \
                     LOADTEST_DURATION=60s LOADTEST_RPS=1
   ```

   That is `32 × 1 req/s × 150 records = 4,800 rec/s` client budget.
   Matching 4,720 → 4,720 only proves the pipeline still keeps up under the
   cap. It does **not** measure the process ceiling.

2. **Docs incorrectly promoted the cap to a ceiling.**
   `docs/loadtest-baseline.md` still says “SQLite persistence is the hard
   ceiling at ~4,700 records/sec”. That sentence is wrong given:
   - historical uncapped runs in `loadtest-results/` already showed
     **~6–9k written rec/s** on modernc;
   - current native-driver writer benches show **tens of thousands** rec/s.

3. **Loadtest compose tuning is silently ignored (real bug).**
   `docker-compose.loadtest.yml` sets:

   ```yaml
   BATCH_SIZE=500
   FLUSH_INTERVAL=1s
   ```

   but `cmd/collector/main.go` only applies legacy `BATCH_SIZE` /
   `FLUSH_INTERVAL` when the specific fields are **already zero**:

   ```go
   if cfg.BatcherBatchSize == 0 { cfg.BatcherBatchSize = n }
   if cfg.WriterBatchSize == 0  { cfg.WriterBatchSize  = n }
   ```

   `DefaultConfig()` always sets non-zero defaults (`BatcherBatchSize=250`,
   `WriterBatchSize=100`, flush intervals `1s`), so the compose values never
   stick. Effective loadtest config today:

   | Setting | Compose intent | Actual |
   |---|---:|---:|
   | Batcher batch size | 500 | **250** |
   | Writer commands/tx | 500 | **100** |
   | Flush intervals | 1s | 1s (default already 1s) |
   | Ingress / batch queue | 200k / 20k | applied (named env vars work) |

4. **EAV removal was never going to move a capped 4.7k number.**
   At 4.7k the machine is idle relative to the writer. Saving four attribute
   inserts per row only shows up once ingest ≥ previous process ceiling.

### Bottom line

Do **not** re-do the schema/driver rewrite. Treat it as done. The next plan is:

1. Fix measurement and config so process rate is real.
2. Establish an uncapped process baseline on the current code.
3. Attack the remaining write-path costs in priority order.
4. Only then claim a throughput win.

---

## Scope locked for this plan

In scope:

1. Fix legacy env handling and loadtest compose env names.
2. Replace the capped “sustained = ceiling” narrative with a correct
   measurement matrix (capped keep-up, uncapped process, burst/drain).
3. Raise single-writer SQLite process throughput on the existing schema
   (inline JSON, native driver, WAL, one writer).
4. Keep density gains; do not reintroduce EAV.

Out of scope (unless a later plan reopens them):

- Multi-DB sharding / multiple writer processes.
- Query API redesign, attribute expression indexes, FTS redesign.
- Making `observed_timestamp_ns` nullable.
- Resource JSON compaction / integer resource IDs (optional later phase only
  if profiling says resource insert is hot).
- Turso or non-SQLite backends.

---

## Current write-path cost model (post-rewrite)

Per persisted record today:

| Step | Cost class | Notes |
|---|---|---|
| gRPC + map + ingress queue | usually cheap vs SQLite | burst ingest already ≥278k rec/s |
| Batcher merge → `WriteBatchCommand` | cheap | size currently 250 (compose 500 ignored) |
| Writer collects up to `WriterBatchSize` **commands** | config | 100 commands × 250 rec = 25k rec before split |
| `MaxTransactionRecords=5000` splits txs | config | caps each SQLite tx |
| `BEGIN` | once/tx | fine |
| `INSERT OR IGNORE log_resource` | once/batch | PK + service_name index lookup |
| `marshalEventAttrs` | **once/record** | `map[string]any` + `json.Marshal` + `string(bytes)` alloc |
| `INSERT log_event` (15 binds) | **once/record** | PK AUTOINCREMENT + 2 secondary indexes + FK check |
| `COMMIT` | once/tx | WAL + synchronous=NORMAL |

Remaining indexes touched on every event insert:

- `log_event` INTEGER PRIMARY KEY AUTOINCREMENT (`sqlite_sequence`)
- `idx_log_event_timestamp`
- `idx_log_event_resource_id`
- FK parent lookup on `log_resource(id)` while `PRAGMA foreign_keys=ON`

Driver/schema wins already taken: no `log_attr`, no attr indexes, native CGO.

---

## Phase 0 — Fix measurement and config (do this first)

### 0.1 Legacy env bug

- [x] Change legacy handling so `BATCH_SIZE` / `FLUSH_INTERVAL` apply when the
  **named** env vars were not set, not when the field is zero after
  `DefaultConfig()`. Implemented as `config.ApplyLegacyEnv` (called from
  `LoadFromEnv`); collector main no longer duplicates the logic.
  - Prefer `BATCHER_BATCH_SIZE` / `WRITER_BATCH_SIZE` when present.
  - Else if `BATCH_SIZE` present, fill whichever sides lack a specific var.
  - Same pattern for flush intervals.
  - Unit tests cover BATCH_SIZE alone, precedence, and loadtest-style env.
- [x] Update `docker-compose.loadtest.yml` to use the explicit names:

  ```yaml
  BATCHER_BATCH_SIZE=500
  WRITER_BATCH_SIZE=50
  BATCHER_FLUSH_INTERVAL=1s
  WRITER_FLUSH_INTERVAL=1s
  WRITER_MAX_TRANSACTION_RECORDS=10000
  ```

- [x] Log effective batcher/writer sizes and flush intervals once at startup.
- [x] Update README env table: mark `BATCH_SIZE` / `FLUSH_INTERVAL` deprecated
  but working; document the precedence rules.

### 0.2 Measurement matrix (replace the false ceiling)

Stop using the capped run as the process baseline. Record three profiles:

| Profile | Purpose | Suggested command |
|---|---|---|
| **A. Keep-up (capped)** | Prove no backlog at a chosen rate | current 4.72k command (regression only) |
| **B. Process ceiling (uncapped, short)** | Max sustained written rec/s while queues non-empty | `CLIENTS=8 RECORDS=250 DURATION=30s RPS=0` (matches historical `loadtest-results`) |
| **C. Burst + drain** | Intake ceiling + time-to-drain after client stops | `CLIENTS=32 RECORDS=1000 DURATION=30s RPS=0`, then watch `logs_written_total` until flat |

For B and C always report from Prometheus deltas, not client send rate:

- `otel_collector_storage_logs_written_total` → **process rate**
- `otel_collector_storage_logs_received_total` → ingest rate
- batch/ingress queue depths during the run
- export error %
- final `COUNT(*)` and density report after stop + checkpoint

- [x] Add Make targets: `loadtest-keepup`, `loadtest-process`,
  `loadtest-burst-drain`.
- [x] Extend `cmd/loadtest` with `-drain-timeout`; print process rate over the
  client window and drain seconds until written ≥ received.
- [x] Re-run A/B on current HEAD after the env fix; append results to
  `docs/loadtest-baseline.md` as **“post-fix uncapped baseline”**
  (process ceiling **~58.3k written rec/s**).
- [x] Strike or rewrite the “hard ceiling at ~4,700” language in
  `docs/loadtest-baseline.md` and README. Keep 4.7k only as the historical
  capped keep-up number.

### 0.3 Local absolute baseline (no Docker required)

- [x] Document the writer bench commands and host numbers in
  `docs/loadtest-baseline.md`.

  ```bash
  CGO_ENABLED=1 go test -tags fts5 ./internal/storage/sqlite -run '^$' \
    -bench 'BenchmarkWriter_' -benchtime=3s -count=3
  ```

  Observed on AMD Ryzen 5 4500U (single sample):

  | Bench | Approx rec/s |
  |---|---:|
  | `BenchmarkWriter_OptimizedPragmas` (fresh DB, 250-rec tx, 5 attrs) | ~45k |
  | `BenchmarkWriter_LargeDB_Optimized` (after 500k prefill) | ~19–20k |
  | `BenchmarkWriter_E2E_Pipeline` (queue + writer) | ~100k+ short-run |

- [x] Fix `BenchmarkWriter_E2E_Pipeline` drain wait: wait until
  `COUNT(*)` catches `totalRecords` and the command queue is empty.

### Phase 0 exit criteria

- [x] Legacy `BATCH_SIZE` actually changes runtime batch sizes (test-covered).
- [x] Loadtest compose uses explicit env names; startup logs print them.
- [x] Docs no longer call 4.7k the process ceiling.
- [x] Profile B number recorded on current code — **~58.3k written rec/s**
  is the baseline later phases must beat.

---

## Phase 1 — Confirm where time goes (before changing SQL)

Do not guess. Spend one short profiling pass on profile B.

- [x] Run `go test -cpuprofile/-memprofile` on `BenchmarkWriter_OptimizedPragmas`
  (writer-bound; matches the SQLite hot path without gRPC noise).
- [x] Attribute time at least into:
  1. SQLite / go-sqlite3 (`sqlite3_step`, page ops) — **~45% cum Exec, ~24% step**
  2. `marshalEventAttrs` / `encoding/json` — **~19% CPU, ~50% alloc_space**
  3. Go `database/sql` bind + Exec args — **bind ~13% CPU; args ~32% alloc**
  4. batcher / channel / GC — not dominant in the writer bench
- [x] Statement counter via go-sqlite3 `RegisterUpdateHook`
  (`TestWriteBatchInsertMix`): 1 resource + N events, 0 `log_attr`.
- [x] Hot spots subsection in `docs/loadtest-baseline.md`.
- [x] `BenchmarkMarshalEventAttrs` baseline: ~5758 ns/op, 1344 B/op, 23 allocs/op
  (5 mixed attrs).

Confirmed order of dominance:

1. SQLite row insert + bind + secondary indexes + CGO (`sqlite3_step`)
2. JSON marshal allocs per record (`map` + `encoding/json`)
3. `database/sql` per-Exec argument packaging
4. Everything else

### Phase 1 exit criteria

- [x] Profile captured; Phase 2 order matches the hypothesis (no reorder).

---

## Phase 2 — High-confidence throughput wins (no schema rewrite)

Implement in this order. Each step gets its own before/after profile B number.

### 2.1 Config defaults that match the write path we already have

- [x] Production defaults: `BatcherBatchSize=500`, `WriterBatchSize=50`
  (commands), `WriterMaxTransactionRecords=10000`. Loadtest compose already
  matched these after Phase 0.
- [x] Documented that `WriterBatchSize` is commands per tx collection (README +
  `WriterConfig` comment).

### 2.2 Cut per-record JSON overhead

- [x] `BenchmarkMarshalEventAttrs` + hand-rolled JSON object writer with
  `sync.Pool` buffer, last-write-wins via index map, no `encoding/json` on
  the hot path. Key order is insertion order of last-wins keys (tests
  unmarshal; no sort requirement).
- [x] Bind `[]byte` for `attributes_json` (no `string(encoded)` copy).
- [x] Encoding rules preserved (bytes wrapper, reject non-finite doubles,
  `{}` for empty, last-write-wins). Escape/Unicode covered in unit tests.
- [x] Result: ~5758 ns/op 23 allocs → **~1180 ns/op 3 allocs** (~5× faster,
  ~8× fewer allocs).

### 2.3 Reduce SQLite work per row

- [x] **FK checks:** writer default `PRAGMA foreign_keys=OFF`; opt in via
  `WriterConfig.EnforceForeignKeys`. E2E FK test opts in. Invariant: resource
  row before events in the same command.
- [ ] **AUTOINCREMENT tax:** deferred to Phase 3 (needs migration 006).
- [x] **Indexes:** unchanged (timestamp + resource_id kept).
- [x] **Resource ID cache:** process-local `seenResources` map on Writer;
  skips `INSERT OR IGNORE` after first successful insert per ID.

### 2.4 Reduce `database/sql` per-row overhead

- [x] Prepared statements retained; bind `[]byte` for JSON.
- [ ] Multi-row `INSERT ... VALUES` chunking: **not landed** in this pass
  (optional; revisit if Phase 2 plateaus further). Single-row prepared
  insert remains.
- [x] Bind types: `int64` severity, `[]byte` attrs JSON.

### 2.5 WAL / checkpoint interaction under sustained write

- [x] Observed startup `wal_checkpoint(PASSIVE): database table is locked`
  race (pre-existing, counted once in `write_errors_total`); not a write-path
  regression under load.
- [ ] Raising `wal_autocheckpoint` for loadtest only: deferred (no clear stall
  signal beyond the known maintenance race).

### Phase 2 measured results

| Metric | Phase 0 | Phase 2 | Delta |
|---|---:|---:|---:|
| Profile B process rec/s | 58,299 | **70,258** | **+20.5%** |
| Writer bench optimized | ~45k | **~64k** | +42% |
| Marshal ns/op (5 attrs) | 5758 | **1180** | −79% |
| Marshal allocs/op | 23 | **3** | −87% |

### Phase 2 exit criteria

- [x] Profile B process rate improved vs Phase 0 baseline (+20.5%).
- [x] `CGO_ENABLED=1 go test -tags fts5 ./...` green.
- [x] Density path unchanged (still no `log_attr`).

---

## Phase 3 — Optional schema/format wins (only if Phase 2 plateaus)

Pursue only the items profiling still blames.

### 3.1 Migration 006 candidates (pick by evidence)

- [ ] Remove `AUTOINCREMENT` if Phase 2.3 showed gain and FTS/purge OK.
- [ ] Nullable `observed_timestamp_ns` with bind `NULL` when equal to
  `timestamp_ns` or zero — small space win, minor bind win; only with a
  real table rebuild.
- [ ] WITHOUT ROWID is **not** a fit (`id` is the rowid integer PK). Skip.

### 3.2 Payload slimming (CPU + pages)

- [ ] Resource attributes: still verbose `AttributeValue` JSON. Compact them
  with the same rules as event attrs if resource insert/update CPU or
  page churn shows up (usually secondary).
- [ ] Consider storing `attributes_json` as raw TEXT from a known-safe encoder
  without intermediate `map[string]any`.

### 3.3 Out of scope unless product requires it

- Sharded DB per tenant/resource.
- Async durability (`synchronous=OFF`) — only as an explicit dangerous knob
  for bulk import tools, never default.
- Dropping timestamp index without a purge redesign.

### Phase 3 exit criteria

- [ ] Any migration has fresh+legacy tests, docs, and density+throughput pair.

---

## Phase 4 — Validation and documentation

### 4.1 Functional

- [ ] Full test suite with CGO + `fts5`.
- [ ] Migration tests if 006 added.
- [ ] E2E, purge, FTS rebuild, resource dedup, shutdown drain.

### 4.2 Performance acceptance

Record in `docs/loadtest-baseline.md`:

| Metric | Phase 0 uncapped baseline | Final |
|---|---:|---:|
| Process rec/s (profile B) | TBD | TBD |
| Keep-up at 4.72k (profile A) | pass | pass |
| Drain time after burst (profile C) | TBD | TBD |
| Logical bytes/event | ~310 | ≥ prior (no major regression) |
| Writer bench fresh / large DB | ~45k / ~20k | higher or explained |

No fixed multiple required (no “must hit 2×”). Accept based on measured
deltas and absence of correctness regressions.

### 4.3 Docs to fix

- [ ] `docs/loadtest-baseline.md` — remove false 4.7k ceiling; add matrix.
- [ ] `README.md` — env precedence; performance section.
- [ ] `docs/architecture.md` — writer batching semantics; FK/pragma choices.
- [ ] `plan.md` (this file) — check boxes as work completes.
- [ ] Leave previous rewrite history intact as “Phase −1 completed work”.

### Final acceptance checklist

- [ ] Uncapped process baseline published for pre-Phase-2 HEAD.
- [ ] Config bug fixed and tested.
- [ ] At least one Phase 2 change lands with a measured process-rate gain,
  **or** profiling proves SQLite page write rate is saturated and further
  single-process gains need Phase 3/sharding.
- [ ] Capped keep-up still green.
- [ ] Density still without `log_attr`.
- [ ] CI/local: `CGO_ENABLED=1 go test -tags fts5 ./...`.

---

## Recommended commit order

1. **fix(config):** honor `BATCH_SIZE` / `FLUSH_INTERVAL` correctly; explicit
   loadtest env names; startup log of effective sizes.
2. **docs(loadtest):** measurement matrix; retract 4.7k ceiling; record
   uncapped baseline on current code.
3. **test(bench):** fix E2E bench drain; add marshal bench.
4. **perf(json):** pooled / specialized `marshalEventAttrs`.
5. **perf(writer):** resource ID cache; optional FK off on writer; tx sizing.
6. **perf(sql):** multi-row insert experiment (keep or revert from benches).
7. **perf(pragma):** wal_autocheckpoint / loadtest-only overrides.
8. **feat(migrate006):** only if AUTOINCREMENT/nullable observed_ts justified.
9. **docs:** final numbers and ops notes.

---

## Completed work this plan builds on (do not redo)

Previous plan (inline attributes + native driver) is **done**:

- Migration 005, compact `attributes_json`, `log_attr` removed from final schema
- Writer one-insert-per-event path
- `go-sqlite3` + CGO Docker/Makefile
- Density 1.41× on the capped workload
- Driver microbench ~3× vs modernc

That work improved **cost per record** and **storage**. It was never given a
chance to show **records/sec** because the only published comparison was
rate-capped at 4.72k and the loadtest batch-size env was not applied.

---

## Immediate next actions (start here)

1. Fix the legacy env bug + loadtest compose env names.
2. Run profile B uncapped; write the number down as the real baseline.
3. Profile once; then execute Phase 2.1 → 2.2 → 2.3 in order.

Do not open another schema-rewrite discussion until step 2 is on paper.
)
