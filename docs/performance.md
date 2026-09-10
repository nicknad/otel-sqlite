# Performance: E2E benchmark harness

The authoritative performance measurement for `otel-sqlite` is the
**end-to-end benchmark** in `tests/otel-sqlite-e2e`. It drives real OTLP/gRPC
traffic through the complete production pipeline and validates correctness
against the resulting SQLite database. Complementing it, per-component
**Criterion benchmarks** measure individual boundaries (mapping, batching,
queueing, SQLite transactions) for regression detection and design decisions;
see [section 10](#10-component-benchmarks-criterion).

```text
OTLP client (otel-sqlite-e2e)
    │ gRPC (tonic, real TCP)
    ▼
ingress handlers            validate → map_chunks (one chunk per ResourceLogs)
    │
    │ bounded ingest queue          (capacity 50_000; try_send)
    ▼
insert batcher thread       InsertBatcher per origin → WriteBatch
    │                       max_records=500, max_batch_age=10ms
    │ bounded command queue         (capacity 50_000; blocking send)
    ▼
single SQLite writer        one transaction per write batch
    │
    ▼
SQLite (WAL, synchronous=NORMAL)
```

The harness never replaces pipeline components with mocks. In embedded mode it
boots exactly the wiring of the production binary (`crates/otel-sqlite`) inside
the harness process; only the shutdown trigger differs (programmatic channel
instead of Ctrl+C/SIGTERM). See "Known limitations".

---

## 1. Running native benchmarks

Development (fast feedback, **not** for publishing numbers):

```bash
cargo run -p otel-sqlite-e2e -- --scenario baseline --duration-secs 10
```

Release (authoritative):

```bash
cargo build --release -p otel-sqlite -p otel-sqlite-e2e
cargo run --release -p otel-sqlite-e2e -- \
    --scenario sustained --duration-secs 600 --output results/sustained.json
```

Against an already-running server (e.g. the production binary started manually):

```bash
cargo build --release -p otel-sqlite
./target/release/otel-sqlite &                 # writes ./otel-logs.db
cargo run --release -p otel-sqlite-e2e -- \
    --scenario baseline \
    --endpoint http://127.0.0.1:4317 \
    --db-path ./otel-logs.db \
    --output results/baseline-external.json
```

Release builds are enforced with a warning otherwise; pass `--allow-dev` to
silence it for functional smoke runs.

## 2. Building the production image

```bash
docker build \
  --build-arg REVISION="$(git rev-parse HEAD)" \
  -f tests/docker/Dockerfile.benchmark \
  -t otel-sqlite-bench:latest .
```

The image contains both binaries (`otel-sqlite`, `otel-sqlite-e2e`).

## 3. Running Docker benchmarks

```bash
cd tests/docker
docker compose -f compose.benchmark.yml build
docker compose -f compose.benchmark.yml up -d otel-sqlite
docker compose -f compose.benchmark.yml run --rm benchmark
docker compose -f compose.benchmark.yml down
```

A bare `run` uses the service defaults: scenario `baseline`, the server
container as endpoint, the shared-volume database and a 300 s drain budget.
Override steps through the environment — arguments passed directly to
`compose run benchmark` **replace** the whole default command and would drop
`--endpoint`/`--db-path`, silently degrading the harness to embedded mode:

```bash
BENCH_SCENARIO=backpressure BENCH_WARMUP_SECS=3 \
    docker compose -f compose.benchmark.yml run --rm benchmark
```

Supported variables: `BENCH_SCENARIO`, `BENCH_ENDPOINT`, `BENCH_WARMUP_SECS`,
`BENCH_DRAIN_TIMEOUT_SECS`, `BENCH_OUTPUT`. Per-step durations stay under
scenario control on purpose; for fully custom workloads use the native
external mode against the published port (section 1).

* The database lives on the shared `bench-data` volume; the harness validates
  `/data/otel-logs.db` directly after draining.
* Results land on the `bench-results` volume.
* Container CPU/memory can be sampled out-of-band with
  `docker stats otel-sqlite-bench-server` during the run.
* Docker Desktop filesystem layers add variance. Treat containerized numbers
  as indicative; prefer native Linux for published claims.

## 4. Scenarios

| Scenario flag      | Purpose | Default steps |
|--------------------|---------|---------------|
| `baseline`         | Correctness + instrumentation verification | 1 client, 100 rec/request, 1k rec/s, 60s |
| `throughput`       | Saturation discovery via offered-load ramp | 1k/5k/10k/25k/50k/100k rec/s × 10s, stops at >5% rejected+ambiguous |
| `batch`            | Records-per-request comparison             | 1/10/100/500/1000 rec/request at 10k rec/s × 15s |
| `concurrency`      | Client-count comparison at constant load   | 1/2/4/8/16/32 clients at 10k rec/s × 15s |
| `sustained`        | Soak: WAL growth, degradation, leaks       | 4 clients, 20k rec/s, 600s |
| `backpressure`     | Overload policy verification               | closed-loop, 32 clients, single-resource requests |
| `insert-batching`  | Storage batcher behaviour vs OTLP shapes   | small / at-capacity / oversized / sustained / low-volume steps |
| `all`              | Everything except `sustained`              | – |

Every scenario performs a warm-up phase first (own run id, excluded from
validation), then measurement windows, then drain + validation.

## 5. Parameters (selected)

```
--seed <u64>                    deterministic workload seed (default 42)
--warmup-secs <u64>             warm-up before each scenario (default 5)
--duration-secs <u64>           override step duration
--clients <n>                   override client count
--records-per-request <n>       override records per OTLP request
--rate <rec/s>                  override offered rate where applicable
--request-timeout-ms <u64>      per-request gRPC timeout (default 30_000)
--body-size <bytes>             log body payload size (default 120)
--attributes <n>                extra attributes per record (default 4)
--resources <n>                 distinct OTLP resources (default 4)
--drain-timeout-secs <u64>      post-run drain budget (default 120)
--endpoint <url>                external server mode
--db-path <path>                validation database (required with --endpoint)
--output <path>                 JSON results file
--keep-going                    do not stop throughput ramp at saturation
--allow-dev                     permit dev-profile runs without warning
```

Workload generation is deterministic: record bodies, trace/span ids and
attributes derive from `(seed, sequence_number)`. Only `bench.run_id`
embeds wall-clock time so repeated runs against one database cannot collide.

## 6. Interpreting results

Each step reports:

* **Throughput** – generated and accepted records/second over the measured
  window (server-side acks). Offered vs accepted gap = backpressure.
* **Request latency** (client perspective) – min/p50/p90/p95/p99/p99.9/max.
  Since durable acks landed (`DurabilityMode::Commit`, wired identically in
  embedded mode via the shared commit ledger), an OK response means the
  records are **committed to SQLite**: measured round-trip includes network,
  decode, validate, map, enqueue AND the wait for the writer's commit
  (batcher age/capacity window + single-writer FIFO ahead of it). Expect
  p50 around the insert-batcher `max_batch_age` under light load.
  Persistence lag beyond that surfaces as drain time after producers stop.
* **Pipeline metrics** – from the production instrumentation captured by the
  embedded recorder:
  `otlp_requests_total`, `otlp_records_received_total`,
  `otlp_request_duration`, `otlp_request_errors_total`,
  `ingress_batch_size`, `ingress_queue_depth`, `ingress_queue_full_total`,
  `ingress_enqueue_duration`,
  `insert_batcher_buffered_records`, `insert_batcher_batches_emitted_total`,
  `storage_queue_depth`, `storage_commands_total{kind}`,
  `storage_records_written_total`, `storage_transaction_batch_size`,
  `storage_transaction_duration`, `storage_errors_total`,
  `storage_dropped_records_total`.
* **Storage deltas** – writer/batcher counters before→after each step.
* **Size trajectory** – sampled DB/WAL sizes during the window (growth,
  WAL pressure).

JSON output additionally records environment metadata (git revision, rustc,
SQLite version, CPU, execution mode `native-embedded`/`native-external`,
Docker detection, fresh/pre-populated database) plus the exact configuration.

## 7. Correctness validation

After every scenario step:

1. Producers stop.
2. Drain: an `IngestMessage::Flush` barrier is injected (embedded mode) or the
   row count is polled until stable (external mode).
3. The database is opened read-only.
4. Every persisted row carries `bench.run_id`, `bench.seq`, `bench.record_id`;
   validation asserts:

   ```text
   persisted set == generated − rejected − ambiguous
   no duplicate sequences
   record_id == seq on every row   (mapping corruption guard)
   ```

   * `rejected` – definitively refused by the server (`UNAVAILABLE` rejections
     or other error statuses). Admission is all-or-nothing: a request
     is either fully accepted or fully rejected, so every rejection is
     whole-request and its sequence positions are exact — including
     multi-resource requests, and `rejection_positions_exact` is always true.
   * `ambiguous` – client never learned the fate (timeout/transport error).
     These rows may legitimately be present or absent and never count as loss.
5. Intentional rejection under overload is reported separately and never
   misclassified as data loss.

A failing validation aborts the run with a non-zero exit code.

### 7.1 Durability A/B: `synchronous = NORMAL` vs `FULL` (bench2 tier)

The SQLite `synchronous` pragma is a durability knob, and the A/B below shows
it is performance-neutral for the pipeline at the recommended regression tier.
Both runs are the standard `baseline` scenario (60 s, 1 000 rec/s offered,
durable-ack mode) under `bench2` (2 pinned cores, tmpfs-backed temp DB),
identical seed and build; only the pragma differs via
`OTEL_SQLITE_E2E_SYNC`.

| Metric | NORMAL | FULL |
|---|---|---|
| Achieved throughput | 999.97 rec/s | 999.97 rec/s |
| Ack latency p50 / p99 (ms) | 14.82 / 18.35 | 14.76 / 17.14 |
| Transaction duration p50 / p99 (ms) | 0.446 / 1.877 | 0.423 / 1.885 |
| Queue-full events | 0 | 0 |
| Correctness | PASS (0 missing/dup) | PASS (0 missing/dup) |

FULL even edged out NORMAL on latency percentiles — well inside run-to-run
noise — because commits were already small and frequent (~25 records/txn)
while the offered load never approached writer saturation.

**Reproduce:**

```sh
docker compose -f tests/docker/compose.criterion.yml run --rm build \
    cargo build --release -p otel-sqlite-e2e
docker compose -f tests/docker/compose.criterion.yml run --rm \
    -e OTEL_SQLITE_E2E_SYNC=normal bench2 \
    cargo run --release -p otel-sqlite-e2e -- --scenario baseline \
    --output results/bench2-sync-normal.json
docker compose -f tests/docker/compose.criterion.yml run --rm \
    -e OTEL_SQLITE_E2E_SYNC=full bench2 \
    cargo run --release -p otel-sqlite-e2e -- --scenario baseline \
    --output results/bench2-sync-full.json
```

*Caveat:* `bench2` places temp databases on **tmpfs**, where fsync is nearly
free — this A/B therefore isolates the *pipeline* cost of the sync mode
(effectively zero), not device-level fsync latency. On slow physical disks
FULL raises per-commit cost proportionally to commit rate; measure with the
same recipe against non-tmpfs storage before drawing conclusions for such
deployments.

## 8. Known limitations

* **Embedded mode shares the process** with the load generator. Component code
  paths are identical to production, but CPU scheduling differs from a separate
  process. For final numbers use external mode against the standalone binary
  or Docker.
* **Ack durability depends on `durability.mode`**: with the default
  `"commit"` mode, latency percentiles include waiting for SQLite COMMIT and a
  2xx means the records are committed (application-crash safe; power-loss
  safe with `synchronous = "full"`). With `"enqueue"`, acks measure request
  handling only and acknowledged records may be lost to a crash. See §7.1
  for the measured cost of each knob.
* **Metrics endpoint**: production exposes Prometheus `/metrics` on
  `metrics_address` (default `127.0.0.1:8888`, `"off"` disables) and gRPC
  health (`grpc.health.v1`) on the OTLP port. The embedded harness uses its
  own recorder and does not mount the health service; external/Docker mode
  sees queue-full events through `UNAVAILABLE` rejections and storage
  counters through validation or the scrape endpoint of a running server.
* **System-level sampling** (CPU/RSS/disk latency) is not collected by the
  harness itself; use `pidstat`/`iostat` (native Linux) or
  `docker stats` alongside serious runs.
* **Queue-full events need slow storage**: with the production defaults
  (50k-deep ingest *chunk* queue + 50k-deep command queue), closed-loop
  clients cannot outrun the insert batcher/writer on fast NVMe storage —
  measured saturation was never reached natively (peak ingress depth stayed
  < 500 of 50,000 chunks at 118k rec/s offered). The `UNAVAILABLE`
  rejection path itself is covered by unit tests (`full_command_queue_applies_backpressure_instead_of_dropping`,
  `saturated_queue_rejects_the_whole_request_without_a_durable_wait`) and
  will engage naturally on slower disks or network-loaded deployments; when
  it does, the harness validates rejected sequence positions exactly (all
  rejections are whole-request).
* Windows-native runs bind loopback (avoids firewall prompts); production
  binds all interfaces. This does not affect throughput characteristics.
* `ingress_queue_full_total` counts *rejected requests* (a
  whole-request rejection), not individual chunks. It increments once per
  export that could not be admitted, which is the number clients actually
  observe as `UNAVAILABLE`.

## 9. Reproducing a published result

1. Check out the exact git revision recorded in the JSON (`git_revision`).
2. Build both crates in release mode with the same rustc version recorded.
3. Re-run with the recorded `configuration` block values (seed, durations,
   clients, rates, body size, attributes, resources).
4. Use the same execution mode and database state (`fresh` vs pre-populated);
   the default is always a fresh temporary directory.
5. Compare `latency_ms`, `results.records_*` counters and `pipeline_metrics`
   histograms; expect run-to-run variance on wall-clock-shared machines —
   repeat 3–5 times and compare medians before drawing conclusions.

## 10. Component benchmarks (Criterion)

While the e2e harness measures the system, Criterion benchmarks measure each
**boundary** in isolation. They live inside the crate that owns the feature:

| Crate                  | Bench          | Measures                                                        |
|------------------------|----------------|-----------------------------------------------------------------|
| `otel-sqlite-core`     | `batching`     | `InsertBatcher` push/merge/split; `WriteCommand` construction    |
| `otel-sqlite-ingress`  | `conversion`   | OTLP `ResourceLogs` → internal model (size + attribute scaling) |
| `otel-sqlite-ingress`  | `queue`        | bounded channel round trips; concurrent 1p/1c enqueue/dequeue   |
| `otel-sqlite-storage`  | `inserts`      | transaction-size sweep, ingest path, prune/checkpoint/vacuum    |
| `otel-sqlite-e2e`      | `pipeline`     | ingress handler → batcher → writer → commit (no gRPC transport) |

Run them with:

```bash
cargo bench -p otel-sqlite-storage            # one crate
cargo bench -p otel-sqlite-storage --bench inserts -- write_batch   # one group
```

Criterion saves a baseline automatically; re-running after a change prints
change distributions (`Performance has regressed/improved`) — use
`--save-baseline <name>` / `--baseline <name>` to pin comparisons.

Methodology notes:

* SQLite benches run each iteration against a **fresh temporary database**
  created in untimed setup, so table growth cannot distort size comparisons.
* An untimed warm-up command per pipeline absorbs lazy startup costs (schema
  migration runs concurrently with `Storage::open`; FTS init; first-use
  statement preparation).
* Completion is awaited by busy-spinning on stats atomics — no sleep polling,
  keeping sub-millisecond transactions measurable.
* Concurrent queue benches include OS scheduling noise; treat them as sanity
  checks and confirm sustained-load claims with the e2e harness or an external
  load generator.

### Running in a constrained container

`tests/docker/compose.criterion.yml` provides ready-made profiles. Compile once
without constraints, then measure inside the constrained container:

```bash
docker compose -f tests/docker/compose.criterion.yml run --rm build     # unconstrained compile
docker compose -f tests/docker/compose.criterion.yml run --rm bench2    # recommended tier (see below)
```

#### Recommended setup for regression gating: `bench2`

| Setting        | Value          | Rationale |
|----------------|----------------|-----------|
| CPU            | `cpuset: "0,1"` — 2 pinned cores | Minimum where the writer thread runs beside the driving thread instead of being timeshared against it. Pinning 4 cores was measured strictly worse on Docker Desktop/WSL2 (`write_batch/records_100` +27%, `ingest_path/chunk_100` +54% median, wider spread): the pipeline has only 2–3 runnable threads, so extra cores add cache-line ping-pong and scheduler migration surface, not parallelism. |
| Memory limit   | `mem_limit: 1024m` | Measured peak RSS across the heaviest families (`write_batch` sweep, `ingest_path`, retention prune with 10k-row databases) was **~71 MiB** — the 1 GiB cap is ~14x headroom. The suite would also fit under a 256 MiB limit if tighter budgets are required; keep 1024 MiB to absorb page-cache/tmpfs spikes and host variance. |
| Temp databases | tmpfs via `TMPDIR` (`size=256m`) | Stable WAL latencies, no Docker filesystem-layer noise. Actual DB churn is MB-scale per iteration. |
| Quota vs pin   | never `cpus:` | A CFS quota freezes tasks at scheduler-period boundaries and pollutes criterion with outliers; pinned cores degrade gracefully and honestly. |

Rules of engagement:

* **Pin cores (`cpuset`), never use a CPU quota (`cpus`).**
* **1-CPU validity by family:** single-threaded benches (batching,
  conversion, uncontended queue round trips) are sound even in the 1-core
  profile (`bench`). Multi-threaded families (storage pipeline, concurrent
  queue, e2e) timeshare threads across the single core and measure scheduling
  as much as the component — use them only for A/B comparisons against a
  baseline captured **in the same container**, never against native numbers.
* **Baseline hygiene:** this host class drifts ±15–40% between sessions even
  at fixed core count. Always compare against a baseline captured in the same
  session/environment (`--save-baseline` / `--baseline`), and treat sub-20%
  deltas as noise until reproduced.
* **Compile outside the budget:** use the unconstrained `build` service first;
  spending the constrained container's CPU on rustc corrupts measurements and
  wastes wall time.

### Baseline findings that shaped the defaults

Measured on the reference machine (Windows, bundled SQLite, WAL +
`synchronous=NORMAL`); relative shapes are what matter:

```text
write_batch (one command = one transaction):
records/txn    1        10       100      1_000    5_000
median      46.9 µs  98.6 µs   701 µs   5.55 ms  27.9 ms
rec/s        ~21k     ~101k     ~143k    ~180k    ~179k

cost model: ≈ 45 µs fixed + ≈ 5.7 µs/record
ingest_path vs write_batch @1_000 records: +90 µs (~1.6%) for the whole
pipeline (batcher + two channel hops)
maintenance @10k rows: analyze 9 ms · checkpoint-truncate 27 ms ·
prune 97 ms · vacuum 98 ms
```

Conclusions applied to the configuration:

1. **Throughput plateaus between ~100 and ~1 000 records/transaction** and does
   not improve beyond. `DEFAULT_MAX_INSERT_BATCH_RECORDS = 500` sits on that
   plateau with full throughput while halving buffered memory and worst-case
   flush backlog versus the previous 1 000.
2. **Pipeline plumbing is not worth optimizing**: at production batch sizes the
   entire ingress→writer path adds under 2%. Optimization effort belongs in
   per-row cost (INSERT + attribute JSON + index/FTS upkeep).
3. **Retention pruning scales like insertion** (~10 µs/row); VACUUM rewrites
   the database and stalls the single writer — keep it on its slow schedule.

## 11. Hardening backlog: PERF-006 / PERF-007

Open work is tracked in [`TODO.md`](../TODO.md) (P2). This section preserves
the design detail from the superseded `HANDOFF-PERFORMANCE.md`.

### Why these matter

All writes funnel through one SQLite writer thread (`writer.rs`). With the
default `durability.synchronous = "normal"` in WAL mode, a commit is an append
to the WAL file plus a lock operation — no fsync per transaction. Consequences:

- Every microsecond of CPU spent inside a transaction is direct throughput
  loss; there is no I/O wait behind which it hides.
- The batcher thread runs concurrently with the writer and is nearly idle
  computationally; CPU work moved off the writer onto the batcher overlaps with
  the writer's loop almost for free.
- The ingest boundary (tokio multi-thread runtime, one task per OTLP request)
  is the only genuinely parallel stage in the pipeline today.

### PERF-006 · Redundant dimension work + JSON allocation on the writer thread (S–M)

The metrics persistence path resolves the full dimension hierarchy
(`resource → scope → metric → series`) per record / per data point inside the
write transaction, each time performing a SHA-256 hash, a hex-format `String`
allocation, a `prepare_cached` lookup, and a conflict-ignoring upsert — even
when the row already exists. Real telemetry is extremely repetitive, so the
work scales O(records) where O(distinct identities) would suffice (often a
100x–1000x gap), all on the thread that must also commit transactions.

**Recommended solution:** a transaction-local dimension cache owned by one
insert call and cleared between batches (memory-bounded by the current batch's
cardinality; never process-lifetime). Cache keys must cover **every**
fingerprint input, and the fingerprint/JSON bytes must remain byte-identical to
preserve historical IDs. Replace `metric_type.to_string()` with a `match`
returning `&'static str`. Tolerant/quarantine and commit-ticket behavior must
not change.

> **Contract warning:** the attribute encoding is explicitly part of the
> storage contract, and dimension IDs are SHA-256 fingerprints *over the
> encoded bytes*. If the serializer is ever replaced it must be proven
> byte-identical (differential property test against `serde_json` over
> randomized inputs) or IDs must be versioned.

**Validation:** `cargo bench -p otel-sqlite-storage --bench inserts` before/after
on the same machine; `storage_transaction_duration` p50/p99 for metric-heavy
synthetic batches with high identity repetition; a criterion scenario with
deliberately repetitive identities (e.g. 10k points / 4 scopes); distinct
inputs never share a cache entry; cache state does not leak between batches.

### PERF-007 · All CPU work serialized behind the single writer (M–L, staged)

Everything expensive — hashing, JSON/hex encoding, parameter preparation —
happens *inside* the transaction on the one thread that also owns SQLite.

**Stage 1 = PERF-006** (memoization). Do this first and re-profile; it may be
sufficient.

**Stage 2 — move preparation onto the batcher thread ("prepared rows").** The
batcher computes final column values while assembling the batch, overlapping
hashing/encoding for batch N+1 with the writer's transaction for batch N. The
writer becomes bind/step/commit only. Salvage semantics migrate carefully:
`ToSqlConversionFailure` surfaces in the batcher instead of at execution
(offending row dropped-and-counted there, its ticket still rides the batch);
constraint violations remain execution-time in the writer's salvage pass.
Ordering, barriers, and ticket claiming are untouched. Cost: a core payload-type
change touching `command.rs`, both fuzz targets, the e2e harness, benches, and
the tolerant-mode plumbing.

**Stage 3 — parallelize preparation at mapping** (only if stage 2 profiles
insufficient). Per-request, multi-core, but couples the transport layer to
storage-internal encodings and weakens the crate boundary. Only pay that if
profiling after stage 2 shows the batcher thread saturated.

**Validation:** Criterion A/B on the `inserts` bench at each stage; expect
`storage_transaction_duration` to drop toward pure bind+step+commit with
batcher CPU rising; fuzz targets and e2e must pass unchanged semantically — the
assertions on quarantine counts and durability tickets guard the
salvage-semantics migration.

### PERF-008 · Fresh timer allocation per batcher loop iteration — DONE

`recv_timeout` replaced the `select! + after` timer allocation in `batcher.rs`;
the existing timer, partial-batch, shutdown, and backpressure tests are the
regression suite.

### Sequencing

1. **PERF-008** — done.
2. **PERF-006** — highest yield per risk; establishes the measurement baselines
   everything after depends on.
3. Re-profile. If the writer remains CPU-bound on realistic workloads, proceed
   to **PERF-007 stage 2**; revisit stage 3 only with profile evidence that a
   single batcher thread cannot keep up.
