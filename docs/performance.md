# Performance: E2E benchmark harness

Authoritative measurement is the **end-to-end benchmark** in
`tests/otel-sqlite-e2e`: real OTLP/gRPC through the full production
pipeline, validated against SQLite. Per-component **Criterion benches**
cover individual boundaries for regression/design (see §10).

```text
OTLP client (otel-sqlite-e2e) ─gRPC──▶ ingress ──▶ bounded ingest queue (50k)
  ──▶ insert batcher (500 rec / 10 ms) ──▶ bounded command queue (50k)
  ──▶ single SQLite writer (1 txn per coalesced group) ──▶ SQLite (WAL, synchronous=NORMAL)
```

Embedded mode boots the exact production wiring in-process (only shutdown
differs); external mode drives a standalone binary over TCP. Use external
or Docker for publishable numbers.

---

## 1. Running benchmarks

```bash
# Fast feedback (not publishable):
cargo run -p otel-sqlite-e2e -- --scenario baseline --duration-secs 10

# Authoritative:
cargo run --release -p otel-sqlite-e2e -- \
  --scenario sustained --duration-secs 600 --output results/sustained.json

# Against a running server:
./target/release/otel-sqlite &
cargo run --release -p otel-sqlite-e2e -- --scenario baseline \
  --endpoint http://127.0.0.1:4317 --db-path ./otel-logs.db \
  --output results/baseline-external.json
```

Release builds enforced (pass `--allow-dev` for smoke runs). Docker:
`tests/docker/compose.benchmark.yml` (`BENCH_SCENARIO`, `BENCH_ENDPOINT`,
`BENCH_WARMUP_SECS`, `BENCH_DRAIN_TIMEOUT_SECS`, `BENCH_OUTPUT`);
don't pass raw args to `compose run benchmark` — they replace
`--endpoint`/`--db-path` and silently fall back to embedded mode.
Treat container numbers as indicative (filesystem-layer variance); prefer
native Linux for claims.

## 2. Scenarios

| Flag | Purpose | Defaults |
|---|---|---|
| `baseline` | Correctness + instrumentation | 1 client, 100 rec/req, 1k rec/s, 60s |
| `throughput` | Saturation ramp | 1k–100k rec/s × 10s, stop at >5% rejected+ambiguous |
| `batch` | Records-per-request sweep | 1/10/100/500/1000 rec/req at 10k rec/s × 15s |
| `concurrency` | Client-count sweep | 1–32 clients at 10k rec/s × 15s |
| `sustained` | Soak (WAL growth, leaks) | 4 clients, 20k rec/s, 600s |
| `backpressure` | Overload policy | closed-loop, 32 clients, single-resource reqs |
| `insert-batching` | Batcher vs OTLP shapes | small / at-capacity / oversized / sustained / low-volume |
| `all` | Everything except `sustained` | – |

Warm-up first (excluded), then measurement, then drain + validation.
Key overrides: `--clients`, `--records-per-request`, `--rate`,
`--body-size` (120 B), `--attributes` (4), `--resources` (4),
`--drain-timeout-secs` (120), `--seed` (42). Workload deterministic from
`(seed, seq)`; only `run_id` embeds wall-clock.

## 3. Interpreting results

- **Throughput**: generated vs accepted rec/s (acks). Gap = backpressure.
- **Latency** (client): min/p50/p90/p95/p99/p99.9/max. In default
  `commit` mode an OK means **committed to SQLite** — includes network,
  decode, map, enqueue *plus* commit wait (batch window + writer FIFO).
  Expect p50 ≈ batcher `max_batch_age` under light load; persistence lag
  shows as drain time.
- **Pipeline metrics**: `otlp_requests_total`, `otlp_records_received_total`,
  `otlp_request_duration/errors`, `ingress_batch_size/queue_depth/queue_full`,
  `insert_batcher_buffered_records/batches_emitted/dropped`,
  `storage_queue_depth/commands/records_written/transaction_batch_size/transaction_duration/errors/quarantined`.
- **Storage deltas + size trajectory**: counter before→after per step;
  sampled DB/WAL growth. JSON output adds env metadata (git, rustc, SQLite,
  CPU, `native-embedded`/`native-external`, fresh vs pre-populated).

Validation per step: producers stop → drain (Flush barrier embedded, row
poll external) → open DB read-only → assert
`persisted == generated − rejected − ambiguous`, no duplicate seq,
`record_id == seq`. Rejected = whole-request `UNAVAILABLE` (queue
admission) or the `partial_success.rejected_log_records` count (storage
disconnect/quarantine); ambiguous = timeout/transport (may be present or
absent, never loss). Failures abort non-zero.

### 3.1 Durability A/B: `synchronous = NORMAL` vs `FULL`

Standard `baseline` (60 s, 1k rec/s, durable-ack) on `bench2` (2 pinned
cores, tmpfs), only pragma differs via `OTEL_SQLITE_E2E_SYNC`:

| Metric | NORMAL | FULL |
|---|---|---|
| Throughput | 999.97 rec/s | 999.97 rec/s |
| Ack p50 / p99 (ms) | 14.82 / 18.35 | 14.76 / 17.14 |
| Txn p50 / p99 (ms) | 0.446 / 1.877 | 0.423 / 1.885 |
| Queue-full / correctness | 0 / PASS | 0 / PASS |

Performance-neutral at this tier — commits were small (~25 rec/txn), load
far from saturation. Caveat: tmpfs makes fsync nearly free; this isolates
*pipeline* cost, not device fsync. Re-measure on physical disks before
conclusions for slow storage.

## 4. Known limitations

- Embedded shares the process with loadgen: same code paths, different
  scheduling. Publish from external/Docker.
- `commit` acks include COMMIT wait (crash-safe; power-safe with
  `synchronous=full`); `enqueue` acks at handling and may lose data.
- Metrics endpoint off by default; embedded harness uses its own recorder.
  System CPU/RSS/disk not collected — use `pidstat`/`iostat`/`docker stats`.
- Queue-full needs slow storage: defaults (50k + 50k) never saturated on
  fast NVMe (ingress depth <500 at 118k rec/s). Rejection path covered by
  unit tests; engages on slower disks. `ingress_queue_full_total` counts
  rejected *requests*, not chunks.

Reproduce: check out `git_revision` from JSON, same rustc, same
`configuration` block, same mode/DB state; repeat 3–5×, compare medians.

## 5. Component benchmarks (Criterion)

| Crate | Bench | Measures |
|---|---|---|
| `core` | `batching` | `InsertBatcher` push/merge/split; `WriteCommand` |
| `ingress` | `conversion` | `ResourceLogs` → model (size/attr scaling) |
| `ingress` | `queue` | bounded channel round trips; concurrent enqueue |
| `storage` | `inserts` | txn-size sweep, ingest path, purge/checkpoint/vacuum |
| `e2e` | `pipeline` | handler → batcher → writer → commit (no gRPC) |

```bash
cargo bench -p otel-sqlite-storage
cargo bench -p otel-sqlite-storage --bench inserts -- write_batch
```

Fresh temp DB per iteration (timed setup excluded); untimed warm-up absorbs
migration/FTS/prepare costs; completion via stats-atomics spin (no sleep).
Queue benches carry scheduler noise — confirm load claims with e2e.

Constrained runs: `tests/docker/compose.criterion.yml` — compile with
`build` (unconstrained), measure with `bench2` (2 pinned cores, 1 GiB,
tmpfs `TMPDIR`). Pin cores (`cpuset`), never quota (`cpus`); 1-core valid
only for single-threaded families; compare against same-session baselines
(`--save-baseline`/`--baseline`), treat <20% as noise (host drifts
±15–40%).

Baseline findings (WAL + `NORMAL`; shapes matter, not absolutes):

```text
records/txn  1       10      100     1_000   5_000
median       46.9µs  98.6µs  701µs   5.55ms  27.9ms  (~45µs fixed + ~5.7µs/rec)
```

1. Throughput plateaus ~100–1 000 rec/txn: `MAX_INSERT_BATCH_RECORDS = 500`
   keeps full throughput with half the memory/backlog vs 1 000.
2. Plumbing adds <2% at production sizes (`ingest_path` +90 µs vs 1k batch)
   — optimize per-row cost (INSERT + JSON + index/FTS), not channels.
3. Purge ≈ insert cost (~10 µs/row); VACUUM stalls the single writer —
   keep it on its slow schedule. Maintenance @10k rows: analyze 9 ms,
   checkpoint 27 ms, purge/vacuum ~98 ms.

## 6. Known performance bottlenecks (open)

Single writer is CPU-bound under `normal`/WAL (commit = WAL append + lock,
no fsync). Every µs in-transaction is throughput loss; batcher is nearly
idle and overlaps for free; ingest is the only parallel stage.

### CPU serialized behind single writer (staged)

Hashing, JSON/hex, param prep all run inside the writer transaction.

- Stage 1 — prepared rows on batcher: batcher computes final columns for
  N+1 while writer commits N; writer becomes bind/step/commit. Salvage
  migrates: `ToSqlConversionFailure` drops-and-counts in batcher (ticket
  rides batch); constraints stay in writer salvage. Ordering/barriers/
  tickets untouched.
- Stage 2 — parallel prep at mapping: only with profile proof batcher is
  saturated (couples transport to storage encodings).

Validate: Criterion A/B per stage (`storage_transaction_duration` →
bind+step+commit, batcher CPU up); fuzz + e2e unchanged (quarantine/ticket
assertions guard salvage).

Already done (regression suites only):

- **Resource resolve is per batch, not per record.**
  `insert_logs_inner` calls `resolve_resource` once for the batch origin
  before the row loop (`storage/src/command/logs.rs`), and origin isolation
  keeps exactly one resource per write batch
  (`storage/src/origin_buffers.rs`). The SHA-256 fingerprint, hex render,
  `prepare_cached` and upsert therefore scale with batches, not records; an
  extra transaction-local cache would add bookkeeping without reducing the
  number of upserts in the common case.
- Batcher timer alloc → `recv_timeout` in `batcher.rs`.
- Scope JSON rendered once per `ScopeLogs` group as shared `Arc<LogScope>`
  (`core/src/model/attribute.rs`).
- The writer coalesces consecutive insert commands up to
  `group_commit_max_records` (default 1000, `0` disables). It only helps when
  a command backlog exists; when the writer keeps up, a group has one command.
  Measure it with high resource cardinality (`--resources`), not the default
  four resources. `OTEL_SQLITE_E2E_GROUP_COMMIT` overrides the harness budget
  (`0` disables grouping) and `OTEL_SQLITE_E2E_SEARCH_MODE` overrides
  `off|body|full`; the JSON report's `configuration` block records the
  effective values.
- `[search] mode = "body"` (default) indexes only `log_event.body` and removes
  the per-row `log_resource` join from the insert trigger. `full` restores the
  six-column cost; `off` removes trigger maintenance. Every insert benchmark
  therefore depends on the configured search mode.

Grouping microbench on this host (`cargo bench -p otel-sqlite-storage --
bench inserts -- group_commit`): 100 one-record commands took 5.83 ms with
grouping disabled and 2.35 ms with the 1000-record cap; 1000 commands took
48.4 ms and 23.7 ms (~2.0-2.5x). That isolates the fixed commit cost, not
end-to-end throughput.

High-cardinality e2e (release, embedded, `--resources 500` so each
100-record request maps to one-record origins; `--scenario baseline`, 60 s
at 1k rec/s):

| group budget | transactions | batches/txn | ack p50 | ack p99 |
|---|---:|---:|---:|---:|
| `0` (off) | 60,000 | 1.0 | 31.3 ms | 49.7 ms |
| `1000` | 1,127 | 53.2 | 22.3 ms | 39.8 ms |

Throughput ramp with the same shape (`--scenario throughput --duration-secs
5 --warmup-secs 2`): ungrouped saturated at ~9.2-10.6k accepted records/s
with ack p99 438-543 ms; the 1000-record budget reached ~23.7-24.6k records/s
with ack p99 192-293 ms. Grouping therefore lifts the writer's saturation
point ~2.5x and roughly halves tail latency in this workload. Embedded mode
shares the process with the load generator, so treat these as relative
numbers rather than a production ceiling.

Next: re-profile before the prepared-rows stage; do it only on evidence.
