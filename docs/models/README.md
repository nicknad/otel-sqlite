# Data models: diagrams, owners, fidelity

Four models carry OTLP log data through this service. They are different
contracts with different owners and different rates of change, so they are
documented separately instead of collapsed into one picture.

| model | defined in | owner crate | diagram |
|---|---|---|---|
| **Wire** — OTLP protobuf messages | `crates/otel-sqlite-ingress/proto/**` | `otel-sqlite-ingress` | [wire.mmd](wire.mmd) |
| **Domain** — in-memory types | `crates/otel-sqlite-core/src/model/*.rs` | `otel-sqlite-core` | [domain.mmd](domain.mmd) |
| **Persisted** — tables, view, FTS index | `crates/otel-sqlite-storage/migrations/*.sql` (replayed) | `otel-sqlite-storage` | [schema.mmd](schema.mmd) |
| **Read contract** — what a reader queries | `logs` view, `logs_fts` | `otel-sqlite-storage` | in [schema.mmd](schema.mmd) |

The conversion path between them is [pipeline.mmd](pipeline.mmd); what happens
to each individual field is [fidelity-matrix.md](fidelity-matrix.md), generated
from [traceability.tsv](traceability.tsv).

## Files

| file | how it is produced | drift check |
|---|---|---|
| `schema.mmd` | generated from a migrated database (`sqlite_master` + pragmas) | `tests/schema_diagram.rs` |
| `domain.mmd` | generated from the Rust type declarations | `tests/domain_diagram.rs` |
| `traceability.tsv` | hand-written; the source of truth for field mapping | `tests/model_traceability.rs` |
| `fidelity-matrix.md` | generated from `traceability.tsv` | `tests/model_traceability.rs` |
| `diagrams.md` | generated: the four `.mmd` files embedded as `mermaid` fences, because GitHub does not render `.mmd` | `tests/model_traceability.rs` |
| `wire.mmd`, `pipeline.mmd` | hand-written | none (structural; both are embedded in `diagrams.md`, so a missing file still fails) |
| `README.md` | hand-written | none |

## Regenerating

The generated files are compared byte-for-byte in the test suite, so a schema
migration, a model change or a proto bump fails with the command to run:

```text
UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test schema_diagram
UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-core    --test domain_diagram
UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test model_traceability
```

`cargo test --workspace` then verifies the result. Nothing regenerates
automatically during a normal build; the tests only compare.

## What the drift checks actually prove

| check | proves | cannot see |
|---|---|---|
| `schema_diagram` | the ER diagram equals the schema a **migrated** database has, including 003's recreated view and 005's dropped tables | whether a column is still *written* |
| `domain_diagram` | every `pub struct`/`pub enum` field and variant in `core/src/model` appears in `domain.mmd` | semantics of a field |
| `model_traceability` | every field of every consumed OTLP message is classified; every stored column is produced by some row; every `loss_counter` matches a counter the ingress really emits; every view column projects a stored column | whether a note is *true* |

That last row is the honest limit: the machine checks coverage, not meaning.
The notes in `traceability.tsv` are reviewed by a human.

### Proto pinning

The vendored `.proto` files are the pin: there is no upstream release tag
recorded by the import commit, so the field set is what the code is aligned to.
That alignment is enforced by the coverage check — adding a field to the
vendored protos makes `model_traceability` fail until the field is classified,
which is exactly the moment to decide where it lands.

Which upstream snapshot this is can be narrowed from the field set if it ever
needs confirming: `LogRecord.event_name = 12`, `AnyValue.string_value_strindex = 8`
and `Resource.entity_refs = 3` all post-date the 1.0 protos.

## Two decisions taken here (reversible)

- **Schema diagrams describe the current schema only.** Migrations 000–004 are
  history; the baseline DDL is *not* the current shape (005 drops `metric`,
  `metric_series`, `metric_data_point`, `scope` and the metric views). Generate
  from a migrated database, never from the SQL files.
- **Drift checking runs as ordinary integration tests**, so it is already part
  of `cargo test --workspace` in CI and needs no new workflow step or hook.

## Findings

Collected while doing the mapping; each is a fact about the current code, not a
complaint.

1. **`dropped_attributes_count` is only persisted for the log record.** The
   Resource and InstrumentationScope versions of that field have no column and
   no counter, so scope- and resource-level drops are invisible to readers.
2. **`Resource.entity_refs` is ignored** (Development-status field).
3. **The two profiling-only string-index fields behave differently.**
   `AnyValue.string_value_strindex` maps to `null` *and* increments
   `otlp_mapping_loss_total{reason="string_value_strindex"}`;
   `KeyValue.key_strindex` is dropped silently by destructuring. The README's
   "treated as absent (`null`)" describes the first and understates the second.
4. **`flags` is stored as the full 32-bit field.** Per the proto, only bits 0–7
   are trace flags; readers must mask with `LOG_RECORD_FLAGS_TRACE_FLAGS_MASK`
   or they will read reserved bits as flags.
5. **`Severity` is a modeling smell.** The enum is an exact mirror of OTLP
   `SeverityNumber` (0–24) but is *named* Severity, is used as both the numeric
   value and the printed severity text (`as_str`), and `as_str` collapses
   `Trace2`–`Fatal4` onto the six base names, so any rendering loses the
   sub-level the column preserves. The glossary's split into *severity number*
   and *severity text* is exactly the split the type is missing.
6. **`log_resource` is first-writer-wins.** The id is a SHA-256 over the
   canonical attribute JSON, so changed attributes produce a *new* resource row,
   and an unchanged id can never update `service_name`. The same service can
   therefore appear under several `resource_id`s; the `logs` view (an inner join)
   still resolves, but a `GROUP BY service_name` over it hides that.
7. **Foreign-key enforcement depends on the connection.** The writer sets
   `PRAGMA foreign_keys = ON` (`storage/src/sqlite.rs`), so `ON DELETE CASCADE`
   holds for the application. An external `sqlite3` session defaults to `OFF`
   and will happily orphan `log_event` rows.

## Vocabulary

[GLOSSARY.md](../../GLOSSARY.md) is the canonical naming for these concepts. The
identifiers below disagree with it, mostly for historical reasons; the glossary
states the language, not the schema, so the mapping is recorded here instead.

| glossary term | schema identifier | note |
|---|---|---|
| Log record | `log_event`, `logs` view | the table says *event*, but `event_name` is nullable, so most rows are plain log records |
| Event | `event_name` | only records with an event name are events |
| Resource | `log_resource` | prefixed with the signal |
| Attribute | `attributes_json`, `scope_attributes_json` | one canonical JSON object per attribute set, not typed columns |
| Instrumentation scope | `scope_name`, `scope_version`, `scope_attributes_json`, `scope_schema_url` | "scope" is shorthand for instrumentation scope |
| Timestamp | `timestamp_ns` | the unit lives in the name |
| Observed timestamp | `observed_timestamp_ns` | |
| Severity number | `severity_number` | |
| Severity text | `severity_text` | |
| Body | `body` | text or canonical JSON, never typed |
| Schema URL | `schema_url`, `scope_schema_url`, `resource_schema_url` | the view renames the resource one |
| Trace context | `trace_id`, `span_id` | nullable together |
| Dropped attributes count | `dropped_attributes_count` | record-level only; see finding 1 |
| Purge | retention deletes rows; no column | |
| Search index | `logs_fts`, `search_index_state` | derived; the state row records the installed mode and the rebuild cursor |

