<!-- GENERATED FILE - do not edit by hand. -->
# OTLP logs: mapping fidelity matrix

What happens to every field of the vendored OTLP protobufs on its way into
the domain model and the SQLite schema. Generated from
[`traceability.tsv`](traceability.tsv), which the test suite checks against the
protos and a migrated database.

```text
UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test model_traceability
```

`re-encoded` keeps the information but changes the representation (JSON, BLOB,
text). `coerced` means the contract normalizes and the original form is gone.
`dropped` means it is not stored at all; `loss_counter` names the
`otlp_mapping_loss_total` reason that records the loss.

## Summary

| classification | rows |
|---|---:|
| `mapped` | 16 |
| `re-encoded` | 12 |
| `coerced` | 2 |
| `dropped` | 4 |
| `structural` | 6 |
| `derived` | 6 |
| **total rows** | **46** |

Rows, not fields: one proto field fans out when persistence denormalizes
it (a scope's name, version and attributes all land on every `log_event` row).

## Message coverage

Every message in the vendored protos is either classified or excluded with a
reason — the test fails on anything unaccounted for.

| message | fields classified |
|---|---:|
| `ExportLogsServiceRequest` | 1 |
| `ResourceLogs` | 3 |
| `ScopeLogs` | 3 |
| `LogRecord` | 11 |
| `Resource` | 3 |
| `InstrumentationScope` | 4 |
| `KeyValue` | 3 |
| `AnyValue` | 8 |
| `ArrayValue` | 1 |
| `KeyValueList` | 1 |
| `LogsData` | excluded: canonical container for file embeds and non-OTLP transports; the gRPC path delivers ExportLogsServiceRequest, so it is compiled but never instantiated |
| `EntityRef` | excluded: Development-status entity references; not consumed |
| `ExportLogsServiceResponse` | excluded: response carries no log data; the sink answers with an empty success or an error status |
| `ExportLogsPartialSuccess` | excluded: partial-success bookkeeping; this sink rejects or accepts whole requests, so it never populates it |

## `ExportLogsServiceRequest`

Full name: `opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest`

| proto field | domain | column | classification | loss counter | note |
|---|---|---|---|---|---|
| `resource_logs` | `ResourceLogs` | — | `structural` | — | the RPC request body; one element per resource group, each becoming one chunk |

## `ResourceLogs`

Full name: `opentelemetry.proto.logs.v1.ResourceLogs`

| proto field | domain | column | classification | loss counter | note |
|---|---|---|---|---|---|
| `resource` | `Resource` | `log_resource.id` | `structural` | — | resource group; persisted once per distinct derived id |
| `scope_logs` | `LogScope` | — | `structural` | — | the scope group vector; one shared LogScope per element |
| `schema_url` | `Resource::schema_url` | `log_resource.schema_url` | `mapped` | — | resource-level schema URL; empty stored as NULL |

## `ScopeLogs`

Full name: `opentelemetry.proto.logs.v1.ScopeLogs`

| proto field | domain | column | classification | loss counter | note |
|---|---|---|---|---|---|
| `scope` | `LogScope::name` | `log_event.scope_name` | `mapped` | — | scope is denormalized onto every log_event row instead of a scope dimension table |
| `scope` | `LogScope::version` | `log_event.scope_version` | `mapped` | — | empty version stored as NULL |
| `scope` | `LogScope::attributes_json` | `log_event.scope_attributes_json` | `re-encoded` | — | canonical JSON rendered once per ScopeLogs group and shared by every record through Arc<LogScope> |
| `log_records` | `LogRecord` | — | `structural` | — | the record vector itself; each element becomes one log_event row |
| `schema_url` | `LogScope::schema_url` | `log_event.scope_schema_url` | `mapped` | — | per-scope schema URL; never merged with the resource schema URL |

## `LogRecord`

Full name: `opentelemetry.proto.logs.v1.LogRecord`

| proto field | domain | column | classification | loss counter | note |
|---|---|---|---|---|---|
| `time_unix_nano` | `LogRecord::time_unix_nano` | `log_event.timestamp_ns` | `mapped` | `timestamp_overflow` | unset 0 is replaced by the observed timestamp per the proto's recommended fallback; values above i64::MAX saturate instead of wrapping negative |
| `observed_time_unix_nano` | `LogRecord::observed_time_unix_nano` | `log_event.observed_timestamp_ns` | `mapped` | `timestamp_overflow` | same saturation rule as time_unix_nano |
| `severity_number` | `LogRecord::severity_number` | `log_event.severity_number` | `coerced` | `unknown_severity` | stored as the OTLP numeric value 0..=24; anything outside that range becomes UNSPECIFIED (0) |
| `severity_text` | `LogRecord::severity_text` | `log_event.severity_text` | `mapped` | — | kept verbatim; empty string is stored as NULL |
| `body` | `LogRecord::body` | `log_event.body` | `re-encoded` | — | scalar bodies render as text (bytes as lossy UTF-8, int/double/bool as their decimal form); structured bodies persist canonical JSON through LogRecord::body_json into the same column |
| `attributes` | `LogRecord::attributes` | `log_event.attributes_json` | `re-encoded` | — | canonical JSON object: keys sorted ascending, duplicate keys collapse to the last value, bytes hex-encoded |
| `dropped_attributes_count` | `LogRecord::dropped_attributes_count` | `log_event.dropped_attributes_count` | `mapped` | — | kept verbatim; counts names dropped by the producer, not by this sink |
| `flags` | `LogRecord::flags` | `log_event.flags` | `mapped` | — | stored as the full 32-bit bit-field; readers must mask LOG_RECORD_FLAGS_TRACE_FLAGS_MASK before reading trace flags |
| `trace_id` | `LogRecord::trace_id` | `log_event.trace_id` | `re-encoded` | `invalid_trace_id` | 16 bytes to BLOB; absent stores NULL; present but wrong length or all zeroes stores NULL and counts |
| `span_id` | `LogRecord::span_id` | `log_event.span_id` | `re-encoded` | `invalid_span_id` | 8 bytes to BLOB; absent stores NULL; present but wrong length or all zeroes stores NULL and counts |
| `event_name` | `LogRecord::event_name` | `log_event.event_name` | `mapped` | — | identifies the record as an Event when set, per the proto comment; empty string stored as NULL |

## `Resource`

Full name: `opentelemetry.proto.resource.v1.Resource`

| proto field | domain | column | classification | loss counter | note |
|---|---|---|---|---|---|
| `attributes` | `Resource::attributes` | `log_resource.attributes_json` | `re-encoded` | — | canonical JSON using the same encoding rules as every other attribute column |
| `attributes` | `Resource::service_name` | `log_resource.service_name` | `derived` | — | extracted from the service.name attribute; last duplicate wins; empty string when absent |
| `attributes` | `Resource::host_name` | `log_resource.host_name` | `derived` | — | extracted from the host.name attribute; NULL when absent or not a string |
| `dropped_attributes_count` | — | — | `dropped` | — | no column and no counter: only the log record's own dropped count is persisted |
| `entity_refs` | — | — | `dropped` | — | Development-status entity references; not stored |

## `InstrumentationScope`

Full name: `opentelemetry.proto.common.v1.InstrumentationScope`

| proto field | domain | column | classification | loss counter | note |
|---|---|---|---|---|---|
| `name` | `LogScope::name` | `log_event.scope_name` | `mapped` | — | empty scope name stored as NULL |
| `version` | `LogScope::version` | `log_event.scope_version` | `mapped` | — | empty scope version stored as NULL |
| `attributes` | `LogScope::attributes_json` | `log_event.scope_attributes_json` | `re-encoded` | — | scope attributes are never merged into the log record's attributes_json |
| `dropped_attributes_count` | — | — | `dropped` | — | no column and no counter: scope-level drops are invisible to readers |

## `KeyValue`

Full name: `opentelemetry.proto.common.v1.KeyValue`

| proto field | domain | column | classification | loss counter | note |
|---|---|---|---|---|---|
| `key` | `Attribute::key` | `log_event.attributes_json` | `mapped` | — | becomes a JSON object key; duplicates collapse to the last value |
| `value` | `Attribute::value` | `log_event.attributes_json` | `re-encoded` | — | rendered through AttributeValue's canonical encoding, so nested values use the same rules |
| `key_strindex` | — | — | `dropped` | — | Profiling-only dictionary reference: silently ignored by destructuring; unlike string_value_strindex this loss is not counted |

## `AnyValue`

Full name: `opentelemetry.proto.common.v1.AnyValue`

| proto field | domain | column | classification | loss counter | note |
|---|---|---|---|---|---|
| `string_value` | `AttributeValue::String` | `log_event.attributes_json` | `mapped` | — | JSON string |
| `bool_value` | `AttributeValue::Bool` | `log_event.attributes_json` | `mapped` | — | JSON boolean |
| `int_value` | `AttributeValue::Int` | `log_event.attributes_json` | `mapped` | — | JSON number (int64 preserved exactly) |
| `double_value` | `AttributeValue::Double` | `log_event.attributes_json` | `re-encoded` | — | non-finite values (NaN, +Inf, -Inf) become JSON null instead of failing serialization |
| `array_value` | `AttributeValue::Array` | `log_event.attributes_json` | `re-encoded` | — | arrays stay arrays rather than being flattened to a string |
| `kvlist_value` | `AttributeValue::Kvlist` | `log_event.attributes_json` | `re-encoded` | — | keys sorted ascending with duplicate keys collapsed to the last value |
| `bytes_value` | `AttributeValue::Bytes` | `log_event.attributes_json` | `re-encoded` | — | hex-encoded inside JSON columns; lossy UTF-8 when the value is a log body |
| `string_value_strindex` | — | `log_event.attributes_json` | `coerced` | `string_value_strindex` | Profiling-only dictionary reference: the value becomes JSON null and the loss is counted, following the proto note to treat it as absent |

## `ArrayValue`

Full name: `opentelemetry.proto.common.v1.ArrayValue`

| proto field | domain | column | classification | loss counter | note |
|---|---|---|---|---|---|
| `values` | `AttributeValue::Array` | `log_event.attributes_json` | `structural` | — | wrapper that makes a repeated AnyValue legal where `oneof` cannot repeat |

## `KeyValueList`

Full name: `opentelemetry.proto.common.v1.KeyValueList`

| proto field | domain | column | classification | loss counter | note |
|---|---|---|---|---|---|
| `values` | `AttributeValue::Kvlist` | `log_event.attributes_json` | `structural` | — | wrapper that makes a repeated KeyValue legal where `oneof` cannot repeat |

## Columns with no wire field

Computed by the sink; a reader of the database needs these to interpret rows.

| column | domain | note |
|---|---|---|
| `log_resource.id` | `Resource::id` | SHA-256 over length-prefixed service_name, host_name, schema_url and canonical attributes JSON; insert is ON CONFLICT (id) DO NOTHING, so the first sighting wins |
| `log_event.resource_id` | `Resource::id` | foreign key to the derived resource id; ON DELETE CASCADE |
| `log_event.id` | `LogRecord::id` | INTEGER PRIMARY KEY, i.e. the rowid alias; the FTS index uses it as its rowid |
| `log_resource.created_at` | — | first-insert timestamp only, because later upserts hit ON CONFLICT DO NOTHING |
