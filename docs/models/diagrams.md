<!-- GENERATED FILE - do not edit by hand. -->
# Data models, rendered

Embedded copies of the `.mmd` sources next to this file, because GitHub
renders `mermaid` fences but not `.mmd` files. The sources are authoritative:
edit and regenerate those, never the fences below.

```text
UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test model_traceability
```

## Wire model: how an OTLP logs export nests

Source: [`wire.mmd`](wire.mmd)

```mermaid
%% HAND-WRITTEN (not generated). The wire model: how an OTLP logs export nests.
%% Source of truth: crates/otel-sqlite-ingress/proto/opentelemetry/proto/**.
%% Field-level fidelity per message is in fidelity-matrix.md; this diagram only
%% shows containment and repetition, which is what explains the row shapes.
classDiagram
    class ExportLogsServiceRequest {
        +ResourceLogs[] resource_logs
    }
    class ResourceLogs {
        +Resource resource
        +ScopeLogs[] scope_logs
        +string schema_url
    }
    class ScopeLogs {
        +InstrumentationScope scope
        +LogRecord[] log_records
        +string schema_url
    }
    class Resource {
        +KeyValue[] attributes
        +uint32 dropped_attributes_count
        +EntityRef[] entity_refs
    }
    class InstrumentationScope {
        +string name
        +string version
        +KeyValue[] attributes
        +uint32 dropped_attributes_count
    }
    class LogRecord {
        +fixed64 time_unix_nano
        +fixed64 observed_time_unix_nano
        +SeverityNumber severity_number
        +string severity_text
        +AnyValue body
        +KeyValue[] attributes
        +uint32 dropped_attributes_count
        +fixed32 flags
        +bytes trace_id
        +bytes span_id
        +string event_name
    }
    class KeyValue {
        +string key
        +AnyValue value
        +int32 key_strindex
    }
    class AnyValue {
        <<union>>
        +string string_value
        +bool bool_value
        +int64 int_value
        +double double_value
        +ArrayValue array_value
        +KeyValueList kvlist_value
        +bytes bytes_value
        +int32 string_value_strindex
    }
    class ArrayValue {
        +AnyValue[] values
    }
    class KeyValueList {
        +KeyValue[] values
    }

    ExportLogsServiceRequest --> ResourceLogs : resource_logs
    ResourceLogs --> Resource : resource
    ResourceLogs --> ScopeLogs : scope_logs
    ScopeLogs --> InstrumentationScope : scope
    ScopeLogs --> LogRecord : log_records
    Resource --> KeyValue : attributes
    InstrumentationScope --> KeyValue : attributes
    LogRecord --> KeyValue : attributes
    LogRecord --> AnyValue : body
    KeyValue --> AnyValue : value
    AnyValue --> ArrayValue : array_value
    AnyValue --> KeyValueList : kvlist_value
    ArrayValue --> AnyValue : values
    KeyValueList --> KeyValue : values
```

## Domain model: the in-memory types and the module owning each

Source: [`domain.mmd`](domain.mmd)

```mermaid
%% GENERATED FILE - do not edit by hand. Pure Mermaid: render with any
%% Mermaid viewer or a `mermaid` fenced block.
%% Source: pub struct / pub enum declarations in crates/otel-sqlite-core/src/model/*.rs.
%% Module owning every type: `otel_sqlite_core::model` (re-export); each class
%% names its defining file in the comment above it.
%% Regenerate: UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-core --test domain_diagram
%% Arrows are derived from field/variant types: `Vec<T>` means many.
%% Enum variants render as `+payload Variant`; `Vec<u8>` is `Vec_u8` and
%% `Arc<LogScope>` collapses to `LogScope` (shared, not copied per record).
%% Self-references (`AttributeValue::Array(Vec<AttributeValue>)`) have no arrow.
classDiagram
    %% Attribute: struct in crates/otel-sqlite-core/src/model/attribute.rs
    class Attribute {
        +String key
        +AttributeValue value
    }
    %% AttributeValue: enum in crates/otel-sqlite-core/src/model/attribute.rs
    class AttributeValue {
        +Null
        +String String
        +i64 Int
        +f64 Double
        +bool Bool
        +Vec_u8 Bytes
        +Vec_AttributeValue Array
        +Vec_Attribute Kvlist
    }
    %% LogRecord: struct in crates/otel-sqlite-core/src/model/log.rs
    class LogRecord {
        +i64 time_unix_nano
        +i64 observed_time_unix_nano
        +Severity severity_number
        +String severity_text
        +u8_16 trace_id
        +u8_8 span_id
        +String body
        +Option_String body_json
        +Vec_Attribute attributes
        +u32 dropped_attributes_count
        +u32 flags
        +String event_name
        +LogScope scope
    }
    %% LogScope: struct in crates/otel-sqlite-core/src/model/log.rs
    class LogScope {
        +String name
        +String version
        +String schema_url
        +String attributes_json
    }
    %% Resource: struct in crates/otel-sqlite-core/src/model/resource.rs
    class Resource {
        +String id
        +Vec_Attribute attributes
        +String schema_url
    }
    %% Severity: enum in crates/otel-sqlite-core/src/model/severity.rs
    class Severity {
        +Unspecified
        +Trace
        +Trace2
        +Trace3
        +Trace4
        +Debug
        +Debug2
        +Debug3
        +Debug4
        +Info
        +Info2
        +Info3
        +Info4
        +Warn
        +Warn2
        +Warn3
        +Warn4
        +Error
        +Error2
        +Error3
        +Error4
        +Fatal
        +Fatal2
        +Fatal3
        +Fatal4
    }
    Attribute --> AttributeValue : value (one)
    AttributeValue --> Attribute : Kvlist (many)
    LogRecord --> Attribute : attributes (many)
    LogRecord --> LogScope : scope (one)
    LogRecord --> Severity : severity_number (one)
    Resource --> Attribute : attributes (many)
```

## Persisted schema: the current schema only, generated from a migrated database

Source: [`schema.mmd`](schema.mmd)

```mermaid
%% GENERATED FILE - do not edit by hand. Pure Mermaid: render with any
%% Mermaid viewer or a `mermaid` fenced block.
%% Source: the schema of a database migrated through 006 (see migrations/*.sql replay order).
%% Regenerate: UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test schema_diagram
%% Signal scope: logs only; migration 005 dropped every metric table.
%% `logs` is a view, `logs_fts` a contentless FTS5 index kept in sync by triggers;
%% both are dashed because neither owns storage of its own.
%% `schema_migrations` is the migration ledger, not log data.
erDiagram
    log_event ||..o{ logs : "read-side projection of log_event + log_resource"
    log_event ||..o{ logs_fts : "rowid = log_event.id, maintained by AFTER INSERT/DELETE triggers"
    log_resource ||--o{ log_event : "resource_id (on delete CASCADE)"
    log_event {
        INTEGER id PK
        TEXT resource_id "not null"
        INTEGER timestamp_ns "not null"
        INTEGER observed_timestamp_ns "not null"
        INTEGER severity_number "not null"
        TEXT severity_text
        BLOB trace_id
        BLOB span_id
        TEXT body
        TEXT event_name
        INTEGER flags "not null; default 0"
        INTEGER dropped_attributes_count "not null; default 0"
        TEXT scope_name
        TEXT scope_version
        TEXT attributes_json "not null; default '{}'"
        TEXT scope_attributes_json "not null; default '{}'"
        TEXT scope_schema_url
    }
    log_resource {
        TEXT id PK
        TEXT service_name "not null"
        TEXT host_name
        TEXT schema_url
        TEXT attributes_json "not null; default '{}'"
        TEXT created_at "not null; default CURRENT_TIMESTAMP"
    }
    logs {
        INTEGER id
        TEXT resource_id
        INTEGER timestamp_ns
        INTEGER observed_timestamp_ns
        INTEGER severity_number
        TEXT severity_text
        BLOB trace_id
        BLOB span_id
        TEXT body
        TEXT event_name
        INTEGER flags
        INTEGER dropped_attributes_count
        TEXT scope_name
        TEXT scope_version
        TEXT scope_attributes_json
        TEXT scope_schema_url
        TEXT attributes_json
        TEXT service_name
        TEXT host_name
        TEXT resource_schema_url
    }
    %% logs is a view
    logs_fts {
        ANY body
    }
    %% logs_fts is a virtual table
    schema_migrations {
        TEXT version PK
        TEXT applied_at "not null; default CURRENT_TIMESTAMP"
        TEXT description
    }
    search_index_state {
        INTEGER id PK
        TEXT mode "not null"
        TEXT status "not null"
        INTEGER last_rowid "not null; default 0"
        INTEGER rebuild_until
        TEXT updated_at "not null; default CURRENT_TIMESTAMP"
    }
    %% indexes: idx_log_event_resource_timestamp(log_event: resource_id, timestamp_ns); idx_log_event_timestamp(log_event: timestamp_ns); idx_log_event_trace_timestamp(log_event: timestamp_ns, trace_id); idx_log_resource_service(log_resource: service_name)
    %% triggers: logs_fts_ad; logs_fts_ai
```

## Conversion path: which module turns a message into a row

Source: [`pipeline.mmd`](pipeline.mmd)

```mermaid
%% HAND-WRITTEN (not generated). The path a log record takes through the
%% application, with the module that owns each conversion. The seams are the
%% interesting part: three codecs turn a proto message into a row.
flowchart TD
    client["OTLP client / SDK / Collector exporter"]

    subgraph ingress_crate["otel-sqlite-ingress"]
        grpc["grpc.rs + logs.rs<br/>LogsService.Export"]
        validate["mapping/logs.rs::validate_request<br/>size + depth bounds"]
        mapping["mapping/logs.rs + common.rs<br/>proto -> LogRecord, Resource, LogScope"]
        grpc --> validate --> mapping
    end

    subgraph core_crate["otel-sqlite-core"]
        model["model/log.rs<br/>LogRecord, LogScope<br/>model/resource.rs, attribute.rs, severity.rs"]
    end

    subgraph storage_crate["otel-sqlite-storage"]
        ingest_q[("bounded ingest queue<br/>all-or-nothing admission")]
        batcher["batcher.rs<br/>insert batcher"]
        cmd_q[("bounded command queue<br/>single consumer")]
        writer["writer.rs<br/>the only SQLite connection"]
        identity["command/identity.rs<br/>Resource -> log_resource upsert"]
        insert["command/logs.rs<br/>LogRecord -> log_event insert"]
        ingest_q --> batcher --> cmd_q --> writer
        writer --> identity
        writer --> insert
    end

    runtime["otel-sqlite-runtime<br/>scheduler.rs / worker.rs<br/>retention, checkpoint, ANALYZE, VACUUM"]

    client -->|"OTLP/gRPC"| grpc
    mapping -->|"LogChunk: BatchOrigin + records"| ingest_q
    mapping -.->|"constructs"| model
    insert -.->|"reads"| model
    runtime -->|"ordinary write commands"| cmd_q

    identity --> resource_tbl[("log_resource")]
    insert --> event_tbl[("log_event")]
    insert -.->|"AFTER INSERT trigger"| fts_tbl[("logs_fts (FTS5)")]
    event_tbl --> view_tbl[("logs view")]
    resource_tbl --> view_tbl
```
