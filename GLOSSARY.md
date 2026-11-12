# OTLP SQLite Sink

A logs-only OpenTelemetry sink: it receives exported telemetry and stores it.
It terminates the pipeline and never forwards data, so it is a store, not a
stage in a relay.

## Language

### Signals and role

**Log record**:
A timestamped recording produced by an application, carrying a body and
optionally severity, attributes and trace context.
_Avoid_: log event, log entry, log line

**Event**:
A log record that carries an event name, which identifies it as a known event
type. Every event is a log record; not every log record is an event.
_Avoid_: log, log entry, message

**Signal**:
A category of telemetry: logs, traces or metrics. This sink serves logs only.
_Avoid_: data source, telemetry type, stream

**Sink**:
A component that terminates a telemetry pipeline by storing telemetry instead
of forwarding it.
_Avoid_: exporter, backend, collector, receiver, processor, destination

### Log record content

**Body**:
The main payload of a log record, which may be a single value or a nested
structure.
_Avoid_: message, payload, text, line

**Attribute**:
A key-value pair describing the producer of the telemetry or the event itself.
_Avoid_: tag, label, dimension, metadata, property

**Resource**:
The set of attributes that identify the producer of the telemetry.
_Avoid_: service, host, source, origin, agent

**Instrumentation scope**:
The library that produced the telemetry, identified by a name and a version.
_Avoid_: scope, logger, module, package, library

**Severity number**:
The severity of a log record on the normalized OpenTelemetry numeric scale.
_Avoid_: level, log level, priority, severity

**Severity text**:
The severity as the producer spelled it, kept verbatim rather than derived from
the severity number.
_Avoid_: level, log level, severity label

**Timestamp**:
When the event occurred, as reported by the producer.
_Avoid_: time, created at, event time

**Observed timestamp**:
When the collection system observed the event, as distinct from when it
occurred.
_Avoid_: received at, ingest time, arrival time

**Event name**:
The identifier of the event type a log record describes.
_Avoid_: name, event type, category, kind

**Trace context**:
The trace ID and span ID that link a log record to a trace. Both are needed;
either one alone is not trace context.
_Avoid_: correlation ID, request ID, trace link

**Schema URL**:
The URL identifying the schema that a resource or an instrumentation scope conforms to.
_Avoid_: schema version, schema ID

**Dropped attributes count**:
How many attributes the producer discarded before export.
_Avoid_: dropped count, lost attributes

### Fidelity

**Mapping fidelity**:
The guarantee that a supported field is never silently converted, flattened or
discarded on its way into storage.
_Avoid_: lossless, round-trip

**Mapping loss**:
A field that is deliberately not stored, or normalized until its original form
is gone, and is therefore counted instead of silently dropped.
_Avoid_: dropped field, data loss, gap

**Quarantine**:
Discarding one unusable log record while the rest of its batch still commits.
_Avoid_: skip, drop, dead letter

### Pipeline

**Admission**:
Accepting an export request whole or rejecting it whole; there is no partial
acceptance.
_Avoid_: accept, backpressure, throttle

**Durable acknowledgement**:
A success reply that is sent only once the data is committed and would survive
a crash.
_Avoid_: ack, confirmation, receipt

**Retention**:
The age after which log records are no longer kept.
_Avoid_: TTL, expiry, cleanup

**Purge**:
Deleting log records that are past their retention.
_Avoid_: prune, trim, delete, vacuum

**Backup retention**:
How many backup files are kept before the oldest are removed.
_Avoid_: keep count, retention window

**Prune**:
Deleting backup files that are beyond the backup retention count.
_Avoid_: purge, trim, delete

### Search

**Search index**:
The derived FTS5 index over stored log records. It is never part of the
canonical data: every entry can be re-derived from `log_event`.
_Avoid_: FTS index, full-text index, search table

**Index mode**:
Which shape the search index is installed in: `off` (present but empty, with no
triggers), `body` (the default, indexing only the body) or `full` (the legacy
six-column index).
_Avoid_: index type, search level, FTS mode

**Reconcile**:
Comparing the configured index mode with the installed shape at startup and
installing a matching shape, without scanning `log_event`.
_Avoid_: sync, synchronize, repair

**Rebuild**:
The bounded, resumable backfill of the search index that follows a mode change
or the asynchronous `006` upgrade; results are partial until it finishes.
_Avoid_: reindex, refresh

## Spelling and capitalization

- *OpenTelemetry* is one unhyphenated capitalized word.
- Use *OTel*, never `OTEL`.
- *OTLP* and *OTEP* are written in capitals.
- *Collector* is capitalized when it names the OpenTelemetry Collector — a
  component this project is not.
