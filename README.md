# OTLP SQLite Collector

A small OpenTelemetry log collector that receives OTLP over gRPC and stores
logs in SQLite. It uses bounded queues and one SQLite writer goroutine.

> Early-stage project: review the operational and security notes before using it
> for production data.

## Features

- OTLP logs over gRPC
- Bounded ingestion and command queues
- SQLite WAL mode, migrations, retention, checkpoints, optimization, and FTS5
- Prometheus metrics and an HTTP health endpoint
- Optional rule-based alerts with log or HTTP-webhook notifiers
- YAML configuration with environment-variable overrides

## Quick start

Requirements: Go 1.25+, a C compiler (CGO), `protoc`,
`protoc-gen-go`, and `protoc-gen-go-grpc`.

```bash
git clone https://codeberg.org/nicknad/otel-sqlite.git
cd otel-sqlite
make generate   # required when protobuf sources change
make build
make run
```

The collector listens on gRPC `:4317`, exposes metrics and health on HTTP
`:9090`, and writes `otel-logs.db` in the working directory.

Send logs with any OTLP-compatible client. The collector implements the OTLP
Logs gRPC service only; it does not provide a query API.

## Docker

```bash
docker build -t otel-sqlite-collector .
docker run --rm \
  -p 4317:4317 -p 9090:9090 \
  -v otel-data:/var/lib/otel-collector \
  otel-sqlite-collector
```

For a development stack with Prometheus and Grafana:

```bash
docker compose up -d
```

The compose file includes Prometheus and Grafana and exposes Grafana with its
upstream default credentials. Do not use that stack unchanged in production.

## Configuration

Set `CONFIG_FILE` to load YAML. Environment variables override YAML, which
overrides defaults. See [`config.example.yaml`](config.example.yaml) for the
complete reference.

Common environment variables:

| Variable | Default | Purpose |
|---|---:|---|
| `LISTEN_ADDRESS` | `:4317` | OTLP gRPC listen address |
| `METRICS_ADDRESS` | `:9090` | Metrics/health HTTP listen address |
| `SQLITE_PATH` | `otel-logs.db` | SQLite database path |
| `INGRESS_QUEUE_CAPACITY` | `10000` | Ingress batch queue capacity |
| `BATCH_QUEUE_CAPACITY` | `5000` | Command queue capacity |
| `BATCHER_BATCH_SIZE` | `500` | Records per write command |
| `BATCHER_FLUSH_INTERVAL` | `1s` | Maximum batcher flush interval |
| `WRITER_BATCH_SIZE` | `50` | Commands collected per transaction |
| `WRITER_FLUSH_INTERVAL` | `1s` | Maximum writer flush interval |
| `WRITER_MAX_TRANSACTION_RECORDS` | `10000` | Records per transaction limit |
| `SHUTDOWN_TIMEOUT` | `30s` | Graceful shutdown timeout |
| `CONFIG_FILE` | — | YAML file path |

`BATCH_SIZE` and `FLUSH_INTERVAL` are deprecated aliases for both batcher and
writer settings when their specific variables are unset. Maintenance and
notification settings are also documented in the example file.

## Backpressure and delivery

The storage path applies backpressure: full queues block exporters until the
request context ends. Set `INGRESS_QUEUE_BACKPRESSURE_THRESHOLD` to reject new
requests early with gRPC `Unavailable`; clients should retry those requests.

Notifications are a side path. Their bounded queue is intentionally
non-blocking, so notification events can be dropped when it is full; this does
not affect SQLite ingestion. Delivery failures are retried and eventually sent
to a persistent dead-letter queue.

## Notifications

Enable `notification` in YAML or set `NOTIFICATION_ENABLED=true`. Rules support
severity thresholds, regular expressions for resource IDs and bodies, and
attribute filters. Alerts aggregate by rule/resource and transition through
`pending`, `firing`, and `resolved` states. Built-in notifiers are `log` and
`http`; see the example configuration for the compact YAML shape.

## Endpoints and security

- `4317`: unauthenticated, plaintext OTLP gRPC
- `9090/metrics`: unauthenticated Prometheus metrics
- `9090/health`: HTTP liveness check returning `200 OK`

The collector does not implement TLS or authentication. Bind it to a trusted
network or put it behind a TLS/authenticating proxy. Protect the database,
notification state files, webhook URLs, and `auth_header` values.

## Storage

Migrations run automatically on startup. The main tables are:

- `log_resource`: deduplicated resource metadata
- `log_event`: log records and inline `attributes_json`
- `logs`: read-side view joining events to resources
- `logs_fts`: contentless FTS5 index for `body` and `service_name`

`logs_fts` is rebuilt periodically by maintenance and may be empty before its
first rebuild. Migration 005 converts legacy `log_attr` rows to
`log_event.attributes_json`; update external readers that use the old table.

SQLite is a single-writer store. Run one collector per database file; do not
share a writable database file between collector instances.

## Development

```bash
make test       # CGO, fts5, race-enabled tests
make lint
make fmt
```

Load-test profiles and historical, host-specific measurements are in
[`docs/loadtest-baseline.md`](docs/loadtest-baseline.md). The design summary is
in [`docs/architecture.md`](docs/architecture.md).

## Contributing and support

See [`CONTRIBUTING.md`](CONTRIBUTING.md), [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md),
and [`SECURITY.md`](SECURITY.md).

Use issues for reproducible bugs, focused feature requests, and documentation
corrections. Search existing issues first and redact secrets from logs and
configurations. Maintainer responses are volunteer-based; no response-time or
support SLA is promised. Report security issues privately as described in
`SECURITY.md`.

## License

[Apache License 2.0](LICENSE). Copyright 2024 nnadolski.
