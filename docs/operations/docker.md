# Docker operations

The published image lives at `ghcr.io/nicknad/otel-sqlite` as one multi-arch
manifest list (`linux/amd64` + `linux/arm64`), built natively per
architecture, keyless-signed, with provenance/SBOM attestations visible in
GHCR. The release gate that says it works - zero-flag start, real OTLP
round-trip, graceful SIGTERM drain, restart survival - runs against the
*published* image by digest on both architectures in
`.github/workflows/publish.yml`, and the README quickstart is executed
verbatim by CI so the docs cannot drift.

## Configuration precedence

1. Built-in defaults: loopback bind, fails closed (bare-metal behaviour,
   unchanged).
2. The TOML file named by `OTEL_SQLITE_CFG_PATH`. The image points it at
   `/etc/otel-sqlite/otel-sqlite.toml` - the container default that sets
   `listen_address = ":4317"` (all interfaces) and
   `allow_insecure_remote = true`.
3. `OTEL_SQLITE_*` environment variables - one per setting, and the
   environment wins over the file.

So `docker run` with zero flags serves OTLP in cleartext on every interface
the container is on: suitable for isolated/trusted networks, and one
environment variable away from TLS. No mount needed for any override.

## Environment variable reference

Every setting the TOML file accepts has an environment form; values keep their
TOML shapes (strings raw, booleans `true|false`, integers plain digits,
durations with a unit or bare seconds, floats like `0.8`).

| Variable | Setting | Example form |
|---|---|---|
| `OTEL_SQLITE_LISTEN_ADDRESS` | `listen_address` | `":4317"` |
| `OTEL_SQLITE_GRPC_MAX_RECV_MSG_SIZE` | `grpc_max_recv_msg_size` | integer |
| `OTEL_SQLITE_GRPC_MAX_CONCURRENT_STREAMS` | `grpc_max_concurrent_streams` | integer |
| `OTEL_SQLITE_MAPPING_CONCURRENCY` | `mapping_concurrency` | integer |
| `OTEL_SQLITE_MAX_RECORDS_PER_REQUEST` | `max_records_per_request` | integer |
| `OTEL_SQLITE_MAX_ATTRIBUTES_PER_RECORD` | `max_attributes_per_record` | integer |
| `OTEL_SQLITE_MAX_ATTRIBUTE_KEY_BYTES` | `max_attribute_key_bytes` | integer |
| `OTEL_SQLITE_MAX_ATTRIBUTE_VALUE_BYTES` | `max_attribute_value_bytes` | integer |
| `OTEL_SQLITE_MAX_BODY_BYTES` | `max_body_bytes` | integer |
| `OTEL_SQLITE_MAX_SCOPE_METADATA_EXPANSION_BYTES` | `max_scope_metadata_expansion_bytes` | integer |
| `OTEL_SQLITE_SHUTDOWN_TIMEOUT` | `shutdown_timeout` | `"30s"` |
| `OTEL_SQLITE_SQLITE_PATH` | `sqlite_path` | path |
| `OTEL_SQLITE_COMMAND_QUEUE_CAPACITY` | `command_queue_capacity` | integer |
| `OTEL_SQLITE_BATCHER_MAX_BATCH_RECORDS` | `batcher_max_batch_records` | integer |
| `OTEL_SQLITE_BATCHER_MAX_BATCH_AGE` | `batcher_max_batch_age` | `"250ms"` |
| `OTEL_SQLITE_GROUP_COMMIT_MAX_RECORDS` | `group_commit_max_records` | integer |
| `OTEL_SQLITE_MAX_DB_BYTES` | `max_db_bytes` | integer |
| `OTEL_SQLITE_INGEST_QUEUE_CAPACITY` | `ingest_queue_capacity` | integer |
| `OTEL_SQLITE_METRICS_ADDRESS` | `metrics_address` | `"127.0.0.1:8888"` or `"off"` |
| `OTEL_SQLITE_ALLOW_REMOTE_METRICS` | `allow_remote_metrics` | `true|false` |
| `OTEL_SQLITE_ALLOW_INSECURE_REMOTE` | `allow_insecure_remote` | `true|false` |
| `OTEL_SQLITE_TLS_CERT` | `tls.cert` | path |
| `OTEL_SQLITE_TLS_KEY` | `tls.key` | path |
| `OTEL_SQLITE_TLS_CLIENT_CA` | `tls.client_ca` | path (enables mTLS) |
| `OTEL_SQLITE_AUTH_MODE` | `auth.mode` | `none|token` |
| `OTEL_SQLITE_AUTH_TOKEN_FILE` | `auth.token_file` | path |
| `OTEL_SQLITE_DURABILITY_MODE` | `durability.mode` | `commit|enqueue` |
| `OTEL_SQLITE_DURABILITY_SYNCHRONOUS` | `durability.synchronous` | `normal|full` |
| `OTEL_SQLITE_SEARCH_MODE` | `search.mode` | `off|body|full` |
| `OTEL_SQLITE_MAINTENANCE_RETENTION` | `maintenance.retention` | `"7d"` or `"off"` (uniform window; the per-signal table form is TOML-only) |
| `OTEL_SQLITE_MAINTENANCE_PURGE_INTERVAL` | `maintenance.purge_interval` | `"15m"` |
| `OTEL_SQLITE_MAINTENANCE_CHECKPOINT_INTERVAL` | `maintenance.checkpoint_interval` | `"5m"` or `"off"` |
| `OTEL_SQLITE_MAINTENANCE_CHECKPOINT_MODE` | `maintenance.checkpoint_mode` | `passive|full|restart|truncate` |
| `OTEL_SQLITE_MAINTENANCE_OPTIMIZE_INTERVAL` | `maintenance.optimize_interval` | `"12h"` or `"off"` |
| `OTEL_SQLITE_MAINTENANCE_VACUUM_INTERVAL` | `maintenance.vacuum_interval` | `"1d"` or `"off"` |
| `OTEL_SQLITE_MAINTENANCE_REBUILD_FTS_INTERVAL` | `maintenance.rebuild_fts_interval` | `"off"` |
| `OTEL_SQLITE_MAINTENANCE_RETRY_DELAY` | `maintenance.retry_delay` | `"10s"` |
| `OTEL_SQLITE_WATCHDOG_TICK_INTERVAL` | `watchdog.tick_interval` | duration |
| `OTEL_SQLITE_WATCHDOG_DEGRADED_AFTER` | `watchdog.degraded_after` | duration |
| `OTEL_SQLITE_WATCHDOG_UNHEALTHY_AFTER` | `watchdog.unhealthy_after` | duration |
| `OTEL_SQLITE_WATCHDOG_ACK_STALL_AFTER` | `watchdog.ack_stall_after` | duration |
| `OTEL_SQLITE_WATCHDOG_MAINTENANCE_STALL_AFTER` | `watchdog.maintenance_stall_after` | duration |
| `OTEL_SQLITE_WATCHDOG_QUEUE_PRESSURE_RATIO` | `watchdog.queue_pressure_ratio` | `0.8` |
| `OTEL_SQLITE_WATCHDOG_HALT_ON_UNHEALTHY` | `watchdog.halt_on_unhealthy` | `true|false` |

Unknown `OTEL_SQLITE_*` names warn at startup and are ignored; the prefix is
shared with reserved, non-config variables (`OTEL_SQLITE_CFG_PATH`,
`OTEL_SQLITE_HEALTHCHECK_ENDPOINT`, the fault-injection `OTEL_SQLITE_FAULT_*`
knobs and the harness's `OTEL_SQLITE_BIN` / `OTEL_SQLITE_CRASH_BIN` /
`OTEL_SQLITE_E2E_*` / `OTEL_SQLITE_BENCH_*`).

TLS + bearer tokens without any config file:

```sh
docker run -d --name otel-sqlite -p 4317:4317 \
  -v otel-sqlite-data:/data \
  -v "$PWD/certs:/certs:ro" \
  -v "$PWD/clients.txt:/run/secrets/tokens:ro" \
  -e OTEL_SQLITE_TLS_CERT=/certs/server.pem \
  -e OTEL_SQLITE_TLS_KEY=/certs/server.key \
  -e OTEL_SQLITE_TLS_CLIENT_CA=/certs/ca.pem \
  -e OTEL_SQLITE_AUTH_MODE=token \
  -e OTEL_SQLITE_AUTH_TOKEN_FILE=/run/secrets/tokens \
  ghcr.io/nicknad/otel-sqlite:latest
```

Generate the certificates once via `otel-sqlite gen-certs` (the image ships
it); see the README security section for the PKI contract.

## Compose

`deploy/docker/compose.yaml` is the declarative form of the quickstart, with
the TLS/auth overrides commented in place.

## Tags and digest pinning

- `vX.Y.Z` - one release, immutable content, multi-arch manifest.
- `X.Y` and `latest` - rolling pointers.
- Pin deployments by digest for reproducibility; the publish workflow records
  each release digest in its job summary:

```sh
docker run -v otel-sqlite-data:/data -p 4317:4317 \
  ghcr.io/nicknad/otel-sqlite@sha256:<digest-from-the-release-summary>
```

Runbook for cutting a release (tags, attestations, cosign verification):
[release.md](release.md).

## Healthcheck

The image ships a built-in `HEALTHCHECK` running `otel-sqlite healthcheck`:
a real `grpc.health.v1.Check` against the OTLP listener, TLS-aware (under
mTLS it presents the server's own clientAuth identity). `SERVING` requires
the writer and batcher to be alive; drain flips it to `NOT_SERVING` before
tearing down, so orchestrators stop routing before records are lost. Override
the probed address with `OTEL_SQLITE_HEALTHCHECK_ENDPOINT`.

## Backups

The operational subcommands ship in the image - run them through the same
image with the data volume mounted:

```sh
docker run --rm -v otel-sqlite-data:/data -v "$PWD/backups:/backups" \
  ghcr.io/nicknad/otel-sqlite:latest \
  backup --db /data/otel-logs.db --out /backups --keep 30
```

Runbook: [backup-restore.md](backup-restore.md).

## Upgrades

Migrations are forward-only and run at startup before the listener binds; a
binary refuses a database newer than itself, and rollback means restoring a
backup ([upgrades.md](upgrades.md)). For images: pull the new tag and restart
- the data volume carries the database, and the startup gates apply.

## arm64

One manifest list; `linux/arm64` is built on a native arm64 runner and the
acceptance suite runs there too, so arm64 is tested, not just built.
