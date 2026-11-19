#!/usr/bin/env bash
# Acceptance test for a PUBLISHED image: the executable form of the release
# contract ("a stranger runs one command with zero flags and it works").
#
#   OTEL_SQLITE_IMAGE=ghcr.io/nicknad/otel-sqlite@sha256:... \
#     bash tests/docker/acceptance.sh
#
# Verifies, against the image AS PUBLISHED (not a local build):
#   1. zero-flag start: no config mount, no env overrides - the image's baked
#      default config must bind OTLP on all interfaces, or the load client
#      could never reach the sink across the compose network;
#   2. real OTLP load lands durably (load + validate use the same accounting
#      as the crash suite: every acknowledged sequence exactly once);
#   3. SIGTERM drain: `docker stop` exits 0, logs end with "shutdown complete",
#      and no flush barrier refusal or watchdog halt appears;
#   4. restart on the same volume: every record from (2) is still there.
#
# State is discarded on exit (`down -v`), so every run starts clean.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE=(docker compose -f tests/docker/compose.acceptance.yml)
SINK="acceptance-sink"
ENDPOINT="http://sink:4317"

if [ -z "${OTEL_SQLITE_IMAGE:-}" ]; then
    echo "error: set OTEL_SQLITE_IMAGE to the published image ref (digest or tag)" >&2
    exit 2
fi

# Unique per invocation so a rerun can never validate against rows left behind
# by a previous failed attempt on a surviving volume.
RUN_ID="acceptance-$(date +%Y%m%d%H%M%S)-$$"

cd "$REPO_ROOT"

cleanup() {
    "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== build loadgen (bench image: load/validate harness) =="
"${COMPOSE[@]}" build loadgen

echo "== start published sink with ZERO flags =="
"${COMPOSE[@]}" up -d --wait sink
echo "sink healthy (built-in gRPC healthcheck)"

echo "== phase 1: real OTLP load run-a-$RUN_ID =="
"${COMPOSE[@]}" run --rm loadgen load \
    --endpoint "$ENDPOINT" \
    --run-id "run-a-$RUN_ID" \
    --duration-secs 6 \
    --output "/artifacts/run-a-$RUN_ID.json"

echo "== validate round-trip against the shared volume =="
"${COMPOSE[@]}" run --rm loadgen validate \
    --db-path /data/otel-logs.db \
    --manifest "/artifacts/run-a-$RUN_ID.json"

echo "== SIGTERM drain (docker stop) =="
"${COMPOSE[@]}" stop --timeout 20 sink

EXIT_CODE="$(docker inspect --format '{{.State.ExitCode}}' "$SINK")"
if [ "$EXIT_CODE" != "0" ]; then
    echo "error: sink exited with $EXIT_CODE after SIGTERM (expected 0)" >&2
    docker logs "$SINK" 2>&1 | tail -30 || true
    exit 1
fi
if ! docker logs "$SINK" 2>&1 | grep -q "shutdown complete: records_written="; then
    echo "error: no 'shutdown complete' line in sink logs after SIGTERM" >&2
    docker logs "$SINK" 2>&1 | tail -30 || true
    exit 1
fi
if docker logs "$SINK" 2>&1 | grep -qE "flush barrier refused|watchdog halted"; then
    echo "error: drain reported a flush barrier refusal or watchdog halt" >&2
    docker logs "$SINK" 2>&1 | tail -30 || true
    exit 1
fi
echo "graceful drain confirmed (exit 0, shutdown complete, no stranded chunks)"

echo "== restart on the same volume =="
"${COMPOSE[@]}" up -d --wait --force-recreate sink
echo "sink recovered and healthy"

echo "== phase 2: post-restart load run-b-$RUN_ID =="
"${COMPOSE[@]}" run --rm loadgen load \
    --endpoint "$ENDPOINT" \
    --run-id "run-b-$RUN_ID" \
    --duration-secs 4 \
    --output "/artifacts/run-b-$RUN_ID.json"

echo "== validate both manifests against the shared database =="
"${COMPOSE[@]}" run --rm loadgen validate \
    --db-path /data/otel-logs.db \
    --manifest "/artifacts/run-a-$RUN_ID.json"
"${COMPOSE[@]}" run --rm loadgen validate \
    --db-path /data/otel-logs.db \
    --manifest "/artifacts/run-b-$RUN_ID.json"

echo
echo "PASS: published image accepted real OTLP load with zero flags, drained gracefully on SIGTERM, and survived a restart without losing acknowledged records"
