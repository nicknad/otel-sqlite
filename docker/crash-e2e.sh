#!/usr/bin/env bash
# Container-level crash/restart durability test.
#
#   docker/crash-e2e.sh [--duration-secs N] [--kill-at-secs M]
#
# Orchestrates docker/compose.crash-e2e.yml:
#
#   1. build the benchmark image (server + driver),
#   2. start the sidecar and wait for the built-in HEALTHCHECK,
#   3. drive a `load` phase against it and `docker kill` the sidecar
#      mid-load (SIGKILL, no cleanup -> the WAL holds un-checkpointed
#      committed frames),
#   4. restart the sidecar on the same volume and wait for recovery,
#   5. drive a fresh `load` phase to prove live traffic,
#   6. validate BOTH manifests against the shared database: every
#      commit-mode-acknowledged record must be present exactly once.
#
# Exits non-zero on any failure (that is the test verdict). State is
# discarded on exit (`down -v`), so every run starts from a clean volume.

set -euo pipefail

# Locate the repo root (docker/crash-e2e.sh -> repo root).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE=(docker compose -f docker/compose.crash-e2e.yml)
SIDECAR="crash-e2e-sidecar"
ENDPOINT="http://sidecar:4317"

DURATION_SECS="${CRASH_DURATION_SECS:-10}"
KILL_AT_SECS="${CRASH_KILL_AT_SECS:-5}"

cd "$REPO_ROOT"

echo "== build benchmark image =="
"${COMPOSE[@]}" build

echo "== start sidecar =="
"${COMPOSE[@]}" up -d --wait sidecar
echo "sidecar healthy"

echo "== phase 1: load crash-a (host kills the sidecar mid-load) =="
(
  "${COMPOSE[@]}" run --rm loadgen load \
    --endpoint "$ENDPOINT" \
    --run-id crash-a \
    --duration-secs "$DURATION_SECS" \
    --output /artifacts/crash-a.json
) &
LOAD_PID=$!
sleep "$KILL_AT_SECS"
docker kill "$SIDECAR"
wait "$LOAD_PID"

echo "== restart sidecar on the same volume (WAL recovery) =="
"${COMPOSE[@]}" up -d --wait --force-recreate sidecar
echo "sidecar recovered and healthy"

echo "== phase 2: post-restart load crash-b =="
"${COMPOSE[@]}" run --rm loadgen load \
  --endpoint "$ENDPOINT" \
  --run-id crash-b \
  --duration-secs 5 \
  --output /artifacts/crash-b.json

echo "== validate manifests =="
"${COMPOSE[@]}" run --rm loadgen validate \
  --db-path /data/otel-logs.db \
  --manifest /artifacts/crash-a.json
"${COMPOSE[@]}" run --rm loadgen validate \
  --db-path /data/otel-logs.db \
  --manifest /artifacts/crash-b.json

echo
echo "PASS: container crash durability verified (no acknowledged loss after docker kill + restart)"

"${COMPOSE[@]}" down -v