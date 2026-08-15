#!/usr/bin/env bash
# e2e-mixed.sh — end-to-end verification of the otel-sqlite collector with a
# mock application emitting BOTH OTLP logs and OTLP metrics.
#
# Flow:
#   1. build collector, mockapp, metrics-query and the e2e verifier
#   2. start the collector against a fresh temp SQLite database
#      (maintenance disabled so retention cannot interfere)
#   3. run cmd/mockapp for MOCKAPP_DURATION (default 2m): one log export +
#      one metric export per tick, every histogram exemplar trace id
#      matching a log record's trace id in the same request
#   4. settle, then SIGTERM the collector (batchers flush, writer drains)
#   5. scripts/e2e_verify compares stored row counts against the mockapp
#      summary, checks orphan invariants, and proves trace↔metric
#      correlation through the read-side query package
#   6. demo the metrics-query CLI on the resulting database
#
# Exit code 0 = all checks passed.
#
# Env overrides: MOCKAPP_DURATION (default 2m), E2E_LISTEN_ADDR (:14317),
# E2E_METRICS_ADDR (:19090), E2E_GRPC_ADDR (localhost:14317).
set -euo pipefail

DURATION="${MOCKAPP_DURATION:-2m}"
LISTEN_ADDR="${E2E_LISTEN_ADDR:-:14317}"
METRICS_ADDR="${E2E_METRICS_ADDR:-:19090}"
GRPC_ADDR="${E2E_GRPC_ADDR:-localhost:14317}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
COLLECTOR_PID=""

cleanup() {
  if [[ -n "$COLLECTOR_PID" ]] && kill -0 "$COLLECTOR_PID" 2>/dev/null; then
    echo "== cleaning up collector (pid $COLLECTOR_PID) =="
    kill -TERM "$COLLECTOR_PID" 2>/dev/null || true
    wait "$COLLECTOR_PID" 2>/dev/null || true
  fi
  rm -r "$WORK" 2>/dev/null || true
}
trap cleanup EXIT

echo "== e2e-mixed: mock app (logs + metrics) -> otel-sqlite collector -> sqlite =="
echo "   duration=$DURATION grpc=$GRPC_ADDR listen=$LISTEN_ADDR metrics=$METRICS_ADDR"
echo "   work dir: $WORK"

# ---------------------------------------------------------------------------
echo "== building =="
(cd "$ROOT" && GOFLAGS=-buildvcs=false go build -tags fts5 -o "$WORK/collector"     ./cmd/collector)
(cd "$ROOT" && GOFLAGS=-buildvcs=false go build -tags fts5 -o "$WORK/mockapp"       ./cmd/mockapp)
(cd "$ROOT" && GOFLAGS=-buildvcs=false go build -tags fts5 -o "$WORK/metrics-query" ./cmd/metrics-query)
(cd "$ROOT" && GOFLAGS=-buildvcs=false go build -tags fts5 -o "$WORK/e2e_verify"    ./scripts/e2e_verify)

# ---------------------------------------------------------------------------
echo "== starting collector =="
SQLITE_PATH="$WORK/e2e.db" \
LISTEN_ADDRESS="$LISTEN_ADDR" METRICS_ADDRESS="$METRICS_ADDR" \
MAINTENANCE_ENABLED=false \
BATCHER_BATCH_SIZE=500 BATCHER_FLUSH_INTERVAL=1s \
WRITER_BATCH_SIZE=50 WRITER_FLUSH_INTERVAL=1s \
"$WORK/collector" > "$WORK/collector.log" 2>&1 &
COLLECTOR_PID=$!

METRICS_PORT="${METRICS_ADDR##*:}"
ready=""
for _ in $(seq 1 50); do
  if (exec 3<>"/dev/tcp/localhost/$METRICS_PORT") 2>/dev/null; then
    exec 3>&- 3<&-
    ready=1
    break
  fi
  sleep 0.2
done
if [[ -z "$ready" ]]; then
  echo "collector did not become ready; log:" >&2
  tail -20 "$WORK/collector.log" >&2
  exit 1
fi
echo "   collector ready (pid $COLLECTOR_PID, db $WORK/e2e.db)"

# The /metrics listener can come up a moment before the gRPC listener is
# bound; give the server a beat so the mockapp's first export never sees a
# connection refused.
sleep 1

# ---------------------------------------------------------------------------
echo "== running mock application for $DURATION =="
# The mockapp exits non-zero when any export errored; the verifier below is
# the judge (it fails on export_errors > 0), so do not let set -e abort here.
set +e
"$WORK/mockapp" -addr "$GRPC_ADDR" -duration "$DURATION" -summary "$WORK/summary.json"
MOCKAPP_EXIT=$?
set -e
echo "   mockapp finished (exit $MOCKAPP_EXIT); summary at $WORK/summary.json"

# ---------------------------------------------------------------------------
# The batcher only flushes its current batch on shutdown — anything still in
# the ingress channel would be dropped. Settle longer than the batcher flush
# interval (1s) so every acked request is flushed and written before SIGTERM.
echo "== settling 5s so in-flight batches flush =="
sleep 5

echo "== stopping collector =="
kill -TERM "$COLLECTOR_PID"
wait "$COLLECTOR_PID"
COLLECTOR_PID=""
echo "   collector stopped cleanly"

# ---------------------------------------------------------------------------
echo "== verifying storage against mockapp summary =="
"$WORK/e2e_verify" -db "$WORK/e2e.db" -summary "$WORK/summary.json"

# ---------------------------------------------------------------------------
echo "== read-side demo: metrics-query CLI =="
"$WORK/metrics-query" -db "$WORK/e2e.db" -series -limit 12
TRACE="$(sed -n 's/.*"sample_trace_id": "\([0-9a-f]*\)".*/\1/p' "$WORK/summary.json")"
echo "-- exemplar correlation for trace $TRACE --"
"$WORK/metrics-query" -db "$WORK/e2e.db" -trace "$TRACE"
echo "-- histogram buckets (first series) --"
"$WORK/metrics-query" -db "$WORK/e2e.db" -metric mockapp.request.duration -buckets -limit 10

echo
echo "e2e-mixed: ALL CHECKS PASSED (db left at $WORK/e2e.db)"
