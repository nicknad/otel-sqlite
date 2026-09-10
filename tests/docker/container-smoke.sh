#!/usr/bin/env bash
# ------------------------------------------------------------
# Production container smoke (P0-5 release gate).
#
# Builds the production image (Dockerfile, --locked) and asserts the
# operational contract the release image is supposed to ship:
#
#   * the container starts and the built-in gRPC health probe reports
#     SERVING (Docker HEALTHCHECK becomes `healthy`),
#   * the process runs as the unprivileged `otel-sqlite` user (uid 10001),
#   * the /data volume is writable by that user and the SQLite database is
#     created there.
#
# Run from the repository root:
#
#   bash tests/docker/container-smoke.sh
#
# The container and its named volume are removed on exit, so repeated local
# runs are safe. The script's exit code is the verdict.
# ------------------------------------------------------------

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="otel-sqlite-smoke:latest"
NAME="otel-sqlite-smoke-$$"
VOLUME="otel-sqlite-smoke-data-$$"

cd "$REPO_ROOT"

cleanup() {
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker volume rm -f "$VOLUME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> building production image (cargo build --release --locked)"
docker build --file Dockerfile --tag "$IMAGE" .

echo "==> starting container with a fresh named /data volume"
docker run --detach \
    --name "$NAME" \
    --volume "$VOLUME:/data" \
    "$IMAGE"

echo "==> waiting for HEALTHCHECK to report SERVING"
deadline=$((SECONDS + 60))
healthy=""
while (( SECONDS < deadline )); do
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}starting{{end}}' "$NAME" 2>/dev/null || true)"
    if [ "$status" = "healthy" ]; then
        healthy=1
        break
    fi
    sleep 2
done
if [ -z "$healthy" ]; then
    echo "error: container never became healthy" >&2
    docker logs "$NAME" 2>&1 | tail -40 || true
    exit 1
fi
echo "==> healthcheck: SERVING"

echo "==> asserting non-root runtime user"
uid="$(docker exec "$NAME" id -u)"
if [ "$uid" != "10001" ]; then
    echo "error: expected the unprivileged uid 10001, got $uid" >&2
    exit 1
fi
echo "==> runtime user: uid $uid (otel-sqlite)"

echo "==> asserting /data volume is writable and hosts the database"
if ! docker exec "$NAME" test -w /data; then
    echo "error: /data is not writable by the runtime user" >&2
    docker logs "$NAME" 2>&1 | tail -40 || true
    exit 1
fi
db_seen=""
deadline=$((SECONDS + 20))
while (( SECONDS < deadline )); do
    if docker exec "$NAME" test -f /data/otel-logs.db; then
        db_seen=1
        break
    fi
    sleep 1
done
if [ -z "$db_seen" ]; then
    echo "error: /data/otel-logs.db was not created" >&2
    docker logs "$NAME" 2>&1 | tail -40 || true
    exit 1
fi
echo "==> sqlite database created on /data"

echo "==> production container smoke passed"