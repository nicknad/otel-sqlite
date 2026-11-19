#!/usr/bin/env bash
# Runs the README quickstart block VERBATIM, so the docs cannot drift from the
# image contract: whatever a stranger copy-pastes from the README must work.
#
#   OTEL_SQLITE_IMAGE=ghcr.io/nicknad/otel-sqlite@sha256:... \
#     bash tests/docker/readme-quickstart.sh
#
# The pulled image is first tagged as whatever image ref the README block
# names, then the block itself is extracted from README.md and executed
# unmodified. The script's own assertions come after the block.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [ -z "${OTEL_SQLITE_IMAGE:-}" ]; then
    echo "error: set OTEL_SQLITE_IMAGE to the published image ref" >&2
    exit 2
fi

cd "$REPO_ROOT"

QUICKSTART="$(mktemp)"
awk '
    /^## Quickstart/ { section = 1; next }
    !section { next }
    /^```sh$/ { inblock = 1; next }
    inblock && /^```$/ { exit }
    inblock { print }
' README.md > "$QUICKSTART"

if [ ! -s "$QUICKSTART" ]; then
    echo "error: no fenced sh block found under '## Quickstart' in README.md" >&2
    rm -f "$QUICKSTART"
    exit 1
fi

# Present the image under the name the README block uses.
README_REF="$(grep -oE 'ghcr\.io/[a-z0-9._/-]+:[a-zA-Z0-9._-]+' "$QUICKSTART" | head -1)"
if [ -z "$README_REF" ]; then
    echo "error: the quickstart block does not name a ghcr.io image ref" >&2
    rm -f "$QUICKSTART"
    exit 1
fi
docker pull "$OTEL_SQLITE_IMAGE"
docker tag "$OTEL_SQLITE_IMAGE" "$README_REF"

echo "== running the README quickstart verbatim =="
bash "$QUICKSTART"
rm -f "$QUICKSTART"

echo "== asserting the container became healthy on a wildcard bind =="
deadline=$((SECONDS + 60))
healthy=""
while (( SECONDS < deadline )); do
    status="$(docker inspect --format '{{.State.Health.Status}}' otel-sqlite 2>/dev/null || true)"
    if [ "$status" = "healthy" ]; then
        healthy=1
        break
    fi
    sleep 2
done
if [ -z "$healthy" ]; then
    echo "error: quickstart container never became healthy" >&2
    docker logs otel-sqlite 2>&1 | tail -30 || true
    exit 1
fi
if ! docker logs otel-sqlite 2>&1 | grep -q "ingress listening on :4317"; then
    echo "error: sink did not log the wildcard listen address" >&2
    docker logs otel-sqlite 2>&1 | tail -30 || true
    exit 1
fi

docker rm -f otel-sqlite >/dev/null 2>&1 || true
echo
echo "PASS: the README quickstart ran verbatim against the published image"
