#!/usr/bin/env bash
# ------------------------------------------------------------
# Chunked long-run fuzz soak.
#
#   bash tests/docker/fuzz-soak.sh <target> [total_secs] [chunk_secs]
#
# Examples:
#   bash tests/docker/fuzz-soak.sh core_batcher            # 30 min in 5 min chunks
#   bash tests/docker/fuzz-soak.sh logs_ingress_export 600 120
#
# Why chunked: ASan keeps freed memory (quarantine + size-class arenas) and
# does not return it to the OS promptly, so a single long `core_batcher`
# process grows RSS ~2 MB/s until libFuzzer's `-rss_limit_mb` trips — see the
# memory note in tests/docker/compose.fuzz.yml. Restarting the target every
# `chunk_secs` resets the allocator's high-water mark, so the cap can never be
# reached regardless of how the nightly toolchain drifts. libFuzzer writes
# newly discovered units into the bind-mounted fuzz/corpus/<target>/, so
# coverage accumulates across chunks and the total fuzz time is unchanged.
#
# `ASAN_OPTIONS` (set on the compose services) already bounds retention; this
# is the belt-and-braces guarantee for the scheduled run. The process is the
# unit that restarts — the container is recreated by `docker compose run`.
# ------------------------------------------------------------

set -euo pipefail

TARGET="${1:?usage: fuzz-soak.sh <target> [total_secs] [chunk_secs]}"
TOTAL_SECS="${2:-1800}"
CHUNK_SECS="${3:-300}"
RSS_LIMIT_MB="${RSS_LIMIT_MB:-2048}"

for value in "TOTAL_SECS=${TOTAL_SECS}" "CHUNK_SECS=${CHUNK_SECS}" "RSS_LIMIT_MB=${RSS_LIMIT_MB}"; do
    name="${value%%=*}"
    number="${value##*=}"
    if ! [[ "${number}" =~ ^[0-9]+$ ]] || (( number < 1 )); then
        echo "error: ${name} must be a positive integer (got '${number}')" >&2
        exit 2
    fi
done

# Each target maps to the compose service that carries its resource envelope
# (both are identical today; the name keeps logs in the logs service).
case "${TARGET}" in
    logs_ingress_export) SERVICE="smoke-logs" ;;
    core_batcher)        SERVICE="smoke-batcher" ;;
    *)
        echo "error: unknown fuzz target '${TARGET}' (expected logs_ingress_export or core_batcher)" >&2
        exit 2
        ;;
esac

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "==> soak ${TARGET}: ${TOTAL_SECS}s total in ${CHUNK_SECS}s chunks (rss cap ${RSS_LIMIT_MB} MB)"

remaining="${TOTAL_SECS}"
chunk=0
while (( remaining > 0 )); do
    chunk=$(( chunk + 1 ))
    run_secs="${CHUNK_SECS}"
    (( run_secs > remaining )) && run_secs="${remaining}"
    remaining=$(( remaining - run_secs ))

    echo "==> chunk ${chunk}: -max_total_time=${run_secs} (${remaining}s remaining after this chunk)"
    docker compose -f "${REPO_ROOT}/tests/docker/compose.fuzz.yml" run --rm "${SERVICE}" \
        run "${TARGET}" -- -rss_limit_mb="${RSS_LIMIT_MB}" -max_total_time="${run_secs}"
done

echo "==> soak ${TARGET} complete: ${chunk} chunk(s), ${TOTAL_SECS}s of fuzzing"
