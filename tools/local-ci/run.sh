#!/usr/bin/env bash
# Local CI with act — runs `.forgejo/workflows/ci.yml` on this machine using
# the Docker daemon, exactly the pipeline the Forgejo runner would run.
#
# Changes into this directory so `.actrc` / `.act.env` are read, then invokes
# `act -C <repo-root> -W <repo>/.forgejo/workflows`.
#
# Usage (WSL / Linux / macOS):
#   ./tools/local-ci/run.sh            # release-gate jobs (containerized, no dind)
#   ./tools/local-ci/run.sh --all      # + docker jobs (needs host Docker daemon)
#   ./tools/local-ci/run.sh --list     # list the workflows act would run
#   ACT=path/to/act ./tools/local-ci/run.sh   # custom act binary
#
# Prerequisites: a running Docker daemon and the `act` binary on PATH
# (install: `brew install act`, `go install github.com/nektos/act@latest`,
# or a release binary from https://github.com/nektos/act/releases).

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
REPO="$(cd ../.. && pwd)"
ACT="${ACT:-act}"

ALL=0
LIST=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        -a|--all) ALL=1; shift ;;
        -l|--list) LIST=1; shift ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

# The release-gate jobs all run in their own `container:` images (rust /
# semgrep) and need no Docker daemon inside the job container.
JOBS=(lint-and-test crash-recovery-release e2e-benchmark cargo-deny cargo-audit semgrep)
SOCKET_ARGS=()

if [[ "$ALL" -eq 1 ]]; then
    JOBS+=(fuzz-smoke crash-e2e-container container-smoke)
    # Mount the host Docker daemon so `docker compose` inside the job talks to
    # the real daemon without docker-in-docker.
    if [[ -S /var/run/docker.sock ]]; then
        SOCKET_ARGS=(--container-daemon-socket /var/run/docker.sock)
    elif [[ -S "$HOME/.docker/run/docker.sock" ]]; then
        SOCKET_ARGS=(--container-daemon-socket "$HOME/.docker/run/docker.sock")
    else
        echo "warning: no Docker daemon socket found; docker jobs may fail" >&2
    fi
fi

WORKFLOWS="$REPO/.forgejo/workflows"

if [[ "$LIST" -eq 1 ]]; then
    exec "$ACT" -C "$REPO" -W "$WORKFLOWS" -l
fi

# act takes a single job id per `-j` flag; build the repeated flags for the
# selected job set.
JOB_ARGS=()
for job in "${JOBS[@]}"; do
    JOB_ARGS+=(-j "$job")
done

exec "$ACT" -C "$REPO" -W "$WORKFLOWS" "${SOCKET_ARGS[@]}" "${JOB_ARGS[@]}"