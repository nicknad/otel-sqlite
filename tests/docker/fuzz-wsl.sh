#!/usr/bin/env bash
# ------------------------------------------------------------
# Native-WSL fuzzing workflow (faster than the Docker route).
#
# Run from PowerShell, with the repo as current directory:
#
#   wsl -d Debian -e bash tests/docker/fuzz-wsl.sh setup        # one-time toolchain bootstrap
#   wsl -d Debian -e bash tests/docker/fuzz-wsl.sh build        # compile all fuzz targets
#   wsl -d Debian -e bash tests/docker/fuzz-wsl.sh smoke-logs   # 120 s OTLP logs target
#   wsl -d Debian -e bash tests/docker/fuzz-wsl.sh smoke-metrics
#   wsl -d Debian -e bash tests/docker/fuzz-wsl.sh smoke-batcher
#
# Arbitrary runs / libFuzzer flags:
#
#   wsl -d Debian -e bash tests/docker/fuzz-wsl.sh run logs_ingress_export -max_total_time=21600
#   wsl -d Debian -e bash tests/docker/fuzz-wsl.sh run logs_ingress_export fuzz/artifacts/logs_ingress_export/crash-<sha>
#
# Design notes:
# * Resource envelope: TWO CPUs are the default expected budget for fuzzing
#   (matches tests/docker/compose.fuzz.yml, which pins cpuset "0,1"). Verified
#   on a 2-vCPU WSL2 VM with --debug-assertions:
#     core_batcher           ~3.6k exec/s   rss ~412 MB
#     logs_ingress_export    ~1.1k exec/s   rss ~75 MB
#     metrics_ingress_export ~1.3k exec/s   rss ~76 MB
#   Reproduce that verification by putting `processors=2` under [wsl2] in
#   %UserProfile%\.wslconfig, running `wsl --shutdown`, then the three
#   smoke targets; remove the file and shut down again afterwards.
# * Why native WSL: Docker Desktop bind-mounts /mnt/c paths through the
#   9P file server, which dominates cargo's many-small-files I/O. Running
#   cargo-fuzz directly in the distro keeps source reads on /mnt/c but
#   moves the heavy write path out of 9P:
#   CARGO_TARGET_DIR defaults to ~/.cache/otel-sqlite-fuzz/target (ext4).
#   Corpus and crash artifacts stay in fuzz/corpus and fuzz/artifacts in
#   the repository so they persist on the Windows side for committing.
# * Toolchain is installed per-user (rustup in $HOME): no sudo required.
#   libFuzzer's C++ runtime is compiled with clang++, which Debian ships;
#   g++/build-essential are NOT needed.
# * The exact same targets run in tests/docker/compose.fuzz.yml; prefer that
#   for CI-parity or when reproducing an environment from scratch. Use
#   this script for day-to-day speed.
# ------------------------------------------------------------

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FUZZ_DIR="${REPO_ROOT}/fuzz"

if [[ -z "${WSL_DISTRO_NAME:-}" ]] && ! grep -qi microsoft /proc/version 2>/dev/null; then
    echo "error: this script targets a WSL2 distro (got: $(uname -a))" >&2
    exit 1
fi

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/otel-sqlite-fuzz/target}"
# libfuzzer-sys builds its C++ runtime via the cc crate; clang++ avoids
# needing build-essential installed.
export CXX="${CXX:-clang++}"
export CXXFLAGS="${CXXFLAGS:--std=c++17}"

# wsl.exe starts non-login shells without ~/.cargo/bin on PATH; source the
# rustup environment up front so tool detection reflects reality.
[[ -f "$HOME/.cargo/env" ]] && . "$HOME/.cargo/env"
# Scope sancov builds to nightly WITHOUT changing the distro-wide default
# (re-running rustup-init would otherwise flip it back to stable).
export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-nightly}"

setup() {
    if ! command -v cargo >/dev/null 2>&1; then
        echo "==> installing rustup (user-local)"
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
            | sh -s -- -y --profile minimal
        # shellcheck disable=SC1091
        . "$HOME/.cargo/env"
    fi

    if ! rustup toolchain list 2>/dev/null | grep -q nightly; then
        echo "==> installing nightly (sancov requires it) + llvm-tools"
        rustup toolchain install nightly \
            --profile minimal \
            --component llvm-tools-preview
    fi

    if ! command -v cargo-fuzz >/dev/null 2>&1; then
        echo "==> installing cargo-fuzz"
        cargo install cargo-fuzz --locked
    fi

    if ! command -v clang++ >/dev/null 2>&1; then
        echo "error: clang++ not found; install clang (sudo apt install clang)" >&2
        exit 1
    fi

    echo "==> toolchain ready: $(cargo --version) / $(cargo fuzz --version)"
}

build_targets() {
    command -v cargo >/dev/null 2>&1 && command -v cargo-fuzz >/dev/null 2>&1 \
        || { echo "==> toolchain incomplete; running setup first"; setup; }
    mkdir -p "$CARGO_TARGET_DIR"
    (cd "$FUZZ_DIR" && cargo fuzz build --debug-assertions)
    echo "==> binaries under $CARGO_TARGET_DIR/x86_64-unknown-linux-gnu/release/"
}

# run_target <target> [libFuzzer args...]  (no leading `--`; inserted here)
run_target() {
    local target="$1"; shift
    (cd "$FUZZ_DIR" && cargo fuzz run "$target" --debug-assertions \
        -- -rss_limit_mb=2048 -timeout=25 "$@")
}

case "${1:-help}" in
    setup)          setup ;;
    build)          build_targets ;;
    smoke-logs)     run_target logs_ingress_export  -max_total_time=120 ;;
    smoke-metrics)  run_target metrics_ingress_export -max_total_time=120 ;;
    smoke-batcher)  run_target core_batcher         -max_total_time=120 ;;
    run)
        [[ $# -ge 2 ]] || { echo "usage: $0 run <target> [libfuzzer args...]" >&2; exit 2; }
        shift; run_target "$@"
        ;;
    help|*)
        sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^#\{0,1\} \?//'
        ;;
esac
