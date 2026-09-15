# Local CI with `act`

Runs the repository's GitHub Actions workflow (`.github/workflows/ci.yml`)
**locally** with [act](https://github.com/nektos/act) — the same GitHub
Actions interpreter the GitHub runner uses, driven through your local Docker
daemon.
This is a release gate loop without pushing to the forge:
validate formatting, clippy, tests, cargo-deny, cargo-audit, semgrep, and (optionally) the
Docker jobs before a commit.

## Install act

| Platform | Command |
|---|---|
| Windows | `winget install nektos.act` (or `scoop install act`) |
| macOS / Linux | `brew install act` |
| Any (Go) | `go install github.com/nektos/act@latest` |

You also need a **running Docker daemon** (Docker Desktop / dockerd). act pulls
the runner images on first use (the `catthehacker` image and
`rust:1.98-slim-bookworm` are large; the first run downloads them).

## Usage

```text
.\tools\local-ci\run.ps1              # Windows PowerShell
./tools/local-ci/run.sh               # WSL / Linux / macOS
```

| Command | What it runs |
|---|---|
| `run.ps1` / `run.sh` | Release-gate jobs: `lint-and-test`, `crash-recovery-release`, `e2e-benchmark`, `cargo-deny`, `cargo-audit`, `semgrep` — all in their own `container:` images. |
| `... -All` / `--all` | Adds the Docker jobs: `fuzz-smoke`, `crash-e2e-container`, `container-smoke`. Mounts the host Docker daemon so `docker compose` inside the job works without docker-in-docker. |
| `... -List` / `--list` | Lists the workflows act sees for the `push` event, then exits. |
| `... -Act <path>` / `ACT=<path>` | Use a specific act binary. |

Targeted runs: the wrapper scripts take only `-All`/`--all`, `-List`/`--list`
and an act binary override; they do **not** forward extra flags. To run a
subset (or pass additional act flags), call act directly from this directory
so `.actrc`/`.act.env` are read, and select the job with `-j`:

```bash
cd tools/local-ci
act -C ../.. -W "$PWD/../../.github/workflows" -j lint-and-test
# extra act flags go before the job selector, e.g.:
act -C ../.. -W "$PWD/../../.github/workflows" -v -j lint-and-test
```

## What this configuration does

- `.actrc` — read from this folder when act runs here: maps the `runs-on:
  ubuntu-latest` (host) jobs to the `catthehacker/ubuntu:act-latest` image,
  pins `--container-architecture linux/amd64`, and loads `.act.env`.
  The `container:` jobs (rust / semgrep) ignore the mapping and use their own
  images, matching real CI.
- `.act.env` — safe environment defaults (`RUST_LOG`, `CARGO_TERM_COLOR`).
  Copy `.act.env.example` over it for overrides.
- `run.ps1` / `run.sh` — `cd` into this folder (so `.actrc` applies), then
  `act -C <repo> -W <repo>/.github/workflows <jobs>`.

## Limitations

- **Not a full CI substitute.** act runs in containers, not the runner's
  virtual machines; subtle environment differences are possible. It is a fast
  local pre-commit gate, not the release verdict.
- **Docker jobs (`-All`)** need the host Docker daemon socket mounted and a
  running daemon. On Windows that is the Docker Desktop named pipe
  (`//./pipe/docker_engine`); the scripts pass the right socket for the host.
- **First run is slow** (image pulls + a full release build of the workspace).
- **Architecture**: `.actrc` pins `linux/amd64`; Apple Silicon / ARM hosts
  should switch that to `linux/arm64`.
- **Secrets**: this workflow uses none; `actions/checkout@v5` works locally
  against the checked-out tree.