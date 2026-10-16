# Local CI with `act`

Runs `.github/workflows/ci.yml` locally with
[act](https://github.com/nektos/act) + Docker — release-gate loop without
pushing: format, clippy, tests, deny, audit, semgrep, optionally Docker jobs.

Install: `winget install nektos.act` / `scoop install act`
(Windows), `brew install act` (macOS/Linux), plus a running Docker daemon
(first run pulls `catthehacker` + `rust:1.98-slim` images).

```text
.\tools\local-ci\run.ps1   # Windows
./tools/local-ci/run.sh    # WSL / Linux / macOS
```

| Args | Runs |
|---|---|
| (none) | `lint-and-test`, `crash-recovery-release`, `e2e-benchmark`, `deny`, `audit`, `semgrep` |
| `-All` / `--all` | + Docker jobs (`fuzz-smoke`, `crash-e2e-container`, `container-smoke`; mounts host daemon) |
| `-List` / `--list` | List workflows for `push`, exit |

Subset: call `act` directly from this dir (reads `.actrc`/`.act.env`):
`act -C ../.. -W ../../.github/workflows -j lint-and-test`.
Scripts `cd` here first so `runs-on: ubuntu-latest` maps to
`catthehacker/ubuntu:act-latest` (`linux/amd64`; ARM hosts switch to
`arm64`); `container:` jobs use their own images like real CI.

Limits: containers ≠ runner VMs (pre-commit gate, not release verdict);
`-All` needs the host daemon socket (Windows: `//./pipe/docker_engine`);
first run is slow (image pulls + release build); no secrets needed.
