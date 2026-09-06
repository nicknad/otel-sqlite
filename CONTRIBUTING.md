# Contributing

Open Issue for discussion.  
Undiscussed PR are not welcomed.

## Quality gates

All of these must pass before a PR is merged (CI enforces the same):

```sh
cargo clippy --workspace --all-targets   # zero findings; RUSTFLAGS=-D warnings in CI
cargo test --workspace                   # all suites green
cargo deny check                         # advisories, licenses, bans, sources
cargo audit                              # RustSec advisories (requires cargo-audit >= 0.22.2)
semgrep scan --config .semgrep.yml       # zero blocking findings
```

### Local git hooks (lefthook)

The two sub-second gates also run on every commit via
[lefthook](https://lefthook.dev) (`lefthook.yml`):

- `cargo fmt --all -- --check`
- `semgrep --error` on staged files

Setup is one command per clone:

```sh
lefthook install   # install lefthook itself via winget/scoop/brew/pipx/go
uv tool install semgrep==1.175.0
```

Semgrep can be installed natively with `uv` on supported platforms. If it is
unavailable, the hook skips with a note instead of failing, and CI enforces it
regardless. The slow gates
(`cargo test --workspace`, `cargo deny check`) intentionally stay CI-only so
the hooks never get slow enough to disable. Hooks can be bypassed with
`git commit --no-verify`; CI remains the gate that counts.

### Clippy

Lint policy lives once in the root `Cargo.toml` under `[workspace.lints]`
and is inherited by every crate via `[lints] workspace = true`.

- `unsafe` is denied; the single exception (`crates/otel-sqlite-ingress/build.rs`)
  carries a `// SAFETY:` justification and an explicit allow.
- `.unwrap()` is reserved for tests: unit tests inherit an allowance from each
  crate root (`#![cfg_attr(test, ...)]`), integration tests carry a file-level
  `#![allow(clippy::unwrap_used)]`. Production code must handle errors or use
  `.expect("reason")`.
- `dbg!`, `todo!`, `unimplemented!` are hard failures.
- Pedantic lints are on; the handful of documented allowances (casts,
  doc-formatting nits, ...) are justified inline in the root manifest.

### cargo-deny

Configured in `deny.toml`: vulnerable/yanked dependencies fail, licenses are
restricted to a permissive allow-list, duplicate dependencies are surfaced as
warnings. Add new license IDs to the allow-list deliberately.

### cargo-audit

`cargo audit` scans `Cargo.lock` against the RustSec advisory database as a
second opinion next to `cargo deny check advisories`. It requires
`cargo-audit >= 0.22.2`: older releases (e.g. 0.21.x) cannot parse CVSS 4.0
vectors now present in the advisory database and abort with a TOML parse
error instead of scanning. Install/upgrade with
`cargo install cargo-audit --locked --version 0.22.2`, then run `cargo audit`
(a clean tree reports zero vulnerabilities).

### Semgrep

Rules live in `.semgrep.yml`; generated code and build artifacts are excluded
via `.semgrepignore`. Justified findings are suppressed inline with
`// nosemgrep: <rule-id>` plus a reason.

Install the local CLI with `uv tool install semgrep==1.175.0`, then run
`semgrep scan --config .semgrep.yml`. WSL or Docker remain alternatives:
`docker run --rm -v "${PWD}:/src" semgrep/semgrep semgrep --config .semgrep.yml /src`.
You can also push and let CI run it (`.forgejo/workflows/ci.yml` on Codeberg / Forgejo
Actions; the repo needs Actions enabled under Settings -> Repository units,
and `runs-on:` must match a label advertised by your runner).

### Toolchain

The local and CI quality toolchain is:

- `rustup` and Cargo for formatting, compilation, tests, and Clippy.
- `lefthook` for fast pre-commit formatting and Semgrep checks.
- `uv` for installing the Semgrep CLI without modifying the project Python dependencies.
- `cargo-deny` for dependency advisories, licenses, bans, and sources.
- `cargo-audit >= 0.22.2` for RustSec advisory scans (older releases fail on
  CVSS 4.0 entries).
- Docker Compose for container, stack, and constrained performance tests.

## Testing expectations

Every production feature must include tests at the appropriate level. A test
that only proves an RPC returns successfully is insufficient for a durable
ingestion system; it must also verify persisted contents, counters, health,
latency bounds, and recovery behavior.

| Area                  | Required evidence                                                                         |
| --------------------- | ----------------------------------------------------------------------------------------- |
| Unit behavior         | Pure mapping, ledger, queue, scheduler, validation, and error-classification tests        |
| Storage integration   | Real SQLite transactions, migrations, indexes, FTS, retention, salvage, and recovery      |
| Pipeline integration  | Real batcher/writer/ledger wiring, barriers, ordering, backpressure, and shutdown         |
| Protocol integration  | Real tonic client, TLS/mTLS, auth, gzip, size limits, and health                          |
| External e2e          | Standalone binary, real TCP, real database, restart, crash, and validation                |
| SDK conformance       | OpenTelemetry Collector or SDK exporter with production encoding/compression defaults     |
| Fuzz/property testing | Protobuf decoding/mapping never panics; canonical encoding and identity invariants hold   |
| Load/soak testing     | Throughput, latency, queues, CPU, RSS, WAL growth, error rates, and degradation over time |
| Packaging             | Clean Docker build, non-root runtime, healthcheck, mounted volume, signals, and restart   |
| Operations            | Backup/restore, migration, disk-full, read-only storage, certificate/token rotation       |
| Security              | Dependency advisories/licenses, static analysis, authentication failures, secret handling |
