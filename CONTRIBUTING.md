# Contributing

Open an Issue for discussion. Undiscussed PRs are not welcomed.

## Quality gates

Must pass before merge (CI enforces):

```sh
cargo clippy --workspace --all-targets   # zero findings; -D warnings in CI
cargo test --workspace
cargo deny check                         # advisories, licenses, bans, sources
cargo audit                              # RustSec (needs cargo-audit >= 0.22.2 for CVSS 4.0)
semgrep scan --config .semgrep.yml       # zero blocking findings
```

Pre-commit (lefthook, `lefthook.yml`): `cargo fmt --check` + `semgrep --error`
on staged files. Setup: `lefthook install`,
`uv tool install semgrep==1.176.0`. Hooks may skip if semgrep unavailable;
CI remains the gate (`git commit --no-verify` bypasses hooks only).

Lint policy: root `Cargo.toml` `[workspace.lints]`, inherited per crate.
`unsafe` denied (one justified exception in `ingress/build.rs`);
`.unwrap()` tests-only (prod uses handled errors / `.expect("reason")`);
`dbg!`/`todo!`/`unimplemented!` fail. Pedantic on with inline-justified
allows. `deny.toml`: vuln/yanked fail, permissive license allow-list.
Suppress semgrep inline only with `// nosemgrep: <rule-id>` + reason
(`.semgrepignore` excludes generated/artifacts).

Toolchain: rustup/Cargo, lefthook, `uv`-installed semgrep, cargo-deny,
cargo-audit ≥0.22.2, Docker Compose (container + constrained perf tests).

## Testing expectations

Features need tests beyond "RPC returned OK" — verify contents, counters,
health, latency, recovery:

| Area | Required evidence |
|---|---|
| Unit | mapping, ledger, queue, scheduler, validation, error classes |
| Storage | real SQLite txns, migrations, indexes, FTS, retention, salvage |
| Pipeline | batcher/writer/ledger wiring, barriers, ordering, backpressure, shutdown |
| Protocol | real tonic client: TLS/mTLS, auth, gzip, limits, health |
| External e2e | binary + TCP + real DB: restart, crash, validation |
| Conformance | Collector/SDK exporter with prod encoding/compression |
| Fuzz/property | decode/mapping never panics; canonical/identity invariants |
| Load/soak | throughput, latency, queues, CPU/RSS, WAL, errors over time |
| Packaging | clean Docker build, non-root, healthcheck, volume, signals |
| Operations | backup/restore, migration, disk-full, read-only, rotation |
| Security | advisories/licenses, SAST, auth failures, secret handling |
