# Releasing the image

How a version of `ghcr.io/nicknad/otel-sqlite` gets from this repository to a
verified, signed, multi-arch manifest. The whole flow is CI; the human part
is a commit, a push, and a tag.

## Preflight (local, optional)

Everything below is gated in CI on every push anyway; locally,
`.github/workflows/ci.yml` runs with `act` (see [tools/local-ci/](../../tools/local-ci/)):

- `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace`;
- `container-smoke` on both architectures (healthcheck, non-root, /data
  volume against a locally built image).

## First push to main

ci.yml gates the pure-Rust suites, cargo-deny/cargo-audit, semgrep, and the
container smokes on amd64 and arm64. Everything below assumes this is green.

## Cut a release

1. Decide the version. The workspace version in `Cargo.toml` is the source
   of truth; tag `vX.Y.Z` to match it (or bump the workspace version first).
2. `git tag vX.Y.Z && git push origin vX.Y.Z` — or run **Publish** manually:
   the workflow accepts a version input (a leading `v` is tolerated) for
   deliberate reruns and hotfixes.
3. The Publish workflow then runs, in order:
   - **resolve** — validates the X.Y.Z shape and derives the image name from
     the repository owner.
   - **build (per architecture, native runners)** — `--pull` build from the
     freshly resolved base, trivy CVE gate (fixable HIGH|CRITICAL; reviewed
     exceptions in [.trivyignore](../../.trivyignore)), then the per-arch
     push with provenance and SBOM attestations.
   - **merge** — one manifest list tagged `vX.Y.Z`, `X.Y` and `latest`;
     the merged digest is recorded in the job summary.
   - **sign** — cosign keyless via GitHub OIDC on the merged digest.
   - **acceptance (per architecture)** — pulls the *published* image by
     digest and drives the contract end to end: zero-flag wildcard bind,
     real OTLP round-trip, SIGTERM drain, restart survival.
   - **readme-quickstart (per architecture)** — extracts the `sh` block
     from [README.md](../../README.md) and runs it verbatim against the
     pulled image, so the docs cannot drift.

## After the first release (one-time)

- **Visibility:** the first push creates a *private* GHCR package. On
  github.com: Packages → otel-sqlite → Package settings → link this
  repository and set visibility to Public (or keep it private and have
  users authenticate for pulls).
- **Attestations:** check the package page shows Provenance/SBOM. If the
  merged manifest lacks them, the `imagetools create` merge dropped them —
  switch the merge step to `--append` chaining and re-release.
- **Signature:**

```sh
cosign verify \
  --certificate-identity-regexp "https://github.com/nicknad/otel-sqlite" \
  ghcr.io/nicknad/otel-sqlite@sha256:<digest-from-the-release-summary>
```

## CVE gate red? (bookworm-slim drift)

A red trivy gate means the `debian:bookworm-slim` tag drifted behind
Debian's security archive: a fix exists, the image does not carry it yet.
Nothing was published — the gate cannot produce a non-secure image, only a
blocked release.

- Remedy: re-run the workflow (the `--pull` flag always resolves the
  newest tag; base tag rebuilds usually land within days).
- Persistent red: review the findings; if acceptable, add dated, one-line
  review notes to [.trivyignore](../../.trivyignore) and delete them on the
  next base rebuild.
- The weekly **CVE scan** workflow ([scan.yml](../../.github/workflows/scan.yml),
  Mondays 03:00 UTC) builds from a fresh base and runs the same gate without
  publishing, so drift surfaces days before a release attempt.

## Tag policy

`vX.Y.Z` is immutable per release; `X.Y` and `latest` roll. Pin production
deployments by digest — every release summary records it.
