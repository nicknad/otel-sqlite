# Security policy

## Scope

The collector currently exposes plaintext, unauthenticated OTLP gRPC and
Prometheus HTTP endpoints. Treat it as a trusted-network service unless a
TLS/authenticating proxy is placed in front of it.

Protect the SQLite database, bbolt state files, webhook URLs, and configured
authorization headers. Do not expose the development Compose stack unchanged.

## Supported versions

No formal release/support matrix exists yet. Test reports against the current
main branch and include the commit when reporting.

## Reporting a vulnerability

Do not open a public issue for a security-sensitive report. Contact the
maintainer privately through the Codeberg profile:
<https://codeberg.org/nicknad>.

Include the affected version or commit, impact, reproduction steps, and a
suggested mitigation if available. Please allow time for a fix before public
disclosure.
