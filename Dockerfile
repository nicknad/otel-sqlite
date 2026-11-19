# ------------------------------------------------------------
# Production image for otel-sqlite.
#
# Local build:
#   docker build \
#     --build-arg REVISION="$(git rev-parse HEAD)" \
#     --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
#     -t otel-sqlite:local .
#
# Published image (multi-arch manifest: linux/amd64 + linux/arm64):
#   docker run -d --name otel-sqlite \
#     -p 4317:4317 \
#     -v otel-sqlite-data:/data \
#     ghcr.io/nicknad/otel-sqlite:latest
#
# Zero flags work: the image ships a default config at
# /etc/otel-sqlite/otel-sqlite.toml (selected via OTEL_SQLITE_CFG_PATH) that
# binds OTLP on all interfaces with cleartext. Override per setting with
# OTEL_SQLITE_* environment variables (environment wins over the file), or
# mount your own TOML and point OTEL_SQLITE_CFG_PATH at it.
# ------------------------------------------------------------

FROM rust:1.100-bookworm AS builder
WORKDIR /app

# The workspace spans crates/ and tests/: both must exist for cargo to
# resolve the member manifests before any target builds.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tests ./tests

# Build only the application binary.
RUN cargo build \
    --release \
    --locked \
    -p otel-sqlite


# ------------------------------------------------------------
# Runtime image
# ------------------------------------------------------------

FROM debian:bookworm-slim AS runtime

ARG REVISION="unknown"
ARG BUILD_DATE="unknown"

LABEL org.opencontainers.image.title="otel-sqlite" \
      org.opencontainers.image.description="OpenTelemetry OTLP sink with SQLite storage" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="https://github.com/nicknad/otel-sqlite" \
      org.opencontainers.image.licenses="MIT"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd \
    --system \
    --uid 10001 \
    --create-home \
    --shell /usr/sbin/nologin \
    otel-sqlite

COPY --from=builder \
    /app/target/release/otel-sqlite \
    /usr/local/bin/otel-sqlite

RUN mkdir -p /data \
    && chown -R otel-sqlite:otel-sqlite /data

# Container default config: wildcard bind + cleartext so `docker run` needs
# no flags. Root-owned and world-readable (no secrets); per-setting env
# overrides and a mounted config file beat this file.
COPY docker/otel-sqlite.default.toml /etc/otel-sqlite/otel-sqlite.toml

VOLUME ["/data"]

USER otel-sqlite

ENV RUST_LOG=info \
    OTEL_SQLITE_CFG_PATH=/etc/otel-sqlite/otel-sqlite.toml

# The database (and its WAL sidecars) must live inside the volume: /data is
# the workdir so the relative default `otel-logs.db` resolves there.
WORKDIR /data

# Built-in gRPC health probe: no curl/bash exists in the slim image, the
# binary speaks grpc.health.v1 for itself. SERVING only while writer and
# batcher run; NOT_SERVING during drain.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/otel-sqlite", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/otel-sqlite"]