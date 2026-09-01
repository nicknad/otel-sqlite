# ------------------------------------------------------------
# docker build \
#   --build-arg REVISION="$(git rev-parse HEAD)" \
#   --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
#   -t otel-sqlite:0.1.0 .

# docker run --rm \
#     -v otel-sqlite-data:/data \
#     -p 4317:4317 \
#     otel-sqlite
# ------------------------------------------------------------

FROM rust:1.89-bookworm AS builder
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
      org.opencontainers.image.description="OpenTelemetry collector sink with SQLite storage" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="https://codeberg.org/nicknad/otel-sqlite" \
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

VOLUME ["/data"]

USER otel-sqlite

ENV RUST_LOG=info

# The database (and its WAL sidecars) must live inside the volume: /data is
# the workdir so the relative default `otel-logs.db` resolves there.
WORKDIR /data

# Built-in gRPC health probe: no curl/bash exists in the slim image, the
# binary speaks grpc.health.v1 for itself. SERVING only while writer and
# batcher run; NOT_SERVING during drain.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/otel-sqlite", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/otel-sqlite"]