# OTLP SQLite Collector Dockerfile

# Build stage
FROM golang:1.25-alpine AS builder

# Install dependencies, including a C compiler required by go-sqlite3.
RUN apk add --no-cache build-base
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Generate protobuf code (pinned versions match go.mod; the latest
# protoc-gen-go-grpc requires Go 1.25+).
RUN apk add --no-cache make protoc protobuf-dev && \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1 && \
    make generate

# Build application
RUN CGO_ENABLED=1 GOOS=linux go build -tags fts5 -o /otel-collector ./cmd/collector

# Runtime stage
FROM alpine:3.19

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates tzdata

# Copy binary
COPY --from=builder /otel-collector /usr/local/bin/otel-collector

# Copy example configuration as a reference
COPY config.example.yaml /etc/otel-collector/config.example.yaml

# Create non-root user
RUN addgroup -S otel && adduser -S -G otel otel

# Create data directory owned by the non-root user so mounted named
# volumes inherit the correct ownership.
RUN mkdir -p /var/lib/otel-collector && chown -R otel:otel /var/lib/otel-collector

# Set working directory
WORKDIR /var/lib/otel-collector

USER otel

# Expose ports: 4317 = gRPC OTLP, 9090 = Prometheus metrics
EXPOSE 4317
EXPOSE 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q --spider http://localhost:9090/health || exit 1

# Entry point
ENTRYPOINT ["/usr/local/bin/otel-collector"]
CMD []
