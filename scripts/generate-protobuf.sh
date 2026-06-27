#!/bin/bash
# Script to generate protobuf and gRPC code for OTLP

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$PROJECT_ROOT/api"
GENERATED_DIR="$PROJECT_ROOT/internal/generated"

# Ensure generated directory exists
mkdir -p "$GENERATED_DIR"

# Proto files to generate
PROTO_FILES=(
    "$PROTO_DIR/opentelemetry/proto/collector/logs/v1/logs_service.proto"
    "$PROTO_DIR/opentelemetry/proto/logs/v1/logs.proto"
    "$PROTO_DIR/opentelemetry/proto/common/v1/common.proto"
    "$PROTO_DIR/opentelemetry/proto/resource/v1/resource.proto"
)

# Go package overrides (M options) so generated code imports local paths
GO_M_OPTS=(
    --go_opt=Mopentelemetry/proto/common/v1/common.proto=github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/common/v1
    --go_opt=Mopentelemetry/proto/resource/v1/resource.proto=github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1
    --go_opt=Mopentelemetry/proto/logs/v1/logs.proto=github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1
    --go_opt=Mopentelemetry/proto/collector/logs/v1/logs_service.proto=github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1
)

GOGRPC_M_OPTS=(
    --go-grpc_opt=Mopentelemetry/proto/collector/logs/v1/logs_service.proto=github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1
)

echo "Generating protobuf and gRPC code..."

# Generate Go code
protoc \
    --go_out="$GENERATED_DIR" \
    --go_opt=paths=source_relative \
    "${GO_M_OPTS[@]}" \
    --go-grpc_out="$GENERATED_DIR" \
    --go-grpc_opt=paths=source_relative \
    "${GOGRPC_M_OPTS[@]}" \
    -I="$PROTO_DIR" \
    "${PROTO_FILES[@]}"

echo "Protobuf code generation complete."

# Run go mod tidy to update dependencies
cd "$PROJECT_ROOT"
go mod tidy

echo "Dependencies updated."
