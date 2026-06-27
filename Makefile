# OTLP SQLite Collector Makefile

GO := go
GOPATH := $(shell $(GO) env GOPATH)
BINARY_NAME := otel-collector
BINARY_PATH := bin/$(BINARY_NAME)

# Protobuf generation
PROTOC := protoc
PROTOC_GEN_GO := protoc-gen-go
PROTOC_GEN_GO_GRPC := protoc-gen-go-grpc
PROTO_DIR := api
GENERATED_DIR := internal/generated

# Source files
PROTO_FILES := \
	$(PROTO_DIR)/opentelemetry/proto/collector/logs/v1/logs_service.proto \
	$(PROTO_DIR)/opentelemetry/proto/logs/v1/logs.proto \
	$(PROTO_DIR)/opentelemetry/proto/common/v1/common.proto \
	$(PROTO_DIR)/opentelemetry/proto/resource/v1/resource.proto

.PHONY: all generate build test lint fmt run clean

all: generate build

generate: $(PROTO_FILES)
	@echo "Generating protobuf and gRPC code..."
	@mkdir -p $(GENERATED_DIR)
	$(PROTOC) \
		--go_out=$(GENERATED_DIR) \
		--go_opt=paths=source_relative \
		--go_opt=Mopentelemetry/proto/common/v1/common.proto=github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/common/v1 \
		--go_opt=Mopentelemetry/proto/resource/v1/resource.proto=github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1 \
		--go_opt=Mopentelemetry/proto/logs/v1/logs.proto=github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1 \
		--go_opt=Mopentelemetry/proto/collector/logs/v1/logs_service.proto=github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1 \
		--go-grpc_out=$(GENERATED_DIR) \
		--go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mopentelemetry/proto/collector/logs/v1/logs_service.proto=github.com/nnadolski/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1 \
		-I=$(PROTO_DIR) \
		$(PROTO_FILES)
	@echo "Protobuf code generation complete."

build: generate
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p bin
	$(GO) build -o $(BINARY_PATH) -v ./cmd/collector
	@echo "Build complete: $(BINARY_PATH)"

test:
	@echo "Running tests..."
	$(GO) test -v -race ./...
	@echo "Tests complete."

lint:
	@echo "Running linter..."
	golangci-lint run ./...
	@echo "Linting complete."

fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...
	@echo "Formatting complete."

run: build
	@echo "Starting $(BINARY_NAME)..."
	$(BINARY_PATH)

clean:
	@echo "Cleaning..."
	rm -rf $(BINARY_PATH)
	rm -rf $(GENERATED_DIR)
	@echo "Clean complete."

# Dependency management
deps:
	@echo "Installing dependencies..."
	$(GO) mod tidy
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "Dependencies installed."

# Tool installation
install-tools:
	@echo "Installing development tools..."
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	$(GO) install github.com/google/addlicense@latest
	@echo "Tools installed."

# ===========================================================================
# Load testing
# ===========================================================================
# Loads the containerized collector (docker-compose.loadtest.yml) and runs the
# load-test client (cmd/loadtest) against it to establish an ingest/process
# throughput baseline. Override LOADTEST_* vars to change the shape of load.

LOADTEST_COMPOSE ?= docker-compose.loadtest.yml
LOADTEST_ADDR    ?= localhost:14317
LOADTEST_METRICS ?= http://localhost:19090/metrics
LOADTEST_CLIENTS ?= 32
LOADTEST_RECORDS ?= 1000
LOADTEST_DURATION ?= 30s
LOADTEST_RPS      ?= 0
LOADTEST_ATTRS   ?= 4
LOADTEST_RESOURCES ?= 8

.PHONY: loadtest-build loadtest-up loadtest-down loadtest-run loadtest

loadtest-build:
	@echo "Building collector image..."
	docker compose -f $(LOADTEST_COMPOSE) build

loadtest-up:
	docker compose -f $(LOADTEST_COMPOSE) up -d --build
	@echo "Waiting for collector health..."
	@for i in $$(seq 1 30); do \
		if curl -sf http://localhost:19090/health >/dev/null 2>&1; then \
			echo "collector is healthy"; exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "collector did not become healthy"; exit 1

loadtest-down:
	docker compose -f $(LOADTEST_COMPOSE) down -v

loadtest-run:
	$(GO) run ./cmd/loadtest \
		-addr $(LOADTEST_ADDR) \
		-metrics $(LOADTEST_METRICS) \
		-clients $(LOADTEST_CLIENTS) \
		-records $(LOADTEST_RECORDS) \
		-duration $(LOADTEST_DURATION) \
		-rps-per-client $(LOADTEST_RPS) \
		-attrs $(LOADTEST_ATTRS) \
		-resources $(LOADTEST_RESOURCES)

# Convenience: up + run + down
loadtest: loadtest-up loadtest-run loadtest-down

# Helper targets
.PHONY: deps install-tools
