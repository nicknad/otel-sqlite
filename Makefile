# OTLP SQLite Collector Makefile

GO := go
GO_SQLITE_TAGS ?= fts5
GO_SQLITE_FLAGS := -tags "$(GO_SQLITE_TAGS)"
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
	$(PROTO_DIR)/opentelemetry/proto/collector/metrics/v1/metrics_service.proto \
	$(PROTO_DIR)/opentelemetry/proto/logs/v1/logs.proto \
	$(PROTO_DIR)/opentelemetry/proto/metrics/v1/metrics.proto \
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
		--go_opt=Mopentelemetry/proto/common/v1/common.proto=codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1 \
		--go_opt=Mopentelemetry/proto/resource/v1/resource.proto=codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1 \
		--go_opt=Mopentelemetry/proto/logs/v1/logs.proto=codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1 \
		--go_opt=Mopentelemetry/proto/metrics/v1/metrics.proto=codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/metrics/v1 \
		--go_opt=Mopentelemetry/proto/collector/logs/v1/logs_service.proto=codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1 \
		--go_opt=Mopentelemetry/proto/collector/metrics/v1/metrics_service.proto=codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/metrics/v1 \
		--go-grpc_out=$(GENERATED_DIR) \
		--go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mopentelemetry/proto/collector/logs/v1/logs_service.proto=codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/logs/v1 \
		--go-grpc_opt=Mopentelemetry/proto/collector/metrics/v1/metrics_service.proto=codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/collector/metrics/v1 \
		-I=$(PROTO_DIR) \
		$(PROTO_FILES)
	@echo "Protobuf code generation complete."

build: generate
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p bin
	CGO_ENABLED=1 $(GO) build $(GO_SQLITE_FLAGS) -o $(BINARY_PATH) -v ./cmd/collector
	@echo "Build complete: $(BINARY_PATH)"

test:
	@echo "Running tests..."
	CGO_ENABLED=1 $(GO) test $(GO_SQLITE_FLAGS) -v -race ./...
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
LOADTEST_DRAIN_TIMEOUT ?= 2m

.PHONY: loadtest-build loadtest-up loadtest-down loadtest-run loadtest \
	loadtest-keepup loadtest-process loadtest-burst-drain

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
		-resources $(LOADTEST_RESOURCES) \
		-drain-timeout $(LOADTEST_DRAIN_TIMEOUT)

# Profile A: capped keep-up (~4.72k rec/s). Proves no backlog at a fixed rate.
loadtest-keepup:
	$(MAKE) loadtest-run \
		LOADTEST_CLIENTS=32 LOADTEST_RECORDS=150 LOADTEST_DURATION=60s \
		LOADTEST_RPS=1 LOADTEST_DRAIN_TIMEOUT=30s

# Profile B: uncapped process ceiling. Report logs_written_total rate.
loadtest-process:
	$(MAKE) loadtest-run \
		LOADTEST_CLIENTS=8 LOADTEST_RECORDS=250 LOADTEST_DURATION=30s \
		LOADTEST_RPS=0 LOADTEST_DRAIN_TIMEOUT=2m

# Profile C: burst intake + drain. High fan-in then wait for written==received.
loadtest-burst-drain:
	$(MAKE) loadtest-run \
		LOADTEST_CLIENTS=32 LOADTEST_RECORDS=1000 LOADTEST_DURATION=30s \
		LOADTEST_RPS=0 LOADTEST_DRAIN_TIMEOUT=5m

# Convenience: up + run + down
loadtest: loadtest-up loadtest-run loadtest-down

# Helper targets
.PHONY: deps install-tools
