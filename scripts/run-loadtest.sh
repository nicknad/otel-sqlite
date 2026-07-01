#!/bin/bash
# Run load test and save results to CSV and JSON files.
#
# Usage: ./scripts/run-loadtest.sh [test_name]
#
# If test_name is not provided, a timestamp-based name is used.

set -euo pipefail

# Configuration
COLLECTOR_ADDR="localhost:4317"
METRICS_URL="http://localhost:9090/metrics"
CLIENTS=8
RECORDS_PER_REQ=200
ATTRS_PER_RECORD=4
RESOURCES=8
DURATION=15s
RPS_PER_CLIENT=3

# Test name (default: timestamp)
TEST_NAME="${1:-$(date +'%Y%m%d-%H%M%S')}"

# Output files
CSV_FILE="loadtest-results/${TEST_NAME}.csv"
JSON_FILE="loadtest-results/${TEST_NAME}.json"
LOG_FILE="loadtest-results/${TEST_NAME}.log"

# Create results directory
mkdir -p loadtest-results

echo "Running load test: ${TEST_NAME}"
echo "Results will be saved to:"
echo "  CSV:  ${CSV_FILE}"
echo "  JSON: ${JSON_FILE}"
echo "  Log:  ${LOG_FILE}"
echo ""

# Run load test and capture output
cd /home/nnadolski/projects/otel-sqlite
go run ./cmd/loadtest/ \
  -addr "${COLLECTOR_ADDR}" \
  -metrics "${METRICS_URL}" \
  -clients "${CLIENTS}" \
  -records "${RECORDS_PER_REQ}" \
  -attrs "${ATTRS_PER_RECORD}" \
  -resources "${RESOURCES}" \
  -duration "${DURATION}" \
  -rps-per-client "${RPS_PER_CLIENT}" \
  2>&1 | tee "${LOG_FILE}"

# Parse results from log file
echo "Parsing results..."

# Extract key metrics using grep and awk
INGEST_RATE=$(grep "Ingest rate (records):" "${LOG_FILE}" | awk '{print $4}')
PROCESS_RATE=$(grep "Confirmed written (metrics):" "${LOG_FILE}" | awk '{print $4}')
ERRORS=$(grep "Export errors:" "${LOG_FILE}" | awk '{print $3}' | sed 's/(.*//')
ERROR_PCT=$(grep "Export errors:" "${LOG_FILE}" | awk '{print $4}')
DURATION=$(grep "Duration (client active):" "${LOG_FILE}" | awk '{print $4}')
CLIENTS=$(grep "Mock API clients:" "${LOG_FILE}" | awk '{print $4}')
RECORDS_PER_REQ=$(grep "Records per request:" "${LOG_FILE}" | awk '{print $4}')
TOTAL_REQ=$(grep "Export requests sent:" "${LOG_FILE}" | awk '{print $4}')
TOTAL_REC=$(grep "Records sent:" "${LOG_FILE}" | awk '{print $3}')

# Get command metrics from Prometheus
COMMANDS_EXECUTED=$(curl -s "${METRICS_URL}" | grep "otel_collector_command_executed_total" | awk '{print $2}')
COMMAND_ERRORS=$(curl -s "${METRICS_URL}" | grep "otel_collector_command_failures_total" | awk '{print $2}')

# Create CSV file
echo "Writing CSV file..."
cat > "${CSV_FILE}" <<EOF
timestamp,test_name,duration_seconds,clients,records_per_request,total_requests,total_records,ingest_rate_rec_s,process_rate_rec_s,errors,error_pct,commands_executed,command_errors
$(date +'%Y-%m-%dT%H:%M:%S%z'),${TEST_NAME},${DURATION},${CLIENTS},${RECORDS_PER_REQ},${TOTAL_REQ},${TOTAL_REC},${INGEST_RATE},${PROCESS_RATE},${ERRORS},${ERROR_PCT},${COMMANDS_EXECUTED},${COMMAND_ERRORS}
EOF

# Create JSON file
echo "Writing JSON file..."
cat > "${JSON_FILE}" <<EOF
{
  "timestamp": "$(date +'%Y-%m-%dT%H:%M:%S%z')",
  "test_name": "${TEST_NAME}",
  "configuration": {
    "collector_address": "${COLLECTOR_ADDR}",
    "metrics_url": "${METRICS_URL}",
    "clients": ${CLIENTS},
    "records_per_request": ${RECORDS_PER_REQ},
    "attributes_per_record": ${ATTRS_PER_RECORD},
    "resources": ${RESOURCES},
    "duration": "${DURATION}",
    "rps_per_client": ${RPS_PER_CLIENT}
  },
  "results": {
    "duration_seconds": "${DURATION}",
    "total_requests": ${TOTAL_REQ},
    "total_records": ${TOTAL_REC},
    "ingest_rate_rec_s": ${INGEST_RATE},
    "process_rate_rec_s": ${PROCESS_RATE},
    "errors": ${ERRORS},
    "error_percentage": "${ERROR_PCT}"
  },
  "command_metrics": {
    "commands_executed": ${COMMANDS_EXECUTED},
    "command_errors": ${COMMAND_ERRORS}
  },
  "notes": "Load test run after refactoring to command-based architecture"
}
EOF

echo ""
echo "Load test results saved:"
echo "  CSV:  ${CSV_FILE}"
echo "  JSON: ${JSON_FILE}"
echo "  Log:  ${LOG_FILE}"
echo ""
echo "Done."
