#!/bin/bash
set -e

# Systematic writer optimization test
# Tests different WRITER_BATCH_SIZE values while keeping BATCHER_BATCH_SIZE=250

echo "=== Writer Optimization Test Suite ==="
echo "Testing different WRITER_BATCH_SIZE values"
echo "BATCHER_BATCH_SIZE fixed at 250 (matches incoming OTLP batch size)"
echo ""

# Build collector
cd /home/nnadolski/projects/otel-sqlite
go build -o bin/otel-collector ./cmd/collector

# Test parameters
WRITER_SIZES=(10 25 50 100 200 500)
BATCHER_SIZE=250
RESULTS_FILE="/tmp/writer-optimization-results.txt"

# Clear previous results
echo "Writer_Batch_Size,Ingest_Rate,Write_Rate,Error_Rate" > "$RESULTS_FILE"

for writer_size in "${WRITER_SIZES[@]}"; do
    echo ""
    echo "=== Testing WRITER_BATCH_SIZE=$writer_size ==="
    
    # Kill any existing collector
    pkill -9 otel-collector 2>/dev/null || true
    sleep 2
    
    # Clean database
    rm -f otel-logs.db*
    
    # Start collector with test config
    BATCHER_BATCH_SIZE=$BATCHER_SIZE \
    WRITER_BATCH_SIZE=$writer_size \
    nohup ./bin/otel-collector > /tmp/collector-w$writer_size.log 2>&1 &
    
    COLLECTOR_PID=$!
    sleep 3
    
    # Verify collector is running
    if ! kill -0 $COLLECTOR_PID 2>/dev/null; then
        echo "ERROR: Collector failed to start"
        cat /tmp/collector-w$writer_size.log
        continue
    fi
    
    # Run load test (30 seconds)
    echo "Running load test..."
    LOADTEST_OUTPUT=$(go run ./cmd/loadtest \
        -clients 8 \
        -records 250 \
        -duration 30s \
        -metrics http://localhost:9090/metrics 2>&1)
    
    # Extract metrics
    INGEST_RATE=$(echo "$LOADTEST_OUTPUT" | grep "Ingest rate (records/sec):" | awk '{print $4}')
    
    # Get write rate from metrics
    WRITE_RATE=$(curl -s http://localhost:9090/metrics | \
        grep "otel_collector_writer_records_written_total" | \
        awk '{print $2}' | \
        awk -v duration=30 '{printf "%.0f", $1/duration}')
    
    # Get error rate
    ERROR_RATE=$(echo "$LOADTEST_OUTPUT" | grep "Error rate:" | awk '{print $3}')
    
    echo "Results:"
    echo "  Ingest Rate: $INGEST_RATE records/sec"
    echo "  Write Rate: $WRITE_RATE records/sec"
    echo "  Error Rate: $ERROR_RATE"
    
    # Save to results file
    echo "$writer_size,$INGEST_RATE,$WRITE_RATE,$ERROR_RATE" >> "$RESULTS_FILE"
    
    # Stop collector
    kill -9 $COLLECTOR_PID 2>/dev/null || true
    sleep 2
done

echo ""
echo "=== Test Complete ==="
echo "Results saved to: $RESULTS_FILE"
echo ""
cat "$RESULTS_FILE"
