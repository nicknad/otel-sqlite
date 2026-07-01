#!/bin/bash
# Aggregate all load test results into a summary CSV.
#
# Usage: ./scripts/aggregate-results.sh [output_file]
#
# If output_file is not provided, writes to loadtest-results/summary.csv

set -euo pipefail

OUTPUT_FILE="${1:-loadtest-results/summary.csv}"

# Create header
cat > "${OUTPUT_FILE}" <<'EOF'
timestamp,test_name,duration_seconds,clients,records_per_request,total_requests,total_records,ingest_rate_rec_s,process_rate_rec_s,errors,error_pct,commands_executed,command_errors
EOF

# Append all CSV files (skip header lines)
for file in loadtest-results/*.csv; do
  if [ "$file" != "${OUTPUT_FILE}" ]; then
    tail -n +2 "$file" >> "${OUTPUT_FILE}"
  fi
done

echo "Aggregated results written to: ${OUTPUT_FILE}"
