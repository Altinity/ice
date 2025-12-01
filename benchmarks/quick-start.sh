#!/bin/bash

# Quick start script for ICE REST Catalog benchmarks
# Usage: ./quick-start.sh [create|mixed|read-only]

set -e

BENCHMARK_TYPE=${1:-create}

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== ICE REST Catalog Benchmark ===${NC}"
echo ""

# Check if ICE catalog is running
CATALOG_URL=${ICE_CATALOG_URL:-http://localhost:8181}
echo "Checking catalog at $CATALOG_URL..."

if curl -s -f "$CATALOG_URL/v1/config" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Catalog is accessible${NC}"
else
    echo -e "${YELLOW}⚠ Warning: Cannot reach catalog at $CATALOG_URL${NC}"
    echo "Make sure ICE REST Catalog is running."
    echo ""
fi

echo ""
echo "Running benchmark type: $BENCHMARK_TYPE"
echo ""

case $BENCHMARK_TYPE in
  create|write)
    echo "100% Write Workload - Creating dataset..."
    ./gradlew gatlingRun-com.altinity.ice.benchmarks.simulations.CreateTreeDatasetSimulation
    ;;
  mixed|read-write)
    echo "90% Read / 10% Write Workload..."
    ./gradlew gatlingRun-com.altinity.ice.benchmarks.simulations.ReadUpdateTreeDatasetSimulation
    ;;
  read|read-only)
    echo "100% Read Workload..."
    ./gradlew gatlingRun-com.altinity.ice.benchmarks.simulations.ReadOnlyWorkloadSimulation
    ;;
  *)
    echo "Unknown benchmark type: $BENCHMARK_TYPE"
    echo "Usage: $0 [create|mixed|read-only]"
    exit 1
    ;;
esac

echo ""
echo -e "${GREEN}Benchmark complete!${NC}"
echo ""
echo "Open the HTML report to view detailed results:"
LATEST_REPORT=$(find build/reports/gatling -name "index.html" -type f | sort -r | head -n 1)
if [ -n "$LATEST_REPORT" ]; then
    echo "  file://$(pwd)/$LATEST_REPORT"
fi


