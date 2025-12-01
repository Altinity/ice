#!/bin/bash

# Quick run script with bearer token authentication
# Usage: ./run-with-token.sh YOUR_TOKEN [simulation-name]

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <bearer-token> [simulation-name]"
    echo ""
    echo "Examples:"
    echo "  $0 'your-token-here' CreateTreeDatasetSimulation"
    echo "  $0 'your-token-here' ReadUpdateTreeDatasetSimulation"
    echo "  $0 'your-token-here' ReadOnlyWorkloadSimulation"
    echo ""
    echo "If simulation-name is omitted, runs CreateTreeDatasetSimulation"
    exit 1
fi

BEARER_TOKEN="$1"
SIMULATION="${2:-CreateTreeDatasetSimulation}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== ICE REST Catalog Benchmark with Bearer Token ===${NC}"
echo ""
echo "Simulation: $SIMULATION"
echo "Catalog URL: ${ICE_CATALOG_URL:-http://localhost:8181}"
echo ""

# Export the bearer token
export ICE_BEARER_TOKEN="$BEARER_TOKEN"

# Run the simulation
mvn gatling:test \
  -Dgatling.simulationClass=com.altinity.ice.benchmarks.simulations.${SIMULATION}

echo ""
echo -e "${GREEN}Benchmark complete!${NC}"
echo ""
echo "View the HTML report at:"
LATEST_REPORT=$(find target/gatling -name "index.html" -type f | sort -r | head -n 1)
if [ -n "$LATEST_REPORT" ]; then
    echo "  file://$(pwd)/$LATEST_REPORT"
fi


