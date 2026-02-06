# ICE REST Catalog Test Scenarios

This directory contains scenario-based integration tests for the ICE REST Catalog. Each scenario is self-contained with its own configuration, input data, and execution scripts.

## Directory Structure

```
scenarios/
  <scenario-name>/
    scenario.yaml           # Scenario configuration and metadata
    run.sh.tmpl            # Main test execution script (templated)
    verify.sh.tmpl         # Optional verification script (templated)
    input.parquet          # Input data files (0 or more)
    input2.parquet
    ...
```

## Scenario Configuration (scenario.yaml)

Each scenario must have a `scenario.yaml` file that defines:

```yaml
name: "Human-readable test name"
description: "Description of what this scenario tests"

# Optional: Override catalog configuration
catalogConfig:
  warehouse: "s3://test-bucket/warehouse"

# Environment variables available to scripts
env:
  NAMESPACE_NAME: "test_ns"
  TABLE_NAME: "test_ns.table1"
  INPUT_FILE: "input.parquet"

# Optional: Cloud resources needed (for future provisioning)
cloudResources:
  s3:
    buckets:
      - "test-bucket"
  sqs:
    queues:
      - "test-queue"

# Optional: Test execution phases
phases:
  - name: "setup"
    description: "Initialize resources"
  - name: "run"
    description: "Execute main test logic"
  - name: "verify"
    description: "Verify results"
  - name: "cleanup"
    description: "Clean up resources"
```

## Script Templates

### run.sh.tmpl

The main test execution script. Template variables are replaced at runtime:

- `{{CLI_CONFIG}}` - Path to temporary ICE CLI config file
- `{{MINIO_ENDPOINT}}` - MinIO endpoint URL
- `{{CATALOG_URI}}` - REST catalog URI (e.g., http://localhost:8080)
- `{{SCENARIO_DIR}}` - Absolute path to the scenario directory
- All environment variables from `scenario.yaml` env section

Example:
```bash
#!/bin/bash
set -e

# Environment variables from scenario.yaml are available
echo "Testing namespace: ${NAMESPACE_NAME}"

# Use template variables
ice --config {{CLI_CONFIG}} create-namespace ${NAMESPACE_NAME}

# Reference input files relative to scenario directory
INPUT_PATH="{{SCENARIO_DIR}}/${INPUT_FILE}"
ice --config {{CLI_CONFIG}} insert --create-table ${TABLE_NAME} ${INPUT_PATH}
```

### verify.sh.tmpl (Optional)

Additional verification logic. Same template variables are available.

Example:
```bash
#!/bin/bash
set -e

# Verify expected output files exist
if [ ! -f /tmp/test_output.txt ]; then
  echo "Expected output not found"
  exit 1
fi

echo "Verification passed"
exit 0
```

## Adding New Scenarios

1. Create a new directory under `scenarios/`
2. Add `scenario.yaml` with configuration
3. Add `run.sh.tmpl` with test logic
4. (Optional) Add `verify.sh.tmpl` for additional verification
5. Add any input data files (`.parquet`, etc.)
6. The test framework will automatically discover and run the new scenario

## Running Scenarios

Scenarios are discovered and executed automatically by the `ScenarioBasedIT` test class:

```bash
mvn test -Dtest=ScenarioBasedIT
```

To run a specific scenario:

```bash
mvn test -Dtest=ScenarioBasedIT#testScenario[basic-operations]
```



