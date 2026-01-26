# Quick Start: Scenario-Based Testing

## Running Your First Scenario Test

### 1. Build the project (if not already built)
```bash
cd /path/to/ice
mvn clean install -DskipTests
```

### 2. Run all scenario tests
```bash
cd ice-rest-catalog
mvn test -Dtest=ScenarioBasedIT
```

### 3. Run a specific scenario
```bash
mvn test -Dtest=ScenarioBasedIT#testScenario[basic-operations]
```

## Understanding Test Output

### Successful Test
```
[INFO] Running com.altinity.ice.rest.catalog.ScenarioBasedIT
[INFO] ====== Starting scenario test: basic-operations ======
[INFO] [script] Running basic operations test...
[INFO] [script] ✓ Created namespace: test_ns
[INFO] [script] ✓ Listed namespaces
[INFO] [script] ✓ Deleted namespace: test_ns
[INFO] [script] Basic operations test completed successfully
[INFO] Run script exit code: 0
[INFO] ====== Scenario test passed: basic-operations ======
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
```

### Failed Test
```
[ERROR] Scenario 'insert-scan' failed:
[ERROR] Run script failed with exit code: 1
[ERROR] Stdout:
Running insert and scan test...
✓ Created namespace: test_scan
✗ Scan output does not contain expected column 'sepal'
```

## Creating Your First Scenario

### 1. Create the directory structure
```bash
cd ice-rest-catalog/src/test/resources/scenarios
mkdir my-first-test
cd my-first-test
```

### 2. Create scenario.yaml
```yaml
name: "My First Test"
description: "A simple test that creates and deletes a namespace"

env:
  MY_NAMESPACE: "my_test_namespace"

cloudResources:
  s3:
    buckets:
      - "test-bucket"
```

### 3. Create run.sh.tmpl
```bash
#!/bin/bash
set -e

echo "Running my first test..."

# Create namespace
{{ICE_CLI}} --config {{CLI_CONFIG}} create-namespace ${MY_NAMESPACE}
echo "✓ Created namespace: ${MY_NAMESPACE}"

# List namespaces to verify it exists
{{ICE_CLI}} --config {{CLI_CONFIG}} list-namespaces | grep ${MY_NAMESPACE}
echo "✓ Verified namespace exists"

# Delete namespace
{{ICE_CLI}} --config {{CLI_CONFIG}} delete-namespace ${MY_NAMESPACE}
echo "✓ Deleted namespace: ${MY_NAMESPACE}"

echo "Test completed successfully!"
```

### 4. Run your scenario
```bash
cd ../../..  # back to ice-rest-catalog
mvn test -Dtest=ScenarioBasedIT#testScenario[my-first-test]
```

## Template Variables Reference

All scripts have access to these template variables:

| Variable | Example Value | Description |
|----------|---------------|-------------|
| `{{ICE_CLI}}` | `/path/to/ice-jar` | Path to ICE CLI executable |
| `{{CLI_CONFIG}}` | `/tmp/ice-cli-123.yaml` | Path to CLI config file |
| `{{MINIO_ENDPOINT}}` | `http://localhost:9000` | MinIO endpoint URL |
| `{{CATALOG_URI}}` | `http://localhost:8080` | REST catalog URI |
| `{{SCENARIO_DIR}}` | `/path/to/scenarios/my-test` | Absolute path to scenario |

Plus all environment variables from `scenario.yaml` env section.

## Common Patterns

### Pattern 1: Create, Use, Cleanup
```bash
#!/bin/bash
set -e

# Create resources
{{ICE_CLI}} --config {{CLI_CONFIG}} create-namespace ${NAMESPACE}

# Use resources
{{ICE_CLI}} --config {{CLI_CONFIG}} insert --create-table ${TABLE} ${INPUT_FILE}
{{ICE_CLI}} --config {{CLI_CONFIG}} scan ${TABLE}

# Cleanup (always cleanup!)
{{ICE_CLI}} --config {{CLI_CONFIG}} delete-table ${TABLE}
{{ICE_CLI}} --config {{CLI_CONFIG}} delete-namespace ${NAMESPACE}
```

### Pattern 2: Verify Output
```bash
#!/bin/bash
set -e

# Run command and capture output
{{ICE_CLI}} --config {{CLI_CONFIG}} scan ${TABLE} > /tmp/output.txt

# Verify output contains expected data
if ! grep -q "expected_value" /tmp/output.txt; then
  echo "✗ Expected value not found in output"
  cat /tmp/output.txt
  exit 1
fi

echo "✓ Verification passed"
```

### Pattern 3: Use Test Data
```bash
#!/bin/bash
set -e

# Reference files relative to scenario directory
SCENARIO_DIR="{{SCENARIO_DIR}}"
INPUT_FILE="${SCENARIO_DIR}/input.parquet"

# Use the file
{{ICE_CLI}} --config {{CLI_CONFIG}} insert --create-table ${TABLE} ${INPUT_FILE}
```

### Pattern 4: Multi-file Insert
```yaml
# scenario.yaml
env:
  NAMESPACE: "multi_file_test"
  TABLE: "multi_file_test.my_table"
```

```bash
# run.sh.tmpl
#!/bin/bash
set -e

SCENARIO_DIR="{{SCENARIO_DIR}}"

{{ICE_CLI}} --config {{CLI_CONFIG}} create-namespace ${NAMESPACE}

# Insert multiple files
for file in ${SCENARIO_DIR}/input*.parquet; do
  echo "Inserting ${file}..."
  {{ICE_CLI}} --config {{CLI_CONFIG}} insert ${TABLE} ${file}
done

{{ICE_CLI}} --config {{CLI_CONFIG}} delete-table ${TABLE}
{{ICE_CLI}} --config {{CLI_CONFIG}} delete-namespace ${NAMESPACE}
```

## Tips and Best Practices

### ✅ Do This
- Always set `set -e` at the top of scripts (exit on error)
- Always cleanup resources (namespaces, tables)
- Use descriptive scenario names (e.g., `insert-scan-delete`, not `test1`)
- Add echo statements to show progress
- Use `{{SCENARIO_DIR}}` for file paths
- Test your scenario independently before committing

### ❌ Avoid This
- Don't hardcode paths (use template variables)
- Don't leave resources dangling (always cleanup)
- Don't use generic names like "test" or "temp"
- Don't assume resources exist (create them in your scenario)
- Don't share state between scenarios (each should be independent)

## Debugging Failed Scenarios

### 1. Check the test output
The test logs show script stdout and stderr:
```bash
mvn test -Dtest=ScenarioBasedIT#testScenario[my-test] 2>&1 | grep -A 50 "Starting scenario"
```

### 2. Run the script manually
```bash
# Extract the processed script from test logs, or process it manually
cd ice-rest-catalog/src/test/resources/scenarios/my-test
cat run.sh.tmpl | sed 's|{{ICE_CLI}}|../../../../../../.bin/local-ice|g' | bash
```

### 3. Check ICE CLI directly
```bash
# Test ICE CLI works
.bin/local-ice --help

# Test with your config
.bin/local-ice --config /path/to/config.yaml list-namespaces
```

### 4. Enable debug logging
Add to your run.sh.tmpl:
```bash
set -x  # Enable bash debug mode
```

## Next Steps

1. Review existing scenarios in `src/test/resources/scenarios/`
2. Read the [Scenarios README](README.md) for detailed documentation
3. Check [SCENARIO_TESTING.md](../../SCENARIO_TESTING.md) for architecture details
4. Start migrating your hardcoded tests to scenarios!

## Questions?

See the [Scenarios README](README.md) for more detailed documentation.



