# Scenario-Based Testing Framework

This document describes the new scenario-based testing framework for ICE REST Catalog integration tests.

## Overview

The scenario-based testing framework provides a scalable, data-driven approach to integration testing. Instead of hardcoding test scenarios in Java test methods, each scenario is defined in its own directory with:

- Configuration files (scenario.yaml)
- Input data (parquet files)
- Executable test scripts (run.sh.tmpl, verify.sh.tmpl)

This approach makes it much easier to:
- Add new test scenarios without writing Java code
- Share test scenarios across teams
- Test complex workflows with multiple steps
- Provision cloud resources dynamically (future feature)

## Directory Structure

```
ice-rest-catalog/src/test/resources/scenarios/
├── README.md                       # Documentation for scenario structure
├── basic-operations/               # Example scenario
│   ├── scenario.yaml              # Scenario configuration
│   ├── run.sh.tmpl                # Main test script (templated)
│   └── verify.sh.tmpl             # Optional verification script
├── insert-scan/
│   ├── scenario.yaml
│   ├── run.sh.tmpl
│   ├── verify.sh.tmpl
│   └── input.parquet              # Test data
└── insert-partitioned/
    ├── scenario.yaml
    ├── run.sh.tmpl
    ├── verify.sh.tmpl
    └── input.parquet
```

## Key Components

### 1. ScenarioConfig.java
Java class that represents the scenario.yaml configuration file. Supports:
- Scenario metadata (name, description)
- Catalog configuration overrides
- Environment variables for scripts
- Cloud resource specifications (for future provisioning)
- Test phases

### 2. ScenarioTestRunner.java
Core test execution engine that:
- Discovers scenario directories
- Loads scenario configurations
- Processes template variables in scripts
- Executes scripts and captures output
- Reports results

### 3. ScenarioBasedIT.java
TestNG parameterized test class that:
- Automatically discovers all scenarios
- Runs each scenario as a separate test case
- Provides test infrastructure (MinIO, REST catalog)
- Injects template variables (CLI config, endpoints, etc.)

### 4. RESTCatalogTestBase.java (Updated)
Base test class that provides:
- MinIO container setup/teardown
- REST catalog server setup/teardown
- Helper methods for scenarios (getMinioEndpoint, getCatalogUri, etc.)

## How It Works

### 1. Test Discovery
When you run `mvn test -Dtest=ScenarioBasedIT`, the test framework:

1. Scans `src/test/resources/scenarios/` for directories
2. Each directory is considered a scenario
3. Creates a parameterized test for each scenario

### 2. Test Execution
For each scenario:

1. **Setup Phase**:
   - Start MinIO container
   - Start ICE REST catalog server
   - Create temporary CLI config file
   - Build template variables

2. **Execution Phase**:
   - Load scenario.yaml configuration
   - Process run.sh.tmpl with template variables
   - Execute the processed script
   - Capture stdout, stderr, and exit code

3. **Verification Phase** (optional):
   - Process verify.sh.tmpl with template variables
   - Execute verification script
   - Capture results

4. **Teardown Phase**:
   - Stop REST catalog server
   - Stop MinIO container
   - Clean up temporary files

### 3. Template Processing
Scripts can use template variables in the format `{{VARIABLE_NAME}}`:

```bash
# Available template variables:
# {{ICE_CLI}} - Path to ice CLI executable
# {{CLI_CONFIG}} - Path to temporary CLI config file
# {{MINIO_ENDPOINT}} - MinIO endpoint URL (e.g., http://localhost:9000)
# {{CATALOG_URI}} - REST catalog URI (e.g., http://localhost:8080)
# {{SCENARIO_DIR}} - Absolute path to scenario directory

# Environment variables from scenario.yaml are also available
{{ICE_CLI}} --config {{CLI_CONFIG}} create-namespace ${NAMESPACE_NAME}
```

## Creating a New Scenario

1. **Create scenario directory**:
   ```bash
   mkdir ice-rest-catalog/src/test/resources/scenarios/my-new-test
   ```

2. **Create scenario.yaml**:
   ```yaml
   name: "My New Test"
   description: "Description of what this test does"
   
   env:
     NAMESPACE_NAME: "my_test_ns"
     TABLE_NAME: "my_test_ns.my_table"
   
   cloudResources:
     s3:
       buckets:
         - "test-bucket"
   ```

3. **Create run.sh.tmpl**:
   ```bash
   #!/bin/bash
   set -e
   
   echo "Running my test..."
   
   # Create namespace
   {{ICE_CLI}} --config {{CLI_CONFIG}} create-namespace ${NAMESPACE_NAME}
   
   # ... more test steps ...
   
   # Cleanup
   {{ICE_CLI}} --config {{CLI_CONFIG}} delete-namespace ${NAMESPACE_NAME}
   
   echo "Test completed successfully"
   ```

4. **Add test data** (optional):
   ```bash
   cp example-data.parquet ice-rest-catalog/src/test/resources/scenarios/my-new-test/input.parquet
   ```

5. **Run the test**:
   ```bash
   mvn test -Dtest=ScenarioBasedIT#testScenario[my-new-test]
   ```

## Running Tests

### Run all scenarios:
```bash
cd ice-rest-catalog
mvn test -Dtest=ScenarioBasedIT
```

### Run a specific scenario:
```bash
mvn test -Dtest=ScenarioBasedIT#testScenario[basic-operations]
```

### Run scenarios matching a pattern:
```bash
mvn test -Dtest=ScenarioBasedIT#testScenario[insert*]
```

## Benefits Over Previous Approach

### Before (Hardcoded Java Tests):
```java
@Test
public void testScenario1() throws Exception {
  File config = createTempCliConfig();
  new CommandLine(Main.class).execute("--config", config.getAbsolutePath(), 
    "create-namespace", "test_ns");
  new CommandLine(Main.class).execute("--config", config.getAbsolutePath(),
    "delete-namespace", "test_ns");
  // ... repeat for each scenario
}

@Test
public void testScenario2() throws Exception {
  // ... duplicate setup code ...
}
```

**Problems**:
- Lots of duplicated code
- Hard to add new scenarios
- Difficult to test complex workflows
- No separation of test data and test logic
- Hard to share tests with non-Java developers

### After (Scenario-Based Tests):
```
scenarios/
  scenario1/
    scenario.yaml
    run.sh.tmpl
  scenario2/
    scenario.yaml
    run.sh.tmpl
    input.parquet
```

**Benefits**:
- ✅ No code duplication
- ✅ Easy to add new scenarios (just add a directory)
- ✅ Can test complex workflows with multiple steps
- ✅ Clear separation of test data and logic
- ✅ Scripts can be shared and run independently
- ✅ Can be used by QA engineers without Java knowledge
- ✅ Future: automatic cloud resource provisioning

## Example Scenarios

### 1. basic-operations
Tests fundamental catalog operations:
- Create namespace
- List namespaces
- Delete namespace

### 2. insert-scan
Tests data insertion and retrieval:
- Create namespace
- Insert data from parquet file
- Scan table
- Verify data contents
- Cleanup

### 3. insert-partitioned
Tests partitioned table creation:
- Create namespace
- Insert data with partition specification
- Verify table was created
- Cleanup

## Future Enhancements

1. **Cloud Resource Provisioning**:
   - Automatically create S3 buckets, SQS queues, etc. based on scenario.yaml
   - Use AWS CloudFormation or Terraform templates
   - Clean up resources after test completion

2. **Parallel Execution**:
   - Run independent scenarios in parallel
   - Speed up test suite execution

3. **Test Data Generation**:
   - Generate synthetic test data based on schema specifications
   - Support for various data formats (Avro, ORC, etc.)

4. **Scenario Composition**:
   - Reuse common scenario steps
   - Create scenario libraries

5. **Performance Testing**:
   - Add performance benchmarks to scenarios
   - Track performance over time

## Migration Guide

To migrate existing hardcoded tests to scenarios:

1. Identify the test scenario (e.g., "insert with partitioning")
2. Create a scenario directory
3. Extract configuration into scenario.yaml
4. Convert Java test code to bash script in run.sh.tmpl
5. Add test data files if needed
6. Delete the old Java test method

Example migration:

**Before** (RESTCatalogInsertIT.java):
```java
@Test
public void testInsertCommand() throws Exception {
  File config = createTempCliConfig();
  String namespace = "test_insert";
  new CommandLine(Main.class).execute("--config", config, 
    "create-namespace", namespace);
  // ... more code ...
}
```

**After** (scenarios/insert-scan/):
```
scenario.yaml       # Configuration
run.sh.tmpl         # Test logic
input.parquet       # Test data
```

## Troubleshooting

### Scenario not discovered
- Ensure scenario directory is in `src/test/resources/scenarios/`
- Check that `scenario.yaml` exists in the directory
- Directory name should not start with `.`

### Script execution fails
- Check script has correct shebang (`#!/bin/bash`)
- Verify template variables are correctly specified
- Check file paths are relative to {{SCENARIO_DIR}}
- Review script output in test logs

### ICE CLI not found
- Ensure `ice/target/ice-jar` is built: `mvn -am -pl ice package`
- Or ensure `.bin/local-ice` script exists
- Check that ICE_CLI template variable is correctly set

## Additional Resources

- [Scenarios README](src/test/resources/scenarios/README.md) - Detailed scenario structure documentation
- [ICE CLI Documentation](../ice/README.md) - CLI command reference
- [TestNG Documentation](https://testng.org/doc/) - TestNG testing framework








