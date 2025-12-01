# ICE REST Catalog Benchmarking Framework

A Gatling-based load testing framework for the ICE REST Catalog, inspired by the [Apache Polaris benchmarking framework](https://www.dremio.com/blog/benchmarking-framework-for-the-apache-iceberg-catalog-polaris/).

## Overview

This framework provides reproducible, procedural load tests for the ICE REST Catalog. It supports:

- **100% Write Workload**: Creates hierarchical datasets with namespaces, tables, and views
- **Mixed Read/Write Workload**: Configurable ratio (default 90% read / 10% write)
- **100% Read Workload**: Tests read scalability and caching

### Key Features

- ✅ **Procedural Dataset Generation**: Same configuration = same dataset (reproducible benchmarks)
- ✅ **Configurable Scale**: Test with thousands to millions of entities
- ✅ **Real-world Workloads**: Simulates actual catalog usage patterns
- ✅ **Detailed Reports**: HTML reports with response time percentiles and throughput metrics
- ✅ **Multi-tenant Ready**: Support for multiple catalogs

## Prerequisites

Before running benchmarks:

1. **Deploy ICE REST Catalog**: Ensure your catalog is running and accessible
2. **Monitor Infrastructure**: Set up monitoring for both client and server machines
3. **Production Configuration**: Use production-like hardware and database settings

### Required Software

- Java 17 or later
- Gradle 8.5 or later (wrapper included)
- ICE REST Catalog running and accessible

## Quick Start

### 1. Configure Your Benchmark

Edit `src/test/resources/application.conf`:

```hocon
http {
  base-url = "http://localhost:8181"  # Your ICE catalog URL
}

auth {
  # Set your bearer token here or via ICE_BEARER_TOKEN env var
  bearer-token = "your-token-here"
}

dataset.tree {
  namespace-width = 2      # Child namespaces per parent
  namespace-depth = 3      # Levels of nesting
  tables-per-namespace = 5
  views-per-namespace = 2
  
  default-base-location = "s3://your-bucket/warehouse/"
}

workload {
  create-tree-dataset {
    namespace-concurrency = 20
    table-concurrency = 40
    view-concurrency = 40
  }
  
  read-update-tree-dataset {
    read-write-ratio = 0.9
    throughput = 150
    duration-in-minutes = 10
  }
}
```

### 2. Run Your First Benchmark

#### Option A: Using the Helper Script (Easiest)

```bash
cd benchmarks
./run-with-token.sh "your-bearer-token-here"
```

#### Option B: Using Environment Variable

```bash
cd benchmarks
export ICE_BEARER_TOKEN="your-bearer-token-here"
mvn gatling:test -Dgatling.simulationClass=com.altinity.ice.benchmarks.simulations.CreateTreeDatasetSimulation
```

#### Option C: Set Token in Config File

Edit `src/test/resources/application.conf` and set your token:
```hocon
auth {
  bearer-token = "your-actual-token"
}
```

Then run:
```bash
cd benchmarks
mvn gatling:test -Dgatling.simulationClass=com.altinity.ice.benchmarks.simulations.CreateTreeDatasetSimulation
```

**What it does:**
- Creates a hierarchical tree of namespaces
- Creates tables and views in each namespace
- Adds properties and schemas to all entities

**Expected output:**
```
Dataset configuration:
  Namespaces: 14
  Tables: 70
  Views: 28
  Total entities: 112

---- Requests ---------|--Total--|---OK----|---KO----
> Global               |     112 |     112 |        0
> Create Namespace     |      14 |      14 |        0
> Create Table         |      70 |      70 |        0
> Create View          |      28 |      28 |        0
```

#### Mixed Read/Write Workload (90/10)

```bash
./run-with-token.sh "your-token" ReadUpdateTreeDatasetSimulation
# OR
mvn gatling:test -Dgatling.simulationClass=com.altinity.ice.benchmarks.simulations.ReadUpdateTreeDatasetSimulation
```

**What it does:**
- Runs for 10 minutes (configurable)
- 90% reads: list tables, load metadata, check existence
- 10% writes: update properties, modify metadata
- Target: 150 ops/s

**Expected output:**
```
Mixed workload configuration:
  Read/Write ratio: 90.0% / 10.0%
  Target throughput: 150 ops/s
  Duration: 10 minutes
  Total operations: 90000

---- Requests ----------------------------|--Total--|---OK----|---KO----
> Read / List Tables                      |   27000 |   27000 |        0
> Read / Load Table                       |   27000 |   27000 |        0
> Write / Update Namespace Properties     |    4500 |    4500 |        0
> Write / Update Table Metadata           |    4500 |    4500 |        0
```

#### Read-Only Workload (100% Read)

```bash
./run-with-token.sh "your-token" ReadOnlyWorkloadSimulation
# OR
mvn gatling:test -Dgatling.simulationClass=com.altinity.ice.benchmarks.simulations.ReadOnlyWorkloadSimulation
```

**What it does:**
- Pure read operations
- Target: 500 ops/s (higher throughput)
- Tests caching and read scalability

## Understanding the Results

After each benchmark, Gatling generates an HTML report:

```
Reports generated, please open the following file:
file:///path/to/ice/benchmarks/build/reports/gatling/simulation-timestamp/index.html
```

### Key Metrics to Review

1. **Throughput (Cnt/s)**: Operations per second achieved
2. **Response Times**:
   - **p50 (median)**: 50% of requests completed in this time
   - **p95**: 95% of requests completed in this time
   - **p99**: 99% of requests completed in this time
3. **Error Rate**: Should be 0% for successful benchmarks

### Example Results

From the summary table:

| Request | Count | Cnt/s | Min | p50 | p95 | p99 | Max | Mean | StdDev |
|---------|-------|-------|-----|-----|-----|-----|-----|------|--------|
| Create Table | 3,172 | 135.78 | 45ms | 89ms | 124ms | 154ms | 278ms | 94ms | 31ms |
| List Tables | 27,000 | 450.12 | 3ms | 7ms | 11ms | 15ms | 42ms | 8ms | 3ms |
| Load Table | 27,000 | 450.08 | 5ms | 12ms | 18ms | 24ms | 67ms | 13ms | 5ms |

**Interpretation:**
- **Create Table**: Sustained 135.78 tables/s with median latency of 89ms, p99 of 154ms
- **List Tables**: Sustained 450.12 lists/s with median latency of 7ms (good caching!)
- **Load Table**: Sustained 450.08 loads/s with median latency of 12ms

## Configuration Reference

### Dataset Size

Calculate total entities:
```
namespaces = sum of n^i for i=1 to depth, where n=width
tables = namespaces × tables-per-namespace
views = namespaces × views-per-namespace
```

Example (width=2, depth=14):
- Namespaces: ~16,383
- Tables: ~81,915
- Views: ~32,766
- **Total: ~131,064 entities**

### Scaling Your Tests

#### Small Scale (Development)
```hocon
dataset.tree {
  namespace-width = 2
  namespace-depth = 3      # 14 namespaces
  tables-per-namespace = 2 # 28 tables
}
```

#### Medium Scale (Staging)
```hocon
dataset.tree {
  namespace-width = 3
  namespace-depth = 5      # 363 namespaces
  tables-per-namespace = 5 # 1,815 tables
}
```

#### Large Scale (Production)
```hocon
dataset.tree {
  namespace-width = 2
  namespace-depth = 14     # 16,383 namespaces
  tables-per-namespace = 5 # 81,915 tables
}
```

### Environment Variables

Override configuration via environment variables:

```bash
export ICE_CATALOG_URL="https://prod-catalog.example.com:8181"
export ICE_BEARER_TOKEN="your-bearer-token"
export ICE_BASE_LOCATION="s3://prod-warehouse/"

./run-with-token.sh "$ICE_BEARER_TOKEN"
```

## Advanced Usage

### Custom Configuration File

```bash
./gradlew gatlingRun-com.altinity.ice.benchmarks.simulations.CreateTreeDatasetSimulation \
  -Dconfig.file=/path/to/custom.conf
```

### JVM Tuning

Edit `build.gradle` to adjust Gatling JVM args:

```gradle
gatling {
  jvmArgs = [
    '-Xms4G',          # Initial heap
    '-Xmx8G',          # Max heap
    '-XX:+UseG1GC'     # Garbage collector
  ]
}
```

### Running Multiple Simulations

```bash
# Run all simulations
./gradlew gatlingRun

# Run specific simulation with short name
./gradlew gatlingRun-CreateTreeDatasetSimulation
```

## Troubleshooting

### Authentication Failures

```
ERROR: 401 Unauthorized or 403 Forbidden
```

**Solution**: 
1. Verify your bearer token is correct
2. Set it via `ICE_BEARER_TOKEN` environment variable
3. Or update `bearer-token` in `application.conf`

### Connection Refused

```
ERROR: Connection refused: localhost/127.0.0.1:8181
```

**Solution**: 
1. Ensure ICE REST Catalog is running
2. Verify `base-url` in configuration
3. Check firewall rules

### Out of Memory

```
ERROR: java.lang.OutOfMemoryError: Java heap space
```

**Solution**: Increase JVM heap in `build.gradle`:
```gradle
jvmArgs = ['-Xmx8G']
```

### Dataset Too Large

If creating a large dataset fails:

1. **Reduce concurrency**: Lower `table-concurrency` values
2. **Increase timeouts**: Adjust `http.request-timeout`
3. **Create in batches**: Run multiple smaller benchmarks

## Best Practices

### 1. Baseline Performance

Before making changes:
```bash
# Run baseline benchmark
./gradlew gatlingRun-ReadUpdateTreeDatasetSimulation

# Save results
cp -r build/reports/gatling/latest/ baseline-results/
```

### 2. Compare Results

After optimization:
```bash
# Run new benchmark
./gradlew gatlingRun-ReadUpdateTreeDatasetSimulation

# Compare with baseline
# Look for improvements in p50, p95, p99 latencies
```

### 3. Monitor During Tests

- **CPU usage**: Should approach 100% on server
- **Memory**: Watch for leaks or excessive GC
- **Disk I/O**: Check database performance
- **Network**: Ensure no network saturation

### 4. Warm-up Period

For consistent results, run a warm-up:

```bash
# Quick warm-up run
./gradlew gatlingRun-ReadOnlyWorkloadSimulation

# Then run your actual benchmark
./gradlew gatlingRun-ReadUpdateTreeDatasetSimulation
```

## Architecture

```
benchmarks/
├── build.gradle                    # Gradle configuration
├── src/test/
│   ├── scala/
│   │   └── com/altinity/ice/benchmarks/
│   │       ├── IceRestProtocol.scala          # Auth & HTTP config
│   │       ├── DatasetGenerator.scala         # Procedural dataset
│   │       └── simulations/
│   │           ├── CreateTreeDatasetSimulation.scala      # 100% write
│   │           ├── ReadUpdateTreeDatasetSimulation.scala  # 90/10 mix
│   │           └── ReadOnlyWorkloadSimulation.scala       # 100% read
│   └── resources/
│       ├── application.conf         # Main configuration
│       └── logback.xml             # Logging configuration
```

## Contributing

To add new benchmarks:

1. Create a new simulation in `simulations/`
2. Extend `Simulation` class
3. Use `IceRestProtocol` for auth
4. Add workload config to `application.conf`

Example:

```scala
class MyCustomSimulation extends Simulation {
  private val config = ConfigFactory.load()
  
  val myScenario = scenario("My Test")
    .exec(http("My Request").get("/v1/namespaces"))
  
  setUp(
    myScenario.inject(atOnceUsers(10))
  ).protocols(IceRestProtocol.httpProtocol)
   .before(exec(IceRestProtocol.authenticate))
}
```

## References

- [Gatling Documentation](https://gatling.io/docs/current/)
- [Apache Iceberg REST Catalog Spec](https://iceberg.apache.org/docs/latest/rest/)
- [Polaris Benchmarking Framework](https://www.dremio.com/blog/benchmarking-framework-for-the-apache-iceberg-catalog-polaris/)
- [ICE Project](https://github.com/Altinity/ice)

## License

See parent project LICENSE file.

