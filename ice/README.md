# ice

A CLI for loading data into Iceberg REST catalogs.

## Table of Contents

- [Usage](#usage)
- [Examples](#examples)
  - [Partitioned Insert](#partitioned-insert)
  - [Sorted Insert](#sorted-insert)
  - [Compression](#compression)
  - [Schema Evolution](#schema-evolution)
  - [Delete Partition](#delete-partition)
  - [Insert Without Copy](#insert-without-copy)
  - [Multiple Files](#multiple-files)
  - [Namespace Management](#namespace-management)
  - [Inspect](#inspect)
  - [S3 with Public Data](#s3-with-public-data)
  - [Describe Metadata](#describe-metadata)

## Usage

```shell
echo $'
uri: http://localhost:5000
bearerToken: foo
' > .ice.yaml

# check if we're able to connect to the Iceberg REST Catalog
ice check

# show all tables and their metadata
# TIP: use -a flag to include metrics, schema, etc. 
ice describe

# create table named iris in flowers namespace
# (-p means "create table if not exists")
# 
# Supported URI schemes: `file://`, `https://`, `http://`, `s3://`.
ice create-table flowers.iris -p \
  --schema-from-parquet file://iris.parquet

# Add one or more parquet files to the table.
#
# Supported URI schemes: `file://`, `https://`, `http://`, `s3://`.
# s3:// supports wildcards.
ice insert flowers.iris -p file://iris.parquet

ice delete-table flowers.iris 
```

See `ice --help` for more.

## Examples

For a full walkthrough (including MinIO and ClickHouse setup), see [examples/scratch](../examples/scratch/README.md).

### Partitioned Insert

```shell
# partition by day
ice insert nyc.taxis_p_by_day -p \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet \
  --partition='[{"column":"tpep_pickup_datetime","transform":"day"}]'

# partition by identity
ice insert flowers.iris_partitioned -p file://iris.parquet \
  --partition='[{"column":"sepal.length","transform":"identity"}]'

# partition by bucket with custom name
ice insert flowers.iris_bucketed -p file://iris.parquet \
  --partition='[{"column":"variety","transform":"bucket[3]","name":"var_bucket"}]'
```

Supported transforms: `identity`, `day`, `month`, `year`, `hour`, `bucket[N]`.

### Sorted Insert

```shell
ice insert nyc.taxis_s_by_day -p \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet \
  --sort='[{"column":"tpep_pickup_datetime"}]'
```

### Compression

```shell
ice insert flowers.iris -p file://iris.parquet --compression=zstd

ice insert flowers.iris file://iris2.parquet --compression=snappy
```

Supported codecs: `zstd`, `snappy`, `gzip`, `lz4`.

### Schema Evolution

```shell
# add a column to an existing table
ice alter-table flowers.iris '[{"op":"add_column","name":"extra","type":"string"}]'

# verify the schema change
ice describe -s flowers.iris
```

### Delete Partition

```shell
# dry run first (default)
ice delete nyc.taxis_p_by_day \
  --partition '[{"name": "tpep_pickup_datetime_day", "values": ["2024-12-31"]}]'

# actually delete
ice delete nyc.taxis_p_by_day \
  --partition '[{"name": "tpep_pickup_datetime_day", "values": ["2024-12-31"]}]' \
  --dry-run=false
```

### Insert Without Copy

Insert an S3 file by reference (no data copy) when it's already in the warehouse:

```shell
ice create-table flowers.iris_no_copy --schema-from-parquet=file://iris.parquet
mc cp iris.parquet local/bucket1/flowers/iris_no_copy/
ice insert flowers.iris_no_copy --no-copy s3://bucket1/flowers/iris_no_copy/iris.parquet
```

### Multiple Files

Pipe a list of files to insert as a single atomic transaction:

```shell
cat filelist | ice insert flowers.iris -p -
```

where `filelist` contains one file path per line. If any file fails, the entire transaction is rolled back.

### Namespace Management

```shell
ice create-namespace flowers
ice list-namespaces
ice delete-namespace flowers
```

### Inspect

```shell
# show all tables
ice describe

# show table with schema, partition spec, and sort order
ice describe -s flowers.iris

# show everything (schema, properties, metrics)
ice describe -a flowers.iris

# scan table data
ice scan flowers.iris --limit 10

# list data files in current snapshot
ice files flowers.iris

# list partitions
ice list-partitions nyc.taxis_p_by_day

# describe a parquet file directly
ice describe-parquet file://iris.parquet
```

### S3 with Public Data

Access public S3 datasets without credentials:

```shell
ice insert btc.transactions -p \
  --s3-region=us-east-2 --s3-no-sign-request \
  s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/*.parquet
```

### Describe Metadata

Inspect Iceberg metadata files directly (without a running catalog):

```shell
# summary (default)
ice describe-metadata /path/to/v3.metadata.json

# full schema
ice describe-metadata -S /path/to/v3.metadata.json

# all snapshots
ice describe-metadata --snapshots s3://bucket/metadata/v3.metadata.json

# everything as JSON
ice describe-metadata -a --json /path/to/v3.metadata.json
```
