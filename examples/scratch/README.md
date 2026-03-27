# examples/scratch

In the example below, we:

- launch a local instance of Iceberg REST Catalog (`ice-rest-catalog`).
- insert a few parquet files from `file://`, `https://` and `s3://` into Iceberg REST Catalog using `ice` CLI.
- query all imported data from ClickHouse.

We rely on [devbox](https://www.jetify.com/docs/devbox/installing_devbox/) to launch a shell with third-party tools, 
but you can skip `devbox shell` if you have `clickhouse` & `minio`/`mc` already. 

```shell
# open shell containing `clickhouse`, `minio` and `mc` (minio client) 
#   + optional etcd/etcdctl, sqlite3 (sqlite client)
devbox shell

# start minio (local s3://), then create bucket named "bucket1"
local-minio
local-mc mb --ignore-existing local/bucket1

# start Iceberg REST Catalog server
# ice-rest-catalog loads config from .ice-rest-catalog.yaml by default 
ice-rest-catalog

# insert data into catalog
# ice loads config from .ice.yaml by default
ice insert flowers.iris -p \
  file://iris.parquet

ice insert nyc.taxis -p \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

# insert data partitioned by day using tpep_pickup_datetime column
ice insert nyc.taxis_p_by_day -p \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet \
  --partition='[{"column":"tpep_pickup_datetime","transform":"day"}]'

# delete partition
ice delete nyc.taxis_p_by_day  \
  --partition '[{"name": "tpep_pickup_datetime", "values": ["2024-12-31T23:51:20"]}]' --dry-run=false

# insert data ordered by tpep_pickup_datetime column
ice insert nyc.taxis_s_by_day -p \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet \
  --sort='[{"column":"tpep_pickup_datetime"}]'

# warning: each parquet file below is ~500mb. this may take a while
ice insert btc.transactions -p --s3-region=us-east-2 --s3-no-sign-request \
  s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/*.parquet

# upload file to minio using local-mc,
# then add file to the catalog without making a copy
ice create-table flowers.iris_no_copy --schema-from-parquet=file://iris.parquet
local-mc cp iris.parquet local/bucket1/flowers/iris_no_copy/
ice insert flowers.iris_no_copy --no-copy s3://bucket1/flowers/iris_no_copy/iris.parquet

# inspect
ice describe

# open ClickHouse shell, then try SQL below 
clickhouse local
```

> TIP: replace `ice` & `ice-rest-catalog` above with `local-ice` & `local-ice-rest-catalog` respectively to use
code in the repo instead of `ice` & `ice-rest-catalog` binaries from the PATH.

```sql
-- enable Iceberg support (required as of 25.4.1.1795)
SET allow_experimental_database_iceberg = 1;

-- (re)create ice db  
DROP DATABASE IF EXISTS ice;

CREATE DATABASE ice
  ENGINE = DataLakeCatalog('http://localhost:5000')
  SETTINGS catalog_type = 'rest',
    auth_header = 'Authorization: Bearer foo', 
    storage_endpoint = 'http://localhost:9000', 
    warehouse = 's3://bucket1';

-- inspect
SHOW DATABASES;
SHOW TABLES FROM ice;
SHOW CREATE TABLE ice.`nyc.taxis`;

select count(*) from ice.`flowers.iris`;
select * from ice.`flowers.iris` limit 10 FORMAT CSVWithNamesAndTypes;

select count(*) from ice.`nyc.taxis`;
select * from ice.`nyc.taxis` limit 10 FORMAT CSVWithNamesAndTypes;

select count(*) from ice.`btc.transactions`;
select * from ice.`btc.transactions` limit 10 FORMAT CSVWithNamesAndTypes;

select count(*) from ice.`flowers.iris_no_copy`;
select * from ice.`flowers.iris_no_copy` limit 10 FORMAT CSVWithNamesAndTypes;
```

> The datasets used above:
> [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
> [AWS Public Blockchain Data](https://registry.opendata.aws/aws-public-blockchain/).
> [Iris Flower Dataset](https://www.tablab.app/parquet/sample).

> To clean up simply terminate processes and `rm -rf data/`.

### Inserting Multiple Files

You can insert multiple parquet files at once by piping a list of files to the insert command:

```shell
# insert multiple files from stdin
cat filelist | ice insert -p flowers.iris -
```

where `filelist` is the list of filenames
```
iris1.parquet
iris3.parquet
iris2.parquet
```

When inserting multiple files to the same table, all files are processed as a **single transaction**. If any file fails (e.g., doesn't exist or has schema issues), **none of the files are committed** to the table:

```
cat filelist| insert flowers.iris -                                       
2026-01-09 11:28:02 [-5-thread-3] INFO c.a.i.c.internal.cmd.Insert > file://iris2.parquet: processing
2026-01-09 11:28:02 [-5-thread-1] INFO c.a.i.c.internal.cmd.Insert > file://iris.parquet: processing
2026-01-09 11:28:02 [-5-thread-2] INFO c.a.i.c.internal.cmd.Insert > file://iris3.parquet: processing
2026-01-09 11:28:03 [-5-thread-1] INFO c.a.i.c.internal.cmd.Insert > file://iris.parquet: copying to s3://bucket1/flowers/iris/data/...parquet
2026-01-09 11:28:03 [-5-thread-3] INFO c.a.i.c.internal.cmd.Insert > file://iris2.parquet: copying to s3://bucket1/flowers/iris/data/...parquet
2026-01-09 11:28:03 [-5-thread-3] INFO c.a.i.c.internal.cmd.Insert > file://iris2.parquet: adding data file (copy took 0s)
2026-01-09 11:28:03 [-5-thread-1] INFO c.a.i.c.internal.cmd.Insert > file://iris.parquet: adding data file (copy took 0s)
2026-01-09 11:28:03 [main] ERROR com.altinity.ice.cli.Main > Fatal
java.io.IOException: Error processing file(s)
    ...
Caused by: java.io.FileNotFoundException: iris3.parquet (No such file or directory)
    ...
```

```sql
-- query shows 0 rows because iris3.parquet failed and the entire transaction was rolled back
select count(*) from `flowers.iris`;
   ┌─count()─┐
1. │       0 │
   └─────────┘
```

When all files are valid, the transaction commits successfully:

```
cat filelist| insert flowers.iris -
2026-01-09 11:29:03 [-5-thread-2] INFO c.a.i.c.internal.cmd.Insert > file://iris2.parquet: processing
2026-01-09 11:29:03 [-5-thread-1] INFO c.a.i.c.internal.cmd.Insert > file://iris.parquet: processing
2026-01-09 11:29:04 [-5-thread-2] INFO c.a.i.c.internal.cmd.Insert > file://iris2.parquet: copying to s3://bucket1/flowers/iris/data/...parquet
2026-01-09 11:29:04 [-5-thread-1] INFO c.a.i.c.internal.cmd.Insert > file://iris.parquet: copying to s3://bucket1/flowers/iris/data/...parquet
2026-01-09 11:29:04 [-5-thread-2] INFO c.a.i.c.internal.cmd.Insert > file://iris2.parquet: adding data file (copy took 0s)
2026-01-09 11:29:04 [-5-thread-1] INFO c.a.i.c.internal.cmd.Insert > file://iris.parquet: adding data file (copy took 0s)
2026-01-09 11:29:04 [main] INFO o.a.i.SnapshotProducer > Committed snapshot 2260546103981693696 (MergeAppend)
```

```sql
-- query shows 300 rows (150 from each file) after successful commit
select count(*) from `flowers.iris`;
   ┌─count()─┐
1. │     300 │
   └─────────┘
```

### Troubleshooting

1. `Code: 336. DB::Exception: Unknown database engine: DataLakeCatalog. (UNKNOWN_DATABASE_ENGINE)`

Solution: Make sure `clickhouse --version` is >=25.4.1.2514 or   
use [devbox](https://www.jetify.com/docs/devbox/installing_devbox/) as shown above.   

### Supplemental

#### Inspecting sqlite content (when used)

```shell
# inspect sqlite data
sqlite3 data/ice-rest-catalog/db.sqlite
sqlite> .help
sqlite> .tables
sqlite> .mode table
sqlite> select * from iceberg_tables;
sqlite> select * from iceberg_namespace_properties;
sqlite> .quit
```

#### Inspecting etcd content (when used)

```shell
etcdctl get --prefix ""
```
