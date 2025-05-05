# examples/scratch

In the example below, we:

- launch a local instance of Iceberg REST Catalog (`ice-rest-catalog`).
- insert a few `file://`, `https://` and `s3://` parquet files into Iceberg REST Catalog using `ice` CLI.
- query all imported data from ClickHouse.

All without using containers.  
This is mostly intended for `ice`/`ice-rest-catalog` development.

```shell
# open shell containing `clickhouse`, `minio` and `mc` (minio client) + optional etcd/etcdctl, sqlite3 (sqlite client)
devbox shell

# start minio (local s3://), then create bucket named "bucket1"
local-minio
local-mc mb --ignore-existing local/bucket1

# start Iceberg REST Catalog server
ice-rest-catalog

# insert data into catalog
ice insert flowers.iris -p \
  file://iris.parquet

ice insert nyc.taxis -p \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

# warning: each parquet file below is ~500mb. this may take a while
AWS_REGION=us-east-2 ice insert btc.transactions -p --s3-no-sign-request \
  s3://aws-public-blockchain/v1.0/btc/transactions/\
date=2025-01-01/part-00000-33e8d075-2099-409b-a806-68dd17217d39-c000.snappy.parquet \
  s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-02/*.parquet

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

### Troubleshooting

1. `Code: 336. DB::Exception: Unknown database engine: DataLakeCatalog. (UNKNOWN_DATABASE_ENGINE)`

Solution: Make sure `clickhouse --version` is >=25.4.1.2514 or   
use [devbox](https://www.jetify.com/docs/devbox/installing_devbox/) as shown above.   

### Supplemental

#### Inspecting sqlite db content

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

#### Inspecting etcd content

```shell
etcdctl get --prefix ""
```
