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
  
# Insert rows with sort-key
ice insert --sort-order='[{"column": "VendorID", "desc": true, "nullFirst": true}]' nyc.taxis2 -p https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

# Insert sort-key(multiple columns)
ice insert --sort-order='[{"column": "VendorID", "desc": true, "nullFirst": true}, {"column": "Airport_fee", "desc": false, "nullFirst": false}]' nyc.taxis24 -p https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

# Insert with partition key
ice insert --partition='[{"column": "RatecodeID", "transform": "identity"}]' nyc.taxis20 -p https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

# warning: each parquet file below is ~500mb. this may take a while
AWS_REGION=us-east-2 ice insert btc.transactions -p --s3-no-sign-request \
  s3://aws-public-blockchain/v1.0/btc/transactions/\
date=2025-01-01/part-00000-33e8d075-2099-409b-a806-68dd17217d39-c000.snappy.parquet \
  s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-02/*.parquet

# upload file to minio using local-mc,
# then add file to the catalog without making a copy
ice create-table flowers.iris_no_copy --schema-from-parquet=file://iris.parquet

# create table with partitioning columns
ice create-table flowers.irs_no_copy_partition --schema-from-parquet=file://iris.parquet --partition-by=variety,petal.width

# create table with sort columns
ice create-table flowers.irs_no_copy_sort --schema-from-parquet=file://iris.parquet --sort-order='[{"column": "variety", "desc": false}]'

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
