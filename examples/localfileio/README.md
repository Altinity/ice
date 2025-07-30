# examples/localfileio

This example is primarily intended for learning and experimentation.  
All data is stored in data/ directory as regular files.

```shell
# optional: open shell containing `sqlite3` (sqlite command line client)
devbox shell

# start Iceberg REST Catalog server backed by sqlite with warehouse set to file://warehouse
ice-rest-catalog

# insert data into catalog
ice insert flowers.iris -p file://iris.parquet

# inspect
ice describe

# list all warehouse files
find data/ice-rest-catalog/warehouse

# inspect sqlite data
sqlite3 data/ice-rest-catalog/catalog.sqlite
sqlite> .help 
sqlite> .tables
sqlite> .mode table
sqlite> select * from iceberg_tables;
sqlite> select * from iceberg_namespace_properties;
sqlite> .quit

# open ClickHouse* shell, then try SQL below
docker run -it --rm --network host -v $(pwd)/data/ice-rest-catalog/warehouse:/warehouse \
  altinity/clickhouse-server:25.3.3.20186.altinityantalya clickhouse local
```

> \* currently this only works with altinity/clickhouse-server:25.3+ builds.

```sql
-- enable Iceberg support (required as of 25.4.1.1795)
SET allow_experimental_database_iceberg = 1;

-- (re)create ice db  
DROP DATABASE IF EXISTS ice;

CREATE DATABASE ice
  ENGINE = DataLakeCatalog('http://localhost:5000')
  SETTINGS catalog_type = 'rest',
    vended_credentials = false,
    warehouse = 'warehouse';

SHOW TABLES FROM ice;

-- inspect
SHOW DATABASES;
SHOW TABLES FROM ice;
SHOW CREATE TABLE ice.`flowers.iris`;

select count(*) from ice.`flowers.iris`;
select * from ice.`flowers.iris` limit 10 FORMAT CSVWithNamesAndTypes;
```
