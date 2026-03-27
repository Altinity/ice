# examples/localfileio

This is an example setup where Iceberg table data is stored on your local disk (under /tmp/ice-example/warehouse) instead of
in cloud object storage (S3, GCS, etc.).
This example is primarily intended for learning and experimentation, and development without cloud credentials.
Table data is stored under `/tmp/ice-example/warehouse` as regular files (see `warehouse` in `.ice-rest-catalog.yaml`). The catalog metadata stays under `data/ice-rest-catalog/`.

```shell
# optional: open shell containing `sqlite3` (sqlite command line client)
devbox shell

# start Iceberg REST Catalog server backed by sqlite with warehouse set to file:///tmp/ice-example/warehouse
ice-rest-catalog

# insert data into catalog
ice insert flowers.iris -p file://iris.parquet

# inspect
ice describe

# list all warehouse files
find /tmp/ice-example/warehouse

# inspect sqlite data
sqlite3 data/ice-rest-catalog/catalog.sqlite
sqlite> .help 
sqlite> .tables
sqlite> .mode table
sqlite> select * from iceberg_tables;
sqlite> select * from iceberg_namespace_properties;
sqlite> .quit

# open ClickHouse* shell, then try SQL below 
# IMPORTANT: make sure mount path is set to /tmp
# For Linux:
docker run -it --rm  --add-host=host.docker.internal:host-gateway --network host -v /tmp/ice-example/warehouse:/tmp/ice-example/warehouse \
  altinity/clickhouse-server:25.8.16.20002.altinityantalya clickhouse local
# For Mac:
docker run -it --rm --network host -v /tmp/ice-example/warehouse:/tmp/ice-example/warehouse \
  altinity/clickhouse-server:25.8.16.20002.altinityantalya clickhouse local

```

>  currently this only works with altinity/clickhouse-server:25.3+ builds.

```sql
-- enable Iceberg support (required as of 25.4.1.1795)
SET allow_experimental_database_iceberg = 1;

-- (re)create ice db  
DROP DATABASE IF EXISTS ice;

CREATE DATABASE ice
  ENGINE = DataLakeCatalog('http://host.docker.internal:5000')
  SETTINGS catalog_type = 'rest',
    vended_credentials = false,
    warehouse = 'warehouse';

-- inspect
SHOW DATABASES;
SHOW TABLES FROM ice;


Query id: 8c671684-ec90-4e18-aacd-aee5fae2aeed

   ┌─name──────────┐
1. │ flowers.iris  │



SHOW CREATE TABLE ice.`flowers.iris`;

select count(*) from ice.`flowers.iris`;
select * from ice.`flowers.iris` limit 10 FORMAT CSVWithNamesAndTypes;

Query id: f3a1773d-ebeb-4d26-80fb-7d9e29b5c07e

     ┌─sepal.length─┬─sepal.width─┬─petal.length─┬─petal.width─┬─variety────┐
  1. │          5.1 │         3.5 │          1.4 │         0.2 │ Setosa     │
  2. │          4.9 │           3 │          1.4 │         0.2 │ Setosa     │
  3. │          4.7 │         3.2 │          1.3 │         0.2 │ Setosa     │
  4. │          4.6 │         3.1 │          1.5 │         0.2 │ Setosa     │
  5. │            5 │         3.6 │          1.4 │         0.2 │ Setosa     │
  6. │          5.4 │         3.9 │          1.7 │         0.4 │ Setosa     │
  7. │          4.6 │         3.4 │          1.4 │         0.3 │ Setosa     │
  8. │            5 │         3.4 │          1.5 │         0.2 │ Setosa     │
  9. │          4.4 │         2.9 │          1.4 │         0.2 │ Setosa     │
 10. │          4.9 │         3.1 │          1.5 │         0.1 │ Setosa     │
 11. │          5.4 │         3.7 │          1.5 │         0.2 │ Setosa     │
 12. │          4.8 │         3.4 │          1.6 │         0.2 │ Setosa     │
 13. │          4.8 │           3 │          1.4 │         0.1 │ Setosa     │
 14. │          4.3 │           3 │          1.1 │         0.1 │ Setosa     │
 15. │          5.8 │           4 │          1.2 │         0.2 │ Setosa     │
 16. │          5.7 │         4.4 │          1.5 │         0.4 │ Setosa     │
 17. │          5.4 │         3.9 │          1.3 │         0.4 │ Setosa     │
 18. │          5.1 │         3.5 │          1.4 │         0.3 │ Setosa     │
 19. │          5.7 │         3.8 │          1.7 │
```

The REST catalog `warehouse` must be an absolute `file:///` URI (e.g. `file:///tmp/ice-example/warehouse`). A relative form like `file://warehouse` is rejected by the server.
