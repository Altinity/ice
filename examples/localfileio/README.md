# examples/localfileio

```shell
# open shell containing `sqlite3` (sqlite command line client)
devbox shell

# start Iceberg REST Catalog server backed by sqlite with warehouse set to file://warehouse
ice-rest-catalog

# insert data into catalog
ice insert flowers.iris -p file://iris.parquet

# inspect
ice describe

# list all files
find data/ice-rest-catalog/warehouse

# inspect sqlite data
sqlite3 data/ice-rest-catalog/catalog.sqlite
sqlite> .help 
sqlite> .tables
sqlite> .mode table
sqlite> select * from iceberg_tables;
sqlite> select * from iceberg_namespace_properties;
sqlite> .quit
```
