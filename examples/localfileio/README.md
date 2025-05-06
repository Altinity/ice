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
```
