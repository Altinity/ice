# ice

A CLI for loading data into Iceberg REST catalogs.

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
