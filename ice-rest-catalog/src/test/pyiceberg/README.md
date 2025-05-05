# ice-rest-catalog/test/pyiceberg

A basic smoke test using [PyIceberg](https://py.iceberg.apache.org/).

## Usage

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then

```shell
cat .pyiceberg.yaml

uv run ice_create_table.py ns1.table1 --location=s3://bucket1/ns1/table1 --schema-from-parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

uv run ice_insert.py ns1.table1 \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```
