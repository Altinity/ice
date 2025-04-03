# examples/docker-compose

In the example below, we:

- launch m`ice-rest-catalog`, minio (for s3:// storage) and clickhouse-server
- insert data via `ice`
- query data using clickhouse

```shell
# open shell containing `clickhouse`
# feel free to skip it if you have clickhouse client installed already
devbox shell

docker compose down -v && sudo rm -rf docker-compose.volumes/
docker compose up # spins up ice-rest-catalog + clickhouse

ice insert nyc.taxis -p \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

clickhouse client --query 'select count(*) from ice.`nyc.taxis`;'
```
