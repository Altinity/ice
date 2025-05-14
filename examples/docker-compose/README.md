# examples/docker-compose

In the example below, we:

- launch `ice-rest-catalog`, minio (for s3:// storage) and clickhouse-server
- insert data via `ice`
- query data using clickhouse

```shell
# open shell containing `clickhouse`
# feel free to skip it if you have clickhouse client installed already
devbox shell

docker compose down -v && sudo rm -rf data/
docker compose up # spin up minio, ice-rest-catalog & clickhouse

ice insert nyc.taxis -p \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

clickhouse client --query 'select count(*) from ice.`nyc.taxis`;'
```

### Troubleshooting

1. `docker compose up` fails with `ERROR: Invalid interpolation format for "content" option in config "clickhouse-init": "#!/bin/bash`

Solution: Upgrade docker/docker compose to v2.

### Supplemental

#### Querying data using Spark

```shell
docker compose -f docker-compose-spark-iceberg.yaml down -v
docker compose -f docker-compose-spark-iceberg.yaml up
docker exec -it spark-iceberg spark-sql

spark-sql> show databases;
spark-sql> show tables in nyc;
spark-sql> select count(*) from nyc.taxis;
```
