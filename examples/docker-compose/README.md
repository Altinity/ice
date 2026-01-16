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

#### Setting ClickHouse Server Timezone

To configure the ClickHouse server timezone:

1. Edit `config.xml` with your desired timezone:

```xml
<clickhouse replace="true">
    <timezone>America/Chicago</timezone>
</clickhouse>
```

2. Uncomment the volume mount in `docker-compose.yaml` under the `clickhouse` service:

```yaml
volumes:
  - ./config.xml:/etc/clickhouse-server/conf.d/config.xml
```

3. Restart the containers: `docker compose down && docker compose up`

#### Querying data using Spark
To set the session timezone in spark, uncomment and set the value for the variable `spark.sql.session.timeZone` 
under the section.
```
configs:
  spark-defaults.conf:
    content: |
```

```
 # spark.sql.session.timeZone                America/Chicago
```

This ensures that Spark SQL sessions will use the specified timezone.

```shell
docker compose -f docker-compose-spark-iceberg.yaml down -v
docker compose -f docker-compose-spark-iceberg.yaml up
docker exec -it spark-iceberg spark-sql

spark-sql> show databases;
spark-sql> show tables in nyc;
spark-sql> select count(*) from nyc.taxis;
```
