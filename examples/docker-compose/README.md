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

## Standalone Apache Polaris catalog

`docker-compose-polaris.yaml` runs a standalone [Apache Polaris](https://polaris.apache.org/)
Iceberg REST catalog backed by MinIO, and connects ClickHouse to it as an Iceberg client.

On startup:

- `minio` + `minio-init` provide `s3://bucket1` storage.
- `polaris` boots a realm with root credentials `root:s3cr3t` (in-memory metastore).
- `polaris-setup` obtains an OAuth token, creates catalog `polariscatalog` pointed at
  `s3://bucket1` on MinIO, and grants the root principal full access.
- `clickhouse` creates database `ice` via the `DataLakeCatalog` REST engine against Polaris.

```shell
docker compose -f docker-compose-polaris.yaml down -v && sudo rm -rf data/
docker compose -f docker-compose-polaris.yaml up
```

Polaris does not ship a data writer, so create/populate tables with any Iceberg client
pointed at Polaris. For example, with Spark:

```
spark.sql.catalog.polaris                 org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.polaris.type            rest
spark.sql.catalog.polaris.uri             http://localhost:8181/api/catalog
spark.sql.catalog.polaris.warehouse       polariscatalog
spark.sql.catalog.polaris.credential      root:s3cr3t
spark.sql.catalog.polaris.scope           PRINCIPAL_ROLE:ALL
spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation vended-credentials
spark.sql.catalog.polaris.io-impl         org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.polaris.s3.endpoint     http://localhost:8999
spark.sql.catalog.polaris.s3.path-style-access true
spark.sql.catalog.polaris.s3.access-key   miniouser
spark.sql.catalog.polaris.s3.secret-key   miniopassword
```

Then query the tables from ClickHouse:

```shell
clickhouse client --query 'SHOW TABLES FROM ice;'
clickhouse client --query 'SELECT count(*) FROM ice.`<namespace>.<table>`;'
```

Notes:

- ClickHouse must be 26.1+ for Polaris OAuth (`catalog_credential` + `oauth_server_uri`)
  in `DataLakeCatalog`. Override the image with `CLICKHOUSE_TAG`
  (e.g. `CLICKHOUSE_TAG=26.1 docker compose -f docker-compose-polaris.yaml up`).
  The Polaris image tag is configurable via `POLARIS_TAG`.
- Polaris uses an in-memory metastore here, so the catalog is re-bootstrapped on every
  `docker compose up` and its metadata does not survive a restart.
