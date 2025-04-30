# examples/docker-compose

In the example below, we:

- launch `ice-rest-catalog`, minio (for s3:// storage) and clickhouse-server
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

#### Spark Iceberg 
A spark-iceberg container can be launched using the `docker-compose-spark-iceberg.yml` file.


The default configuration is located in the following path
`/opt/spark/conf/spark-defaults.conf`

For spark to communicate with `ice-rest-catalog` and `minio`, the following configuration variables need to be updated.\

`spark.sql.catalog.demo.uri` - ice-rest-catalog URI \
`spark.sql.catalog.demo.s3.endpoint` - minio server url.
`spark.sql.catalog.demo.s3.access-key` - minio access key.
`spark.sql.catalog.demo.s3.secret-key` - minio password.


```
spark.sql.extensions                   org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.demo                 org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.demo.type            rest
spark.sql.catalog.demo.uri             http://localhost:5000
spark.sql.catalog.demo.io-impl         org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.demo.warehouse       s3://warehouse/wh/
spark.sql.catalog.demo.s3.endpoint     http://localhost:9000
spark.sql.catalog.demo.s3.access-key   miniouser
spark.sql.catalog.demo.s3.secret-key   miniopassword
spark.sql.catalog.demo.s3.path-style-access true
spark.sql.catalog.demo.s3.ssl-enabled  false
spark.sql.defaultCatalog               demo
spark.eventLog.enabled                 true
spark.eventLog.dir                     /home/iceberg/spark-events
spark.history.fs.logDirectory          /home/iceberg/spark-events
spark.sql.catalogImplementation        in-memory
```