# Rewriting table paths using Spark

Copying a table to a new location in S3 requires rewriting metadata files since they contain absolute paths.
This can be done using the [rewrite_table_path](https://iceberg.apache.org/docs/latest/spark-procedures/#rewrite_table_path) procedure in spark-iceberg.

## Configure Spark

Spark must be configured to connect to the REST catalog that holds your source Iceberg table.
The file [examples/docker-compose/docker-compose-spark-iceberg.yaml](/examples/docker-compose/docker-compose-spark-iceberg.yaml)
in this repo contains a simple Docker Compose to launch spark-iceberg.

Modify the following values under `configs -> spark-defaults.conf`:
- Set `spark.sql.catalog.default.uri` to your catalog URI
- Set `spark.sql.catalog.default.header.authorization` with your bearer token (ex: bearer your-token)
- Set `spark.sql.catalog.default.warehouse` to the path of your catalog warehouse (ex: s3://your-warehouse-iceberg)
- Remove `spark.sql.catalog.default.s3.endpoint`, this is only used for the default MinIO configuration
- Set `spark.sql.catalog.default.s3.access-key` to your S3 access key
- Set `spark.sql.catalog.default.s3.secret-key` to your S3 secret access key
- Add `spark.sql.catalog.default.s3.session-token` with your S3 session token, if using
- Set `spark.sql.catalog.default.client.region` to your S3 region
- Set `spark.sql.catalog.default.s3.ssl-enabled` to true

## Launch spark-sql

After modifying the config values above, bring up the container:

```
cd ../examples/docker-compose
docker compose -f docker-compose-spark-iceberg.yaml up -d
```

Then, launch spark-sql inside the container with additional options. Ensure valid S3 credentials are set.

```
docker exec -it spark-iceberg spark-sql --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET_KEY \
  --conf spark.hadoop.fs.s3a.session.token=YOUR_SESSION_TOKEN
```

## Run procedure

Run the `rewrite_table_path` procedure to copy the Iceberg table metadata files.
Every absolute path with the `source_prefix` is replaced by the `target_prefix`.
The `staging_location` is the location where the copied metadata files are written.

```
CALL default.system.rewrite_table_path(
  table => 'ns.table_name',
  source_prefix => 's3://your-warehouse-iceberg/ns/table_name',
  target_prefix => 's3://your-warehouse-iceberg/new_ns/table_name_rewritten',
  staging_location => 's3://your-warehouse-iceberg/new_ns/table_name_rewritten/metadata);
```

See the [Iceberg docs](https://iceberg.apache.org/docs/latest/spark-procedures/#rewrite_table_path) for info on
other arguments.

After copying the metadata, you must also copy the data files. This can be a simple copy using an external tool like the
AWS CLI. The data files should keep the same directory structure relative to the source and target prefixes.

Finally, register the copied table with the ice-rest-catalog using ice. This can either be the same
or a different catalog (if copied to a different bucket).

```
ice insert new_ns.table_name_rewritten -p 's3://your-warehouse-iceberg/new_ns/table_name_rewritten/data/*.parquet' --no-copy --thread-count=10 
```

If the table is partitioned, add the `--partition` flag with the partition scheme as JSON and change the `s3://` path to
match the path of your data files.
