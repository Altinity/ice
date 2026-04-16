# ice-rest-catalog with etcd Cluster

This guide walks through setting up ice-rest-catalog backed by a 3-node etcd cluster, inserting data with the ice CLI, verifying replication, and querying from ClickHouse.

## Prerequisites

- Docker Compose
- ice CLI (`ice`)
- `etcdctl` (v3)

## 1. Start the etcd Cluster

Use the provided docker-compose file to bring up etcd, MinIO, ice-rest-catalog, and ClickHouse:

```bash
cd examples/docker-compose
docker compose -f docker-compose-etcd.yaml up -d
```

This starts a single-node etcd by default. For a 3-node cluster, replace the `etcd` service definition with three separate nodes (etcd1, etcd2, etcd3) each with their own ports mapped to the host:

| Node  | Client Port |
|-------|-------------|
| etcd1 | 12379       |
| etcd2 | 12479       |
| etcd3 | 12579       |

## 2. Configure ice-rest-catalog

Update the `uri` in your `ice-rest-catalog.yaml` to point at all three etcd endpoints:

```yaml
uri: etcd:http://127.0.0.1:12379,http://127.0.0.1:12479,http://127.0.0.1:12579
warehouse: s3://bucket1

s3:
  endpoint: http://localhost:9000
  pathStyleAccess: true
  accessKeyID: miniouser
  secretAccessKey: miniopassword
  region: minio

bearerTokens:
  - value: foo
```

Start (or restart) ice-rest-catalog so it picks up the new config.

## 3. Insert Data

Use the ice CLI to create a table and insert a Parquet file:

```bash
ice insert flowers.iris file://iris.parquet
```

## 4. Verify Replication with etcdctl

Query the table key across all three etcd endpoints to confirm the data was replicated:

```bash
ETCDCTL_API=3 etcdctl \
  --endpoints=http://127.0.0.1:12379,http://127.0.0.1:12479,http://127.0.0.1:12579 \
  get t/flowers/iris
```

Expected output:

```
t/flowers/iris
{"table_type":"ICEBERG","metadata_location":"s3://bucket1/flowers/iris/metadata/00002-b2cd8da0-74a7-460d-ac3c-12f85d65225b.metadata.json","previous_metadata_location":"s3://bucket1/flowers/iris/metadata/00001-659c1907-5ac7-4ae1-b8c2-573d2f61b45b.metadata.json"}
```

This confirms the key was replicated across all etcd instances.

## 5. Query from ClickHouse

Connect to ClickHouse and create the Iceberg catalog database:

```sql
CREATE DATABASE ice
  ENGINE = DataLakeCatalog('http://host.docker.internal:5000')
  SETTINGS
    catalog_type = 'rest',
    auth_header = 'Authorization: Bearer foo',
    storage_endpoint = 'http://host.docker.internal:9000',
    warehouse = 's3://bucket1',
    aws_access_key_id = 'miniouser',
    aws_secret_access_key = 'miniopassword';
```

Query the table:

```sql
SELECT * FROM ice.`flowers.iris`;
```

```
┌─sepal.length─┬─sepal.width─┬─petal.length─┬─petal.width─┬─variety────┐
│          5.1 │         3.5 │          1.4 │         0.2 │ Setosa     │
│          4.9 │           3 │          1.4 │         0.2 │ Setosa     │
│          4.7 │         3.2 │          1.3 │         0.2 │ Setosa     │
│          4.6 │         3.1 │          1.5 │         0.2 │ Setosa     │
│            5 │         3.6 │          1.4 │         0.2 │ Setosa     │
│          5.4 │         3.9 │          1.7 │         0.4 │ Setosa     │
└──────────────┴─────────────┴──────────────┴─────────────┴────────────┘
```
