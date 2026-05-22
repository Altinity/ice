# ice-rest-catalog

A dead-simple Iceberg REST Catalog backed by [etcd](https://etcd.io/).

## Usage

Generally speaking, all you need to start your own instance of `ice-rest-catalog` is to 
create `.ice-rest-catalog.yaml` (schema defined [here](src/main/java/com/altinity/ice/rest/catalog/internal/config/Config.java)) 
and then execute `ice-rest-catalog`. 
That's it.

Examples of `.ice-rest-catalog.yaml` (as well as Kubernetes deployment manifests) can be found [here](../examples/).

## Parallel writers (`commitLock`)

Many concurrent commits to the **same table** can cause repeated `CommitFailedException` (optimistic concurrency). For the **etcd** metastore you can serialize commits per table using etcd’s lock API:

```yaml
commitLock:
  enabled: true
  leaseTtlSeconds: 30
  acquireTimeoutMs: 30000
```

If `enabled` is true but the catalog backend is not etcd, the lock is ignored (warning in logs). When lock acquisition exceeds `acquireTimeoutMs`, the server responds with HTTP **503** so clients can retry.

## Documentation

- [Architecture](../docs/architecture.md) -- components, design principles, HA, backup/recovery
- [Kubernetes Setup](../docs/k8s_setup.md) -- k8s deployment with etcd StatefulSet and replicas
- [etcd Cluster Setup](../docs/etcd-cluster-setup.md) -- docker-compose setup with 3-node etcd, data insertion, replication verification, ClickHouse queries
- [etcd Cluster upgrade](../docs/etcd-backup-restore-upgrade-3-node.md) -- Includes k8s manifest and steps to backup data from 1 node etcd and restoring to 3 nodes.(with downtime)
- [GCS Setup](../docs/ice-rest-catalog-gcs.md) -- configuring ice-rest-catalog with Google Cloud Storage
- [etcd Backend Schema](../docs/etcd-backend-schema.md) -- etcd key/value schema (`n/`, `t/` prefixes) and mapping to SQLite
- [SQLite Backend Schema](../docs/sqlite-backend-schema.md) -- SQLite tables (`iceberg_tables`, `iceberg_namespace_properties`)
