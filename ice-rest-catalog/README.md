# ice-rest-catalog

A dead-simple Iceberg REST Catalog backed by [etcd](https://etcd.io/).

## Usage

Generally speaking, all you need to start your own instance of `ice-rest-catalog` is to 
create `.ice-rest-catalog.yaml` (schema defined [here](src/main/java/com/altinity/ice/rest/catalog/internal/config/Config.java)) 
and then execute `ice-rest-catalog`. 
That's it.

Examples of `.ice-rest-catalog.yaml` (as well as Kubernetes deployment manifests) can be found [here](../examples/).

## Documentation

- [Architecture](../docs/architecture.md) -- components, design principles, HA, backup/recovery
- [Kubernetes Setup](../docs/k8s_setup.md) -- k8s deployment with etcd StatefulSet and replicas
- [etcd Cluster Setup](../docs/etcd-cluster-setup.md) -- docker-compose setup with 3-node etcd, data insertion, replication verification, ClickHouse queries
- [GCS Setup](../docs/ice-rest-catalog-gcs.md) -- configuring ice-rest-catalog with Google Cloud Storage
- [etcd Backend Schema](../docs/etcd-backend-schema.md) -- etcd key/value schema (`n/`, `t/` prefixes) and mapping to SQLite
- [SQLite Backend Schema](../docs/sqlite-backend-schema.md) -- SQLite tables (`iceberg_tables`, `iceberg_namespace_properties`)
