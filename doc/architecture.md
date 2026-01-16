# ICE REST Catalog Architecture

![ICE REST Catalog Architecture](ice-rest-catalog-architecture.drawio.png)

## Components

- **ice-rest-catalog**: Stateless REST API service (Kubernetes Deployment)
- **etcd**: Distributed key-value store for catalog state (Kubernetes StatefulSet)
- **Object Storage**: S3-compatible storage for data files
- **Clients**: ClickHouse or other Iceberg-compatible engines

## Design Principles

### Stateless Catalog

The `ice-rest-catalog` is completely stateless and deployed as a Kubernetes Deployment with multiple replicas.
It can be scaled horizontally without coordination. The catalog does not store any state locally—all metadata is persisted in etcd.

### State Management

All catalog state (namespaces, tables, schemas, snapshots, etc.) is maintained in **etcd**, a distributed, consistent key-value store. 
Each etcd instance runs as a StatefulSet pod with persistent storage, ensuring data durability across restarts.

### Service Discovery

`ice-rest-catalog` uses the k8s service to access the cluster.
The catalog uses jetcd library to interact with etcd https://github.com/etcd-io/jetcd. 
In the etcd cluster, the data is replicated in all the nodes of the cluster. 
The service provides a round-robin approach to access the nodes in the cluster.

### High Availability

- Multiple `ice-rest-catalog` replicas behind a load balancer
- etcd cluster.
- Persistent volumes for etcd data
- S3 for durable object storage

## Backup/Recovery
All state information for the catalog is maintained in etcd. To back up the ICE REST Catalog state, you can use standard etcd snapshot tools. The official etcd documentation provides guidance on [snapshotting and recovery](https://etcd.io/docs/v3.5/op-guide/recovery/).

**Backup etcd Example**:
```shell
etcdctl --endpoints=<etcd-endpoint> \
  --cacert=<trusted-ca-file> \
  --cert=<cert-file> \
  --key=<key-file> \
  snapshot save /path/to/backup.db
```

Replace the arguments as appropriate for your deployment (for example, endpoints, authentication, and TLS options).

**Restore etcd Example**:
```shell
etcdctl snapshot restore /path/to/backup.db \
  --data-dir /var/lib/etcd
```

The ICE REST Catalog is designed such that if you restore etcd and point the catalog services at the restored etcd cluster, all catalog state (databases, tables, schemas, snapshots) will be recovered automatically.
  
**Note:** Data files themselves (table/parquet data) are stored in Object Storage (e.g., S3, MinIO), and should be backed up or protected in accordance with your object storage vendor's recommendations.

### k8s Manifest Files

Kubernetes deployment manifests and configuration files are available in the [`examples/eks`](../examples/eks/) folder:

- [`etcd.eks.yaml`](../examples/eks/etcd.eks.yaml) - etcd StatefulSet deployment
- [`ice-rest-catalog.eks.envsubst.yaml`](../examples/eks/ice-rest-catalog.eks.envsubst.yaml) - ice-rest-catalog Deployment (requires envsubst)
- [`eks.envsubst.yaml`](../examples/eks/eks.envsubst.yaml) - Combined EKS deployment template

See the [EKS README](../examples/eks/README.md) for detailed setup instructions.