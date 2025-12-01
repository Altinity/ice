# ICE REST Catalog Architecture

![ICE REST Catalog Architecture](ice-rest-catalog-architecture.drawio.png)

## Components

- **ice-rest-catalog**: Stateless REST API service (Kubernetes Deployment)
- **etcd**: Distributed key-value store for catalog state (Kubernetes StatefulSet)
- **Object Storage**: S3-compatible storage for data files
- **Clients**: ClickHouse or other Iceberg-compatible engines

## Design Principles

### Stateless Catalog

The `ice-rest-catalog` is completely stateless and deployed as a Kubernetes Deployment with multiple replicas. It can be scaled horizontally without coordination. The catalog does not store any state locally—all metadata is persisted in etcd.

### State Management

All catalog state (namespaces, tables, schemas, snapshots, etc.) is maintained in **etcd**, a distributed, consistent key-value store. Each etcd instance runs as a StatefulSet pod with persistent storage, ensuring data durability across restarts.

### Service Discovery

`ice-rest-catalog` uses **DNS+SRV** records to discover and connect to the etcd cluster. This allows dynamic discovery of etcd endpoints without hardcoded addresses. The catalog queries the etcd headless service for available instances.

### High Availability

- Multiple `ice-rest-catalog` replicas behind a load balancer
- etcd cluster with Raft consensus (quorum-based replication)
- Persistent volumes for etcd data
- S3 for durable object storage

## Data Flow

1. **Client → Catalog**: HTTPS requests to ice-rest-catalog service
2. **Catalog → etcd**: DNS+SRV discovery, then read/write operations on :2379
3. **Catalog → S3**: Direct read/write of Parquet/metadata files
4. **etcd Cluster**: Internal Raft consensus on :2380

