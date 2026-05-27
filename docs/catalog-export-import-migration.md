# Migrate catalog metadata: 1-node etcd to 3-node etcd

Move **catalog registry** data (namespace and table metadata pointers in etcd) from a
1-node etcd cluster to a fresh 3-node etcd cluster using `ice catalog-export` and
`ice catalog-import`. Table data stays in object storage (S3/MinIO/GCS); only registry
keys are copied.

This avoids `etcdctl` / `etcdutl` snapshot restore and PVC data copying. For that
approach, see [etcd backup & restore (3-node)](etcd-backup-restore-upgrade-3-node.md).

Command reference: [Catalog import / export](catalog-import-export.md).

## Starting from an etcd backup file

If the source 1-node etcd is no longer running but you have an `etcd-backup.db` snapshot
(from `etcdctl snapshot save`), restore it to a temporary local 1-node etcd, start
`ice-rest-catalog` against it, then continue with **Step 1** (export).

Requires `etcdctl` and `etcdutl` (same v3.5.x as the backup). See also
[etcd backup & restore (3-node)](etcd-backup-restore-upgrade-3-node.md) for full
snapshot restore on Kubernetes.

```bash
mkdir -p restore
etcdutl snapshot restore etcd-backup.db \
  --name etcd-0 \
  --initial-cluster "etcd-0=http://localhost:2380" \
  --initial-cluster-token etcd-cluster-iceberg \
  --initial-advertise-peer-urls http://localhost:2380 \
  --data-dir ./restore/etcd-0

etcd --name etcd-0 \
  --data-dir ./restore/etcd-0 \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://localhost:2379 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --initial-advertise-peer-urls http://localhost:2380 \
  --initial-cluster "etcd-0=http://localhost:2380" \
  --initial-cluster-state existing &

# Verify keys, then start ice-rest-catalog with uri: etcd:http://localhost:2379
etcdctl --endpoints=http://127.0.0.1:2379 get '' --prefix --keys-only

# ice catalog-export -o catalog-snapshot.json   (Step 1)
```

After exporting, stop the temporary etcd and `ice-rest-catalog`, then proceed from
**Step 2** (deploy 3-node cluster and import).

## Prerequisites

- `ice` CLI built and on your `PATH` (or `java -jar ice-...-shaded.jar`)
- Source cluster: `ice-rest-catalog` backed by **1-node** etcd (e.g.
  [ice-rest-catalog-k8s-1node.yaml](ice-rest-catalog-k8s-1node.yaml))
- Target cluster: empty **3-node** etcd + `ice-rest-catalog` (e.g.
  [ice-rest-catalog-k8s.yaml](ice-rest-catalog-k8s.yaml))
- **Same warehouse** on both catalogs (same S3/MinIO bucket and paths). Import only
  restores metadata pointers; it does not copy Parquet files.
- Port-forward or network access to each catalog’s HTTP `addr` (default `8181`)

## Overview

```
export from 1-node catalog --> deploy fresh 3-node catalog --> dry-run import --> import --> verify
```

## Step 1: Export from the 1-node cluster

Port-forward the source catalog (if needed):

```bash
kubectl port-forward -n iceberg-system svc/ice-rest-catalog 8181:8181 &
```

Create `.ice.yaml` pointing at the **source** catalog:

```yaml
uri: http://localhost:8181
# bearerToken: ...   # if auth is enabled
```

Export the full registry to a JSON file:

```bash
ice catalog-export -o catalog-snapshot.json
```

Optional: export one namespace only:

```bash
ice catalog-export --namespace flowers -o catalog-snapshot.json
```

Visually Inspect the exported JSON to confirm it contains the expected keys.

Stop writes on the source (recommended before cutover):

```bash
kubectl scale deployment ice-rest-catalog -n iceberg-system --replicas=0
```

## Step 2: Deploy the 3-node etcd cluster

Deploy a **new** stack with 3 etcd replicas, or tear down the 1-node etcd StatefulSet
and apply the 3-node manifest on a clean etcd data set.

Example (fresh 3-node deploy):

```bash
kubectl apply -f ice-rest-catalog-k8s.yaml
```

Wait for etcd and MinIO to be ready:

```bash
kubectl wait --for=condition=Ready pod/etcd-0 pod/etcd-1 pod/etcd-2 \
  -n iceberg-system --timeout=120s
kubectl rollout status deployment/ice-rest-catalog -n iceberg-system
```

Ensure the target catalog ConfigMap uses all three etcd client endpoints:

```yaml
uri: etcd:http://etcd-0.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-1.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-2.etcd.iceberg-system.svc.cluster.local:2379
warehouse: s3://warehouse
# same s3/minio settings as the source cluster
```

## Step 3: Import into the 3-node cluster

Port-forward the **target** catalog (use a different local port if the source forward
is still running):

```bash
kubectl port-forward -n iceberg-system svc/ice-rest-catalog 8182:8181 &
```

Point `.ice.yaml` at the **target** catalog:

```yaml
uri: http://localhost:8182
```

Preview import (no writes):

```bash
ice catalog-import -i catalog-snapshot.json --dry-run
```

Apply import (skip keys that already exist):

```bash
ice catalog-import -i catalog-snapshot.json
```

To replace existing registry keys on the target:

```bash
ice catalog-import -i catalog-snapshot.json --overwrite
```

## Step 4: Verify

```bash
ice list-namespaces
ice list-tables flowers
ice describe flowers.iris
```

From inside the cluster:

```bash
kubectl exec -n iceberg-system etcd-0 -- etcdctl get '' --prefix --keys-only
kubectl exec -n iceberg-system etcd-1 -- etcdctl get '' --prefix --keys-only
kubectl exec -n iceberg-system etcd-2 -- etcdctl get '' --prefix --keys-only
```

```bash
curl -s http://localhost:8182/v1/namespaces | jq .
```

Insert a new table to confirm writes work on the 3-node cluster:

```bash
ice insert -p flowers.iris2 file://iris.parquet
```

## Step 5: Cleanup

Remove local snapshot if no longer needed:

```bash
rm -f catalog-snapshot.json
```

Decommission the old 1-node etcd deployment when satisfied with the new cluster.

## Same cluster: 1-node etcd to 3-node etcd in place

If you are **replacing** etcd in the same namespace (not a second cluster):

1. Export with Step 1 while the 1-node catalog is still up.
2. Scale etcd to 0, delete old etcd PVCs, apply 3-node etcd config, create fresh PVCs
   (see [etcd backup & restore](etcd-backup-restore-upgrade-3-node.md) Steps 4–7 for
   etcd data layout only if you are **not** using catalog import).
3. Start empty 3-node etcd and `ice-rest-catalog` with the multi-endpoint `uri`.
4. Run Step 3 import — registry is repopulated from `catalog-snapshot.json`.

Warehouse and MinIO data are unchanged; only etcd registry keys are restored via import.

## Comparison

| Approach | Tools | Copies etcd WAL/snap | Cross-cluster |
|----------|-------|----------------------|---------------|
| `ice catalog-export` / `catalog-import` | `ice` only | No (JSON registry) | Yes |
| etcd snapshot restore | `etcdctl`, `etcdutl`, `kubectl cp` | Yes | Harder |
