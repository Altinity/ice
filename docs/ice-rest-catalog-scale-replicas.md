# Scaling ice-rest-catalog from 1 to 3 replicas

This guide walks through scaling the **ice-rest-catalog** Deployment from 1 to 3
replicas on Kubernetes. The catalog is stateless: namespace and table metadata
live in etcd; Iceberg data files live in S3/MinIO. Adding replicas does not
require data migration and can be done with zero downtime.

For high availability of the **metadata backend**, also upgrade etcd from 1 to 3
nodes. See [etcd-backup-restore-upgrade-3-node.md](etcd-backup-restore-upgrade-3-node.md)
(live scaling: [etcd-upgrade-etcd-cluster.md](etcd-upgrade-etcd-cluster.md)).

## Prerequisites

- Cluster deployed from [`ice-rest-catalog-k8s-1node.yaml`](ice-rest-catalog-k8s-1node.yaml)
  (or equivalent) in namespace `iceberg-system`
- `ice-rest-catalog` Deployment currently at **1** replica
- `kubectl` access to the cluster

If you applied the 1-node manifest as-is, the Deployment spec may already list
`replicas: 3`. If only one pod is running, something scaled it down or the
Deployment was patched — these steps still apply.

## Step 1: Verify current state

```bash
kubectl get deployment ice-rest-catalog -n iceberg-system
kubectl get pods -n iceberg-system -l app.kubernetes.io/name=ice-rest-catalog
```

Expected before scaling: one `ice-rest-catalog-*` pod in `Running` / `Ready`.

## Step 2: Pre-flight checks (etcd and MinIO)

All catalog replicas share the same etcd and object store. Confirm backends are
healthy before scaling.

```bash
# etcd (single-node setup)
kubectl exec -n iceberg-system etcd-0 -- etcdctl endpoint health

# MinIO
kubectl exec -n iceberg-system minio-0 -- wget -q -O- http://localhost:9000/minio/health/ready
```

Optional: confirm the catalog API responds:

```bash
kubectl port-forward -n iceberg-system svc/ice-rest-catalog 8181:8181 &
curl -s http://localhost:8181/v1/config | jq .
kill %1
```

## Step 3: Scale the Deployment to 3 replicas

```bash
kubectl scale deployment ice-rest-catalog -n iceberg-system --replicas=3
kubectl rollout status deployment/ice-rest-catalog -n iceberg-system --timeout=120s
```

The Deployment uses a `RollingUpdate` strategy with `maxUnavailable: 0`, so the
existing pod stays available while new pods start.

## Step 4: Verify replicas and the Service

```bash
kubectl get pods -n iceberg-system -l app.kubernetes.io/name=ice-rest-catalog -o wide
kubectl get endpoints ice-rest-catalog -n iceberg-system
```

You should see three pods `1/1 Ready` and three addresses on the
`ice-rest-catalog` Service endpoints.

Test the API (via port-forward or NodePort `30181` on kind):

```bash
kubectl port-forward -n iceberg-system svc/ice-rest-catalog 8181:8181 &
curl -s http://localhost:8181/v1/namespaces | jq .
curl -s http://localhost:8181/v1/config | jq .
```

Optional: confirm writes still work with the `ice` CLI (port-forward MinIO if
needed):

```bash
ice insert -p flowers.iris file://iris.parquet
```

## Step 5: Update PodDisruptionBudget and HPA

The 1-node manifest includes a PDB and HPA. Align them with three replicas so
voluntary disruptions and autoscaling behave as intended.

**PodDisruptionBudget** — allow at most one catalog pod to be unavailable during
drains (2 of 3 remain):

```bash
kubectl patch pdb ice-rest-catalog -n iceberg-system --type merge -p \
  '{"spec":{"minAvailable":2}}'
```

**HorizontalPodAutoscaler** — if the HPA exists but `minReplicas` is still 1,
set the floor to 3:

```bash
kubectl patch hpa ice-rest-catalog -n iceberg-system --type merge -p \
  '{"spec":{"minReplicas":3}}'
```

Verify:

```bash
kubectl get pdb,hpa -n iceberg-system -l app.kubernetes.io/name=ice-rest-catalog
```

## Step 6: Update the manifest for future applies

So `kubectl apply` does not revert your cluster to 1 replica, keep the
Deployment spec in sync:

In [`ice-rest-catalog-k8s-1node.yaml`](ice-rest-catalog-k8s-1node.yaml):

```yaml
spec:
  replicas: 3
```

And optionally:

```yaml
# PodDisruptionBudget
spec:
  minAvailable: 2

# HorizontalPodAutoscaler
spec:
  minReplicas: 3
```

## Upgrade etcd for full HA (recommended)

A single etcd member is a single point of failure for all catalog replicas.
After scaling ice-rest-catalog, upgrade etcd to 3 nodes:

| Approach | Doc |
|----------|-----|
| Live member add (no snapshot) | [etcd-upgrade-etcd-cluster.md](etcd-upgrade-etcd-cluster.md) |
| Backup, restore, fresh 3-node cluster | [etcd-backup-restore-upgrade-3-node.md](etcd-backup-restore-upgrade-3-node.md) |

After etcd is 3-node, update the catalog ConfigMap `uri` to list all endpoints
(see Step 8 in the backup/restore guide), then restart or roll the Deployment:

```yaml
uri: etcd:http://etcd-0.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-1.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-2.etcd.iceberg-system.svc.cluster.local:2379
```

## Scale back to 1 replica (optional)

For dev or resource savings:

```bash
kubectl scale deployment ice-rest-catalog -n iceberg-system --replicas=1
kubectl patch pdb ice-rest-catalog -n iceberg-system --type merge -p \
  '{"spec":{"minAvailable":0}}'
kubectl patch hpa ice-rest-catalog -n iceberg-system --type merge -p \
  '{"spec":{"minReplicas":1}}'
```

## Summary

```
pre-flight (etcd + MinIO) --> scale deployment to 3 --> rollout status --> verify endpoints/API --> patch PDB/HPA --> update YAML
```

Scaling ice-rest-catalog is independent of etcd size; run both upgrades for
production HA.
