# Scaling etcd from 1 node to 3 nodes in Kubernetes

This guide walks through expanding a running 1-node etcd StatefulSet to a 3-node
cluster for ice-rest-catalog, without downtime or data loss.

## Prerequisites

- A running 1-node etcd StatefulSet (`etcd-0`) in namespace `iceberg-system`
  deployed via `ice-rest-catalog-k8s-1node.yaml`
- `kubectl` access to the cluster
- The headless service `etcd` already exists with peer port 2380

## Initial setup (1-node)

### Step 1: Deploy ice-rest-catalog (3 replicas), etcd (1 node), and MinIO

```bash
kubectl apply -f ice-rest-catalog-k8s-1node.yaml
```

### Step 2: Set up port-forwarding for ice-rest-catalog and MinIO

```bash
kubectl port-forward -n iceberg-system svc/ice-rest-catalog 8181:8181 &
kubectl port-forward -n iceberg-system svc/minio 9000:9000 &
```

### Step 3: Insert data using ice CLI

```bash
ice insert -p flowers.iris file://iris.parquet
```

### Step 4: Verify keys in etcd

```bash
kubectl port-forward -n iceberg-system pod/etcd-0 2379:2379 &
etcdctl --endpoints=http://127.0.0.1:2379 get '' --prefix
```

Expected output:

```
n/flowers
{}
t/flowers/iris
{"table_type":"ICEBERG","metadata_location":"s3://warehouse/flowers/iris/metadata/..."}
```

### Step 5: Verify the 1-node cluster is healthy

```bash
kubectl exec -n iceberg-system etcd-0 -- etcdctl endpoint health
kubectl exec -n iceberg-system etcd-0 -- etcdctl member list -w table

127.0.0.1:2379 is healthy: successfully committed proposal: took = 1.508375ms
+------------------+---------+--------+----------------------------------------------------------+----------------------------------------------------------+------------+
|        ID        | STATUS  |  NAME  |                        PEER ADDRS                        |                       CLIENT ADDRS                       | IS LEARNER |
+------------------+---------+--------+----------------------------------------------------------+----------------------------------------------------------+------------+
| 65f9fec08921c158 | started | etcd-0 | http://etcd-0.etcd.iceberg-system.svc.cluster.local:2380 | http://etcd-0.etcd.iceberg-system.svc.cluster.local:2379 |      false |
+------------------+---------+--------+----------------------------------------------------------+----------------------------------------------------------+------------+
```

You should see one member (`etcd-0`) with status `started`.

## Scale from 1 to 3 nodes

### Why "scale to 0, flip to existing, scale to 3" does NOT work

etcd maintains an internal membership list. When etcd-0 is the only member, its
cluster membership is `{etcd-0}`. Setting `ETCD_INITIAL_CLUSTER_STATE=existing`
on a new pod tells etcd "I am joining an existing cluster" but the existing
cluster must already know about the new member. Without a prior `member add`,
the new pod is rejected.

The correct sequence: **always `member add` before the pod starts, never after.**

### Step 6: Update the StatefulSet env vars

Update `ETCD_INITIAL_CLUSTER` to list all 3 nodes and set
`ETCD_INITIAL_CLUSTER_STATE` to `existing`:

```bash
kubectl set env statefulset/etcd -n iceberg-system \
  ETCD_INITIAL_CLUSTER="etcd-0=http://etcd-0.etcd.iceberg-system.svc.cluster.local:2380,etcd-1=http://etcd-1.etcd.iceberg-system.svc.cluster.local:2380,etcd-2=http://etcd-2.etcd.iceberg-system.svc.cluster.local:2380" \
  ETCD_INITIAL_CLUSTER_STATE=existing
```

**Note:** `kubectl set env` changes the pod template, which triggers a restart of
etcd-0. This is safe -- etcd ignores `ETCD_INITIAL_CLUSTER` and
`ETCD_INITIAL_CLUSTER_STATE` after first boot (it uses its data dir / WAL).
Wait for etcd-0 to come back up before proceeding:

```bash
kubectl rollout status statefulset/etcd -n iceberg-system
kubectl exec -n iceberg-system etcd-0 -- etcdctl endpoint health
```

### Step 7: Register etcd-1 in the cluster

Register etcd-1 with the running cluster *before* its pod starts:

```bash
kubectl exec -n iceberg-system etcd-0 -- etcdctl member add etcd-1 \
  --peer-urls=http://etcd-1.etcd.iceberg-system.svc.cluster.local:2380
```

The cluster now knows about etcd-1 but it is not yet running (reported as
`unstarted`).

### Step 8: Ensure etcd-1 has a clean PVC

If a PVC from a previous failed attempt exists, delete it so etcd-1 starts
fresh:

```bash
kubectl get pvc -n iceberg-system | grep etcd-data-etcd-1
# If it exists:
kubectl delete pvc etcd-data-etcd-1 -n iceberg-system
```

### Step 9: Scale to 2

```bash
kubectl scale statefulset etcd -n iceberg-system --replicas=2
```

etcd-1 starts, reads `ETCD_INITIAL_CLUSTER_STATE=existing` from its env, and
joins the cluster. Its PVC is fresh (no data dir), so it bootstraps as a new
member joining an existing cluster.

Wait for etcd-1 to become healthy:

```bash
kubectl exec -n iceberg-system etcd-0 -- etcdctl member list -w table
kubectl exec -n iceberg-system etcd-0 -- etcdctl endpoint health \
  --endpoints=http://etcd-0.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-1.etcd.iceberg-system.svc.cluster.local:2379
```

Both members should show `started` and `healthy`.

### Step 10: Register etcd-2 in the cluster

```bash
kubectl exec -n iceberg-system etcd-0 -- etcdctl member add etcd-2 \
  --peer-urls=http://etcd-2.etcd.iceberg-system.svc.cluster.local:2380
```

### Step 11: Ensure etcd-2 has a clean PVC

```bash
kubectl get pvc -n iceberg-system | grep etcd-data-etcd-2
# If it exists:
kubectl delete pvc etcd-data-etcd-2 -n iceberg-system
```

### Step 12: Scale to 3

```bash
kubectl scale statefulset etcd -n iceberg-system --replicas=3
```

Wait for etcd-2 to join:

```bash
kubectl exec -n iceberg-system etcd-0 -- etcdctl member list -w table
kubectl exec -n iceberg-system etcd-0 -- etcdctl endpoint health \
  --endpoints=http://etcd-0.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-1.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-2.etcd.iceberg-system.svc.cluster.local:2379
```

All 3 members should show `started` and `healthy`.

### Step 13: Update ice-rest-catalog config

Update the `uri` in ice-rest-catalog's ConfigMap to include all 3 endpoints:

```bash
kubectl edit configmap ice-rest-catalog-config -n iceberg-system
```

Change the `uri` line to:

```yaml
uri: etcd:http://etcd-0.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-1.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-2.etcd.iceberg-system.svc.cluster.local:2379
```

Then restart ice-rest-catalog pods to pick up the new config:

```bash
kubectl rollout restart deployment ice-rest-catalog -n iceberg-system
```

### Step 14: Verify data is replicated

```bash
kubectl exec -n iceberg-system etcd-0 -- etcdctl get --prefix t/ --keys-only
kubectl exec -n iceberg-system etcd-1 -- etcdctl get --prefix t/ --keys-only
kubectl exec -n iceberg-system etcd-2 -- etcdctl get --prefix t/ --keys-only
```

All 3 should return the same keys.

## Recovery: if you already scaled to 3 and pods are crash-looping

If etcd-1 and etcd-2 are stuck in CrashLoopBackOff because they were never
registered via `member add`:

```bash
# Scale back to 1
kubectl scale statefulset etcd -n iceberg-system --replicas=1

# Delete the PVCs for the failed pods (they have corrupt/empty data)
kubectl delete pvc etcd-data-etcd-1 -n iceberg-system
kubectl delete pvc etcd-data-etcd-2 -n iceberg-system

# Verify etcd-0 is healthy
kubectl exec -n iceberg-system etcd-0 -- etcdctl endpoint health
kubectl exec -n iceberg-system etcd-0 -- etcdctl member list -w table

# Remove any phantom members that were partially added
# (check member list for unstarted members and remove them)
# kubectl exec -n iceberg-system etcd-0 -- etcdctl member remove <MEMBER_ID>

# Now follow Steps 7-14 above
```
