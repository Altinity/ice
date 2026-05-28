
## Alternative: Backup & restore to a fresh 3-node cluster

If live-scaling fails or you prefer a clean migration with downtime, use this
approach. It works well for ice-rest-catalog because etcd stores only namespace
and table metadata pointers — actual table data lives in S3/MinIO.

**Prerequisites**: `etcdctl` and `etcdutl` installed locally (same v3.5.x as the
cluster). Install via `brew install etcd` on macOS or download from
https://github.com/etcd-io/etcd/releases.


 ### Deploy ice-rest-catalog (3 replicas), etcd (1 node), and MinIO

```bash
kubectl apply -f ice-rest-catalog-k8s-1node.yaml
```

### Set up port-forwarding for ice-rest-catalog and MinIO

```bash
kubectl port-forward -n iceberg-system svc/ice-rest-catalog 8181:8181 &
kubectl port-forward -n iceberg-system svc/minio 9000:9000 &
```

###  Insert data using ice CLI

```bash
ice insert -p flowers.iris file://iris.parquet
```

### Verify keys in etcd

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

###  Verify the 1-node cluster is healthy

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


### Step 1: Take a snapshot of the 1-node cluster

```bash
kubectl port-forward -n iceberg-system pod/etcd-0 2379:2379 &
PF_PID=$!
sleep 2

etcdctl --endpoints=http://127.0.0.1:2379 endpoint health
etcdctl --endpoints=http://127.0.0.1:2379 get '' --prefix --keys-only

etcdctl --endpoints=http://127.0.0.1:2379 snapshot save etcd-backup.db
etcdutl snapshot status etcd-backup.db -w table

kill $PF_PID
```

### Step 2: Stop ice-rest-catalog (prevents writes during migration)

```bash
kubectl scale deployment ice-rest-catalog -n iceberg-system --replicas=0
```

### Step 3: Restore snapshot locally for all 3 members

`etcdutl snapshot restore` creates a separate data directory for each member
with the correct cluster membership baked in.

Make sure `restore` is empty.

```bash
INITIAL_CLUSTER="etcd-0=http://etcd-0.etcd.iceberg-system.svc.cluster.local:2380,etcd-1=http://etcd-1.etcd.iceberg-system.svc.cluster.local:2380,etcd-2=http://etcd-2.etcd.iceberg-system.svc.cluster.local:2380"
CLUSTER_TOKEN="etcd-cluster-iceberg"

etcdutl snapshot restore etcd-backup.db \
  --name etcd-0 \
  --initial-cluster "$INITIAL_CLUSTER" \
  --initial-cluster-token "$CLUSTER_TOKEN" \
  --initial-advertise-peer-urls http://etcd-0.etcd.iceberg-system.svc.cluster.local:2380 \
  --data-dir ./restore/etcd-0

etcdutl snapshot restore etcd-backup.db \
  --name etcd-1 \
  --initial-cluster "$INITIAL_CLUSTER" \
  --initial-cluster-token "$CLUSTER_TOKEN" \
  --initial-advertise-peer-urls http://etcd-1.etcd.iceberg-system.svc.cluster.local:2380 \
  --data-dir ./restore/etcd-1

etcdutl snapshot restore etcd-backup.db \
  --name etcd-2 \
  --initial-cluster "$INITIAL_CLUSTER" \
  --initial-cluster-token "$CLUSTER_TOKEN" \
  --initial-advertise-peer-urls http://etcd-2.etcd.iceberg-system.svc.cluster.local:2380 \
  --data-dir ./restore/etcd-2
```

You should now have three directories: `./restore/etcd-0`, `./restore/etcd-1`,
`./restore/etcd-2`, each containing a `member/` subdirectory.

### Step 4: Tear down the 1-node etcd

```bash
kubectl scale statefulset etcd -n iceberg-system --replicas=0
kubectl wait --for=delete pod/etcd-0 -n iceberg-system --timeout=60s
kubectl delete pvc etcd-data-etcd-0 -n iceberg-system
```

### Step 5: Update the StatefulSet for 3 nodes and create PVCs

```bash
kubectl set env statefulset/etcd -n iceberg-system \
  ETCD_INITIAL_CLUSTER="etcd-0=http://etcd-0.etcd.iceberg-system.svc.cluster.local:2380,etcd-1=http://etcd-1.etcd.iceberg-system.svc.cluster.local:2380,etcd-2=http://etcd-2.etcd.iceberg-system.svc.cluster.local:2380" \
  ETCD_INITIAL_CLUSTER_STATE=existing

kubectl scale statefulset etcd -n iceberg-system --replicas=3
sleep 5
kubectl scale statefulset etcd -n iceberg-system --replicas=0

kubectl wait --for=delete pod/etcd-0 pod/etcd-1 pod/etcd-2 \
  -n iceberg-system --timeout=60s


kubectl get pvc -n iceberg-system -l app.kubernetes.io/name=etcd
```

### Step 6: Copy restored data into each PVC

For each member, run a temporary busybox pod that mounts the PVC, copy the
restored data in, then clean up.

```bash
for i in 0 1 2; do
  echo "--- Populating PVC for etcd-${i} ---"

  kubectl run etcd-restore-${i} --namespace=iceberg-system \
    --image=busybox:1.36 --restart=Never \
    --overrides="{
      \"spec\": {
        \"containers\": [{
          \"name\": \"restore\",
          \"image\": \"busybox:1.36\",
          \"command\": [\"sleep\", \"3600\"],
          \"volumeMounts\": [{
            \"name\": \"etcd-data\",
            \"mountPath\": \"/var/lib/etcd\"
          }]
        }],
        \"volumes\": [{
          \"name\": \"etcd-data\",
          \"persistentVolumeClaim\": {
            \"claimName\": \"etcd-data-etcd-${i}\"
          }
        }]
      }
    }"

  kubectl wait --for=condition=Ready pod/etcd-restore-${i} \
    -n iceberg-system --timeout=60s

  # Clear any leftover data and copy restored member directory in
  kubectl exec -n iceberg-system etcd-restore-${i} -- rm -rf /var/lib/etcd/member
  kubectl cp ./restore/etcd-${i}/member iceberg-system/etcd-restore-${i}:/var/lib/etcd/member

  kubectl delete pod etcd-restore-${i} -n iceberg-system
  echo "--- etcd-${i} PVC populated ---"
done
```

### Step 7: Start the 3-node cluster

```bash
kubectl scale statefulset etcd -n iceberg-system --replicas=3
kubectl rollout status statefulset/etcd -n iceberg-system --timeout=120s

# Verify all 3 members are healthy
kubectl exec -n iceberg-system etcd-0 -- etcdctl member list -w table

kubectl exec -n iceberg-system etcd-0 -- etcdctl endpoint health \
  --endpoints=http://etcd-0.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-1.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-2.etcd.iceberg-system.svc.cluster.local:2379

# Verify data is present
kubectl exec -n iceberg-system etcd-0 -- etcdctl get '' --prefix --keys-only
```

### Step 8: Update ice-rest-catalog config and restart

```bash
kubectl edit configmap ice-rest-catalog-config -n iceberg-system
```

Change the `uri` line to include all 3 endpoints:

```yaml
uri: etcd:http://etcd-0.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-1.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-2.etcd.iceberg-system.svc.cluster.local:2379
```

Bring ice-rest-catalog back up:

```bash
kubectl scale deployment ice-rest-catalog -n iceberg-system --replicas=3
kubectl rollout status deployment/ice-rest-catalog -n iceberg-system
```

### Step 9: Verify end-to-end

```bash
# Verify data is replicated across all 3 etcd nodes
kubectl exec -n iceberg-system etcd-0 -- etcdctl get '' --prefix --keys-only
kubectl exec -n iceberg-system etcd-1 -- etcdctl get '' --prefix --keys-only
kubectl exec -n iceberg-system etcd-2 -- etcdctl get '' --prefix --keys-only

# Verify ice-rest-catalog can read the data
kubectl port-forward -n iceberg-system svc/ice-rest-catalog 8181:8181 &
curl -s http://localhost:8181/v1/namespaces | jq .
```

### Insert newer data 
```
ice  insert -p flowers.iris2 iris.parquet
2026-05-14 12:12:56 [-5-thread-1/62876] INFO c.a.i.c.internal.cmd.Insert > iris.parquet: processing
2026-05-14 12:12:56 [-5-thread-1/62876] INFO c.a.i.c.internal.cmd.Insert > iris.parquet: copying to s3://warehouse/flowers/iris2/data/1778778776420-be3107e7c089b22d28aca3d577b1510d2920d8d0e9e096d52e09541898ebbdc1.parquet
2026-05-14 12:12:57 [-5-thread-1/62876] INFO c.a.i.c.internal.cmd.Insert > iris.parquet: adding data file (copy took 1s)
2026-05-14 12:12:57 [main/62876] INFO o.a.i.SnapshotProducer > Committed snapshot 1973195689872914758 (MergeAppend)
```

### Verify if the newer and existing data exists.
```
kubectl exec -n iceberg-system etcd-0 -- etcdctl get '' --prefix --keys-only
kubectl exec -n iceberg-system etcd-1 -- etcdctl get '' --prefix --keys-only
kubectl exec -n iceberg-system etcd-2 -- etcdctl get '' --prefix --keys-only
Defaulted container "etcd" out of: etcd, backup-helper
n/flowers

t/flowers/iris

t/flowers/iris2

Defaulted container "etcd" out of: etcd, backup-helper
n/flowers

t/flowers/iris

t/flowers/iris2

Defaulted container "etcd" out of: etcd, backup-helper
n/flowers

t/flowers/iris

t/flowers/iris
```

### Cleanup

```bash
rm -rf ./restore etcd-backup.db
```

## Summary (live scaling)

```
member add etcd-1 --> scale to 2 --> wait healthy --> member add etcd-2 --> scale to 3 --> wait healthy
```

The key rule: **always `member add` before the pod starts, never after.**

## Summary (backup & restore)

```
snapshot save --> restore locally for 3 members --> tear down 1-node --> copy data into PVCs --> start 3-node --> update config
```

Preferred when live-scaling has failed or you want a clean-slate cluster.
