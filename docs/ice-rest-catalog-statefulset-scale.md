# Scaling ice-rest-catalog StatefulSet from 1 to 3 replicas

This guide walks through deploying ice-rest-catalog as a **StatefulSet** with
1 replica (backed by a 3-node etcd cluster), verifying it works, then scaling
to 3 replicas for high availability.

ice-rest-catalog is stateless — all metadata lives in etcd and all table data
lives in S3/MinIO. Scaling replicas requires no data migration.

## Deploy (1 replica)

```bash
kubectl apply -f ice-rest-catalog-k8s-statefulset.yaml
```

Wait for all pods to be ready:

```bash
kubectl get pods -n iceberg-system -w
```

Expected output (1 catalog pod, 3 etcd pods, 1 MinIO pod):

```
 kubectl get pods -n iceberg-system
NAME                       READY   STATUS      RESTARTS   AGE
etcd-0                     1/1     Running     0          2m47s
etcd-1                     1/1     Running     0          2m47s
etcd-2                     1/1     Running     0          2m47s
ice-rest-catalog-0         1/1     Running     0          2m47s
minio-0                    1/1     Running     0          2m47s
minio-bucket-setup-j2j88   0/1     Completed   0          2m47s                 1/1     Running   0          60s
```

### Set up port-forwarding

```bash
kubectl port-forward -n iceberg-system svc/ice-rest-catalog 8181:8181 &
kubectl port-forward -n iceberg-system svc/minio 9000:9000 &
```

### Verify the catalog is running

```bash
curl -s http://localhost:8181/v1/config | jq .
```

```
Handling connection for 8181
{
  "defaults": {
    "ice.io.default.s3.path-style-access": "true",
    "ice.io.default.client.region": "us-east-1",
    "ice.io.default.s3.endpoint": "http://minio.iceberg-system.svc.cluster.local:9000"
  },
  "overrides": {},
  "endpoints": [
    "POST v1/oauth/tokens",
    "GET v1/credentials",
    "GET v1/config",
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "POST /v1/{prefix}/namespaces/{namespace}/properties",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/register",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/tables/rename",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
    "POST /v1/{prefix}/transactions/commit",
    "GET /v1/{prefix}/namespaces/{namespace}/views",
    "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "POST /v1/{prefix}/namespaces/{namespace}/views",
    "POST /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "POST /v1/{prefix}/views/rename",
    "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}"
  ]
}
```
### Ice configuration.

Before using `ice` , make sure `.ice.yaml` is pointing to rest-catalog 
```

uri: http://localhost:8181
```
### Insert data using ice CLI

```bash
cd ../examples/scratch
ice insert -p flowers.iris file://iris.parquet
```

### Verify data in etcd

```bash
kubectl exec -n iceberg-system etcd-0 -- etcdctl get '' --prefix --keys-only
```

Expected:

```
n/flowers
t/flowers/iris
```

### Verify data across all etcd nodes

```bash
kubectl exec -n iceberg-system etcd-0 -- etcdctl get '' --prefix --keys-only
kubectl exec -n iceberg-system etcd-1 -- etcdctl get '' --prefix --keys-only
kubectl exec -n iceberg-system etcd-2 -- etcdctl get '' --prefix --keys-only
```

All three should return identical keys.

## Pre-flight checks before scaling

Confirm all backends are healthy before adding replicas.

```bash
# etcd cluster health
kubectl exec -n iceberg-system etcd-0 -- etcdctl endpoint health \
  --endpoints=http://etcd-0.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-1.etcd.iceberg-system.svc.cluster.local:2379,http://etcd-2.etcd.iceberg-system.svc.cluster.local:2379

http://etcd-1.etcd.iceberg-system.svc.cluster.local:2379 is healthy: successfully committed proposal: took = 2.870167ms
http://etcd-0.etcd.iceberg-system.svc.cluster.local:2379 is healthy: successfully committed proposal: took = 2.908709ms
http://etcd-2.etcd.iceberg-system.svc.cluster.local:2379 is healthy: successfully committed proposal: took = 3.387625ms



# Catalog API
curl -s http://localhost:8181/v1/namespaces | jq .

Handling connection for 8181
{
  "namespaces": [
    [
      "flowers"
    ]
  ],
  "next-page-token": null
}
```

## Scale to 3 replicas

```bash
kubectl scale statefulset ice-rest-catalog -n iceberg-system --replicas=3
kubectl rollout status statefulset/ice-rest-catalog -n iceberg-system --timeout=120s

Waiting for 2 pods to be ready...
Waiting for 1 pods to be ready...
statefulset rolling update complete 3 pods at revision ice-rest-catalog-69c54c6964..
```

### Verify all 3 pods are ready

```bash
kubectl get pods -n iceberg-system -l app.kubernetes.io/name=ice-rest-catalog -o wide
```

Expected:

```
AME                 READY   STATUS    RESTARTS   AGE   IP            NODE           NOMINATED NODE   READINESS GATES
ice-rest-catalog-0   1/1     Running   0          18m   10.244.2.10   minikube-m03   <none>           <none>
ice-rest-catalog-1   1/1     Running   0          38s   10.244.1.14   minikube-m02   <none>           <none>
ice-rest-catalog-2   1/1     Running   0          38s   10.244.0.12   minikube       <none>           <none>
```

### Verify the Service endpoints

```bash
kubectl get endpoints ice-rest-catalog -n iceberg-system
```

All 3 pod IPs should appear in the endpoints list.

### Test the API

```bash
curl -s http://localhost:8181/v1/namespaces | jq .
curl -s http://localhost:8181/v1/namespaces/flowers/tables | jq .
```

### Insert more data and verify

```bash
ice insert -p flowers.iris2 file://iris.parquet

# Confirm the new table is visible
curl -s http://localhost:8181/v1/namespaces/flowers/tables | jq .

# Confirm etcd has both tables
kubectl exec -n iceberg-system etcd-0 -- etcdctl get '' --prefix --keys-only
```

Expected:

```
n/flowers
t/flowers/iris
t/flowers/iris2
```

## Update PDB and HPA for 3 replicas

With 3 replicas running, tighten the PodDisruptionBudget so at least 2 pods
remain during voluntary disruptions, and set the HPA floor to 3.

```bash
kubectl patch pdb ice-rest-catalog -n iceberg-system --type merge -p \
  '{"spec":{"minAvailable":2}}'

kubectl patch hpa ice-rest-catalog -n iceberg-system --type merge -p \
  '{"spec":{"minReplicas":3}}'
```

Verify:

```bash
kubectl get pdb,hpa -n iceberg-system -l app.kubernetes.io/name=ice-rest-catalog
```

## Update the manifest (optional)

To make future `kubectl apply` runs idempotent at 3 replicas, update
`ice-rest-catalog-k8s-statefulset.yaml`:

```yaml
# StatefulSet
spec:
  replicas: 3

# PodDisruptionBudget
spec:
  minAvailable: 2

# HorizontalPodAutoscaler
spec:
  minReplicas: 3
```

## Summary

```
deploy (1 replica) --> insert data --> verify --> pre-flight --> scale to 3 --> verify --> patch PDB/HPA
```

## Related docs

- [etcd cluster upgrade (live scaling)](etcd-upgrade-etcd-cluster.md)
- [etcd backup, restore & upgrade (with downtime)](etcd-backup-restore-upgrade-3-node.md)
- [Kubernetes setup overview](k8s_setup.md)
- [Architecture](architecture.md)
