### k8s setup

The file ice-rest-catalog-k8s.yaml contains the following components:

| Component | K8s Resource Type | Replicas | Purpose |
|-----------|-------------------|----------|---------|
| ice-rest-catalog | Deployment | 3 | Stateless REST catalog service (horizontally scalable) |
| etcd | StatefulSet | 3 | Distributed key-value store for catalog metadata |
| minio | StatefulSet | 1 | S3-compatible object storage for Iceberg data |

```
kubectl get pods -n iceberg-system
NAME                               READY   STATUS    RESTARTS   AGE
etcd-0                             1/1     Running   0          19h
etcd-1                             1/1     Running   0          19h
etcd-2                             1/1     Running   0          19h
ice-rest-catalog-dcdd9cb99-6gd8h   1/1     Running   0          15h
ice-rest-catalog-dcdd9cb99-bh7kt   1/1     Running   0          15h
ice-rest-catalog-dcdd9cb99-hdx8c   1/1     Running   0          15h
minio-0                            1/1     Running   0          19h
```

---

### Replacing MinIO with AWS S3

For production deployments, you can replace MinIO with AWS S3. Follow these steps:

#### 1. Remove MinIO Resources

Delete or comment out these sections from `ice-rest-catalog-k8s.yaml`:
- `minio-credentials` Secret
- `minio` Service (NodePort)
- `minio-headless` Service
- `minio` StatefulSet
- `minio-bucket-setup` Job

#### 2. Update the ConfigMap

Replace the `ice-rest-catalog-config` ConfigMap with S3 settings:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ice-rest-catalog-config
  namespace: iceberg-system
data:
  config.yaml: |
    uri: etcd:http://etcd.iceberg-system.svc.cluster.local:2379
    
    # Use your S3 bucket path
    warehouse: s3://your-bucket-name/warehouse
    
    # S3 settings (remove endpoint and pathStyleAccess for real S3)
    s3:
      region: us-east-1
    
    addr: 0.0.0.0:8181
    
    anonymousAccess:
      enabled: true
      accessConfig: {}
```

#### 3. Update the Secret with AWS Credentials

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: ice-rest-catalog-secrets
  namespace: iceberg-system
type: Opaque
stringData:
  S3_ACCESS_KEY_ID: "<your-aws-access-key>"
  S3_SECRET_ACCESS_KEY: "<your-aws-secret-key>"
```

#### 4. Remove MinIO Init Container

In the `ice-rest-catalog` Deployment, remove the `wait-for-minio` init container:

```yaml
initContainers:
  - name: wait-for-etcd
    # ... keep this one
  # Remove the wait-for-minio init container
```

#### 5. Create the S3 Bucket

Ensure your S3 bucket exists before deploying:

```bash
aws s3 mb s3://your-bucket-name --region us-east-1
```

#### Summary of Changes

| Resource | Action |
|----------|--------|
| `minio-credentials` Secret | Remove |
| `minio` Service | Remove |
| `minio-headless` Service | Remove |
| `minio` StatefulSet | Remove |
| `minio-bucket-setup` Job | Remove |
| `ice-rest-catalog-config` ConfigMap | Update (remove endpoint, pathStyleAccess) |
| `ice-rest-catalog-secrets` Secret | Update (use AWS credentials) |
| `ice-rest-catalog` Deployment | Remove `wait-for-minio` init container |
