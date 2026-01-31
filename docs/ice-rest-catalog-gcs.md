# Setting up ice-rest-catalog with Google Cloud Storage

This guide covers configuring `ice-rest-catalog` to use GCS buckets as the warehouse location.

## Prerequisites

- A GCS bucket with appropriate permissions
- Service account with Storage Object Admin role (`roles/storage.objectAdmin`)
- Service account JSON key file

## Configuration

Create `.ice-rest-catalog.yaml`:

```yaml
uri: jdbc:sqlite:file:data/ice-rest-catalog/db.sqlite?journal_mode=WAL&synchronous=OFF&journal_size_limit=500
warehouse: gs://your-bucket-name/warehouse

iceberg:
  gcs.project-id: your-gcp-project-id

bearerTokens:
- value: your-secret-token

anonymousAccess:
  enabled: true
  accessConfig: {}
```

## Authentication

Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to your service account key file:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

## Running

```bash
java -jar ice-rest-catalog-0.0.0-SNAPSHOT-shaded.jar -c .ice-rest-catalog.yaml
```

## Configuration Options

| Property | Description |
|----------|-------------|
| `warehouse` | GCS bucket path (`gs://bucket-name/path`) |
| `iceberg.gcs.project-id` | GCP project ID |
