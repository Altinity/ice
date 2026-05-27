# Catalog import / export

Export and import the **catalog registry** (namespace and table metadata pointers in etcd). Table data stays in object storage (S3/MinIO/GCS); only registry keys are moved.

**Requires:** etcd-backed `ice-rest-catalog`. Configure the CLI with `.ice.yaml` (`uri`, optional `bearerToken`).

## Export

```bash
ice catalog-export -o catalog-snapshot.json
```

| Option | Description |
|--------|-------------|
| `-o`, `--output` | Output file (`-` or omit for stdout) |
| `--namespace` | Export one namespace and its tables only (e.g. `flowers`) |

Example (single namespace to stdout):

```bash
ice catalog-export --namespace flowers
```

Snapshot JSON fields: `version`, `catalog_name`, `exported_at`, `namespaces[]`, `tables[]` (each entry has `key` and `value`).

## Import

```bash
ice catalog-import -i catalog-snapshot.json
```

| Option | Description |
|--------|-------------|
| `-i`, `--input` | Input file (`-` or omit for stdin) |
| `--dry-run` | Preview only; no writes |
| `--overwrite` | Replace existing keys (default: skip existing) |

Preview changes:

```bash
ice catalog-import -i catalog-snapshot.json --dry-run
```

Import result JSON: `created`, `skipped`, `overwritten`, `catalog_name`, `exported_at`.

Pipe export → import:

```bash
ice catalog-export | ice catalog-import -
```

## REST API

Same operations on the main catalog server (`addr` in `.ice-rest-catalog.yaml`). Not exposed on optional `adminAddr`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/v1/catalog-export` | Export; optional query `namespace` |
| `POST` | `/admin/v1/catalog-import` | Import; body = snapshot JSON; query `dry-run`, `overwrite` |

Example:

```bash
curl -s http://localhost:8181/admin/v1/catalog-export | jq .
curl -s -X POST "http://localhost:8181/admin/v1/catalog-import?dry-run=true" \
  -H "Content-Type: application/json" \
  -d @catalog-snapshot.json | jq .
```

## Typical uses

- Move catalog metadata between environments (dev → staging)
- Registry backup lighter than a full etcd snapshot
- Clone a namespace into another catalog instance
- [Migrate 1-node etcd to 3-node etcd](catalog-export-import-migration.md) using export/import (no `etcdctl` snapshot restore)

For full etcd cluster backup/restore, see [etcd backup & restore (3-node)](etcd-backup-restore-upgrade-3-node.md).
