# Iceberg REST Catalog — etcd Backend Schema

This document describes the etcd key/value schema used by the Iceberg REST Catalog to track namespaces and tables.

## Key Prefixes

For the `default` catalog, keys use bare prefixes:

| Prefix | Purpose |
|--------|---------|
| `n/`   | Namespace entries |
| `t/`   | Table entries |

For non-default catalogs, the catalog name is prepended: `<catalogName>/n/` and `<catalogName>/t/`.

Defined in `EtcdCatalog.java`:

```java
private static final String NAMESPACE_PREFIX = "n/";
private static final String TABLE_PREFIX = "t/";
```

## Schema

### Namespaces (`n/`)

**Key format:** `n/<namespace>`

**Value:** JSON map of namespace properties (may be empty `{}`).

| Field | Description |
|-------|-------------|
| key   | `n/` + namespace name (levels joined by `/` for nested namespaces) |
| value | JSON object with namespace properties |

**Example:**

```
key:   n/flowers
value: {}
```

```
key:   n/nyc
value: {}
```

### Tables (`t/`)

**Key format:** `t/<namespace>/<table_name>`

**Value:** JSON object with table metadata pointers.

| Field | Description |
|-------|-------------|
| `table_type` | Object type (e.g. `ICEBERG`) |
| `metadata_location` | S3/file path to the current metadata JSON file |
| `previous_metadata_location` | S3/file path to the previous metadata JSON file (empty if first version) |

**Example:**

```
key:   t/flowers/iris2
value: {
  "table_type": "ICEBERG",
  "metadata_location": "s3://bucket1/flowers/iris2/metadata/00001-5d6604f9-d6f1-4ced-a036-8f20d6c0def2.metadata.json",
  "previous_metadata_location": "s3://bucket1/flowers/iris2/metadata/00000-3f6b12e4-0a85-42bf-8ed0-d7cb443d5830.metadata.json"
}
```

## Mapping to SQLite Backend

The SQLite backend stores the same information in two relational tables. Here is how the etcd keys correspond:

```
etcd key                       SQLite table
───────────────────────────    ──────────────────────────────────
n/<namespace>               -> iceberg_namespace_properties
t/<namespace>/<table>       -> iceberg_tables
```

### `n/` -> `iceberg_namespace_properties`

| etcd | SQLite column |
|------|---------------|
| key prefix after `n/` | `namespace` |
| (implicit) | `catalog_name` = catalog name (e.g. `default`) |
| each entry in JSON value | one row per property: `property_key` + `property_value` |

In SQLite, each namespace property is stored as a separate row. In etcd, all properties for a namespace are stored as a single JSON blob.

### `t/` -> `iceberg_tables`

| etcd | SQLite column |
|------|---------------|
| namespace segment of key | `table_namespace` |
| table segment of key | `table_name` |
| (implicit) | `catalog_name` = catalog name (e.g. `default`) |
| `table_type` in value | `iceberg_type` |
| `metadata_location` in value | `metadata_location` |
| `previous_metadata_location` in value | `previous_metadata_location` |

## Notes

- The composite primary key equivalent in etcd is the key itself (`t/<namespace>/<table>` is unique per table).
- Tables with an empty `previous_metadata_location` are at their initial version (version 0).
- The metadata version can be inferred from the filename prefix (e.g. `00003-...` = version 3).
- All metadata files follow the pattern: `s3://<bucket>/<namespace>/<table>/metadata/<version>-<uuid>.metadata.json`.
- Namespace levels are joined by `/` in the key (e.g. `n/parent/child` for nested namespace `parent.child`).
