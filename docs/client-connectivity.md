# Client Connectivity

How to connect various Iceberg clients to `ice-rest-catalog`.

## DuckDB

Connect DuckDB to an Iceberg REST catalog using a Bearer token.

Official reference: [Iceberg REST Catalogs](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs.html)

### 1. Install extensions

```sql
INSTALL iceberg;
INSTALL httpfs;   -- needed for S3/GCS-backed tables
LOAD iceberg;
LOAD httpfs;
```

### 2. Store the Bearer token in a secret

If you already have a token (from your IdP, catalog UI, `curl`, etc.), put it in an Iceberg secret with `TOKEN`:

```sql
CREATE SECRET iceberg_secret (
    TYPE iceberg,
    TOKEN 'your_bearer_token_here'
);
```

- Use the raw token only — do **not** include the `Bearer ` prefix.
- DuckDB sends it as `Authorization: Bearer <token>` on REST catalog requests.

Optional: add extra headers if your catalog requires them (e.g. GCP billing project):

```sql
CREATE SECRET iceberg_secret (
    TYPE iceberg,
    TOKEN 'your_bearer_token_here',
    EXTRA_HTTP_HEADERS MAP {
        'x-goog-user-project': 'your_gcp_project_id'
    }
);
```

### 3. Attach the REST catalog

```sql
ATTACH 'warehouse_name' AS my_catalog (
    TYPE iceberg,
    SECRET iceberg_secret,
    ENDPOINT 'https://your-rest-catalog.example.com'
);
```

| Parameter          | Meaning                                      |
|--------------------|----------------------------------------------|
| `'warehouse_name'` | Catalog warehouse name (from your provider)  |
| `ENDPOINT`         | Base URL of the Iceberg REST catalog         |
| `SECRET`           | Name of the secret from step 2               |

### 4. Storage access

The Bearer token authenticates to the REST catalog, not necessarily to S3/GCS where data lives.

By default DuckDB uses vended credentials (`ACCESS_DELEGATION_MODE 'vended_credentials'`): the catalog returns temporary storage credentials when you load a table.

If your catalog does not vend credentials, configure storage separately:

```sql
-- Example: direct S3 access
CREATE SECRET s3_secret (
    TYPE s3,
    KEY_ID '...',
    SECRET '...',
    REGION 'us-east-1'
);
ATTACH 'warehouse' AS my_catalog (
    TYPE iceberg,
    SECRET iceberg_secret,
    ENDPOINT 'https://catalog.example.com',
    ACCESS_DELEGATION_MODE 'none'
);
```

Supported storage backends: S3, S3 Tables, GCS (see [limitations](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs.html#limitations)).

### 5. Query tables

```sql
SHOW ALL TABLES;
SELECT * FROM my_catalog.default.my_table LIMIT 10;
```

Tables are referenced as `catalog.schema.table`.
