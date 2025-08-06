## Testing ###
DuckDB iceberg extension can also be used to validate ice created tables.

`duckdb`

`install iceberg`

`load iceberg`

`update extensions`

`D create secret iceberg_secret(type iceberg, token 'foo');`

```
D attach 'warehouse' as iceberg_catalog(type iceberg, secret iceberg_secret, endpoint 'http://localhost:5000');
D show all tables;
```
