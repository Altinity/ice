## Testing ###
DuckDB iceberg extension can also be used to validate ice created tables.
https://blog.min.io/duckdb-and-minio-for-a-modern-data-stack/

`duckdb`

`install iceberg`

`load iceberg`

`update extensions`

`create secret iceberg_secret(type iceberg, token 'foo');`


```
set s3_use_ssl=false;
set s3_endpoint='localhost:9000';
create secret(type s3, key_id 'miniouser', secret 'miniopassword');

attach 'warehouse' as iceberg_catalog(type iceberg, secret iceberg_secret, endpoint 'http://localhost:5000');
show all tables;
```
