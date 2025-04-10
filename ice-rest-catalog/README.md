# ice-rest-catalog

A dead-simple Iceberg REST Catalog.

## Reference

See [../examples/](../examples/) to get started.

Generally speaking, all you need to start your own instance of `ice-rest-catalog` is to 
create `.ice-rest-catalog.yaml` (sample config shown below) and then execute `ice-rest-catalog`. 
That's it.

```shell
uri: "jdbc:sqlite:file:data/ice-rest-catalog/db.sqlite?journal_mode=WAL&synchronous=OFF&journal_size_limit=500"
warehouse: "s3://bucket1/"
s3.endpoint: "http://localhost:9000/"
s3.access-key-id: "miniouser"
s3.secret-access-key: "miniopassword"
ice.token: foo
ice.s3.region: minio
```
