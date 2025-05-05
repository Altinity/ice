# ice-rest-catalog

A dead-simple Iceberg REST Catalog.

## Reference

Generally speaking, all you need to start your own instance of `ice-rest-catalog` is to 
create `.ice-rest-catalog.yaml` (defined [here](src/main/java/com/altinity/ice/rest/catalog/internal/config/Config.java)) 
and then execute `ice-rest-catalog`. 
That's it.

`.ice-rest-catalog.yaml` example:

```shell
uri: "jdbc:sqlite:file:data/ice-rest-catalog/db.sqlite?journal_mode=WAL&synchronous=OFF&journal_size_limit=500"
warehouse: "s3://bucket1"
s3:
  endpoint: "http://localhost:9000"
  accessKeyID: "miniouser"
  secretAccessKey: "miniopassword"
  region: minio
bearerTokens: 
- value: foo
```

For more examples see [../examples/](../examples/). 
