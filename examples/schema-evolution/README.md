# examples/schema-evolution

1. Generate parquet files with slightly different schema    
(feel free to skip this step if you don't intend to modify included t.v1.parquet & t.v2.parquet)

```shell
# optional: open shell containing `clickhouse`
devbox shell

clickhouse local -q $"
CREATE TABLE t (id Int32, toq String, il Int32, d Int32) ORDER BY id;
INSERT INTO t VALUES 
  (1, 'foo1', 4, 7),
  (2, 'foo2', 5, 8),
  (3, 'foo3', 6, 9);

SELECT * FROM t INTO OUTFILE 't.v1.parquet' TRUNCATE FORMAT Parquet;

ALTER TABLE t RENAME COLUMN toq TO q;
ALTER TABLE t ALTER COLUMN il TYPE Int64;
ALTER TABLE t DROP COLUMN d;
ALTER TABLE t ADD COLUMN a Nullable(String);

SELECT * FROM t INTO OUTFILE 't.v2.parquet' TRUNCATE FORMAT Parquet;

SELECT * FROM 't.v1.parquet';
SELECT * FROM 't.v2.parquet';
"

2. Start Iceberg REST Catalog

```
ice-rest-catalog
```

3. Create Iceberg table from `t.v1.parquet`

```shell
ice insert examples.schema_evolution -p file://t.v1.parquet
ice describe -sp examples.schema_evolution
ice scan examples.schema_evolution
```

4. Update table schema to match `t.v2.parquet`.

```shell
ice alter-table examples.schema_evolution $'[
  {"op":"rename_column","name":"toq","new_name":"q"},
  {"op":"alter_column","name":"il","type":"long"},
  {"op":"drop_column","name":"d"},
  {"op":"add_column","name":"a","type":"string"}
]'
ice describe -sp examples.schema_evolution
ice scan examples.schema_evolution
````

5. Append `t.v2.parquet` to the table. Confirm data can be read.

```shell
ice insert examples.schema_evolution -p file://t.v2.parquet
ice scan examples.schema_evolution
```
