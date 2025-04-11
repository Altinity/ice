# ice

A CLI for loading data into Iceberg REST catalogs.

## Reference

<table>
<thead><tr><th>Command</th><th>Outcome</th></tr></thead>
<tbody>
<tr><td>

```shell
ice check
```

</td><td>

Checks if `ice` is able to connect to the Iceberg REST Catalog using config in `$(pwd)/.ice.yaml`.

Look for `.ice.yaml` files in [../examples/](../examples/) to get started. 

</td></tr>
<tr><td>

```shell
ice describe
```

</td><td>

Sample output: 

```yaml
default:
- nyc:
  - taxis:
      schema_raw: |-
        table {
          1: VendorID: optional int
          2: tpep_pickup_datetime: optional timestamp
          3: tpep_dropoff_datetime: optional timestamp
          4: passenger_count: optional long
          5: trip_distance: optional double
          6: RatecodeID: optional long
          7: store_and_fwd_flag: optional string
          8: PULocationID: optional int
          9: DOLocationID: optional int
          10: payment_type: optional long
          11: fare_amount: optional double
          12: extra: optional double
          13: mta_tax: optional double
          14: tip_amount: optional double
          15: tolls_amount: optional double
          16: improvement_surcharge: optional double
          17: total_amount: optional double
          18: congestion_surcharge: optional double
          19: Airport_fee: optional double
        }
      partition_spec_raw: |-
        []
      sort_order_raw: |-
        []
      properties: 
        write.parquet.compression-codec: "zstd"
      location: s3://bucket1/nyc/taxis
      current_snapshot: 
        sequence_number: 1
        id: 7548400764320456737
        parent_id: null
        timestamp: 1744269811642
        timestamp_iso: "2025-04-10T07:23:31.642Z"
        timestamp_iso_local: "2025-04-10T00:23:31.642-07:00"
        operation: append
        summary:
          added-data-files: "1"
          added-records: "3475226"
          added-files-size: "55934912"
          changed-partition-count: "1"
          total-records: "3475226"
          total-files-size: "55934912"
          total-data-files: "1"
          total-delete-files: "0"
          total-position-deletes: "0"
          total-equality-deletes: "0"
          iceberg-version: "Apache Iceberg 1.8.1 (...)"
        location: s3://bucket1/nyc/taxis/metadata/snap-....avro
```

</td></tr>
<tr><td>

```shell
ice create-table flowers.iris -p \
  --schema-from-parquet \
    file://iris.parquet
```

</td><td>

Creates table named `iris` inside `flowers` namespace
using schema from the input (`iris.parquet` file in this case).  
`-p` is used to ignore TableAlreadyExistsError.

Supported URI schemes: `file://`, `https://`, `http://`, `s3://`.

</td></tr>
<tr><td>

```shell
ice insert flowers.iris -p \
  file://iris.parquet
```

</td><td>

`-p` is an alias for `--create-table`, which instructs `ice` to create a table named `iris` inside `flowers` namespace 
using schema from the input (`iris.parquet` file in this case) (but only if the table does not exist yet).

Once table is found, `ice` appends `iris.parquet` to the catalog.

Supported URI schemes: `file://`, `https://`, `http://`, `s3://`.  

</td></tr>
</tbody></table>

