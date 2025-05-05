# ice

A CLI for loading data into Iceberg REST catalogs.

## Reference

Sample configs can be found among included [../examples/](../examples/).  
When in doubt, `.ice.yaml` format is defined [here](src/main/java/com/altinity/ice/cli/internal/config/Config.java).

<table>
<thead><tr><th>Command</th><th>Outcome</th></tr></thead>
<tbody>
<tr><td>

```shell
ice check
```

</td><td>

Checks if `ice` is able to connect to the Iceberg REST Catalog using `$(pwd)/.ice.yaml` config.

Sample output:
```shell
OK
```

</td></tr>
<tr><td>

```shell
ice describe
```

</td><td>

Sample output: 

```yaml
...
```

Run with `-a` for more details.

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
using schema from the input (`iris.parquet` file in this case) (but only if the table does not exist).

Once table is found, `ice` appends `iris.parquet` to the catalog.

Supported URI schemes: `file://`, `https://`, `http://`, `s3://`.  

</td></tr>
<tr><td>

```shell
ice delete-table flowers.iris
```

</td><td>

Delete table.

</td></tr>
</tbody></table>

