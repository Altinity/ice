# examples/s3tables

In the example below, we:

- create S3 Table bucket
- insert & query data via `ice`

```shell
# optional: open shell containing `aws` (awscliv2), `envsubst` & `clickhouse`
devbox shell

export CATALOG_BUCKET="$USER-ice-rest-catalog-s3tables-demo"
export AWS_REGION=us-west-1

source aws.credentials

# create S3 Table bucket
aws s3tables create-table-bucket --name "$CATALOG_BUCKET"
export CATALOG_BUCKET_ARN=$(aws s3tables list-table-buckets --query "tableBuckets[?name==\`$CATALOG_BUCKET\`].arn" --output=text)

# start Iceberg REST Catalog
cat .ice-rest-catalog.envsubst.yaml | envsubst -no-unset -no-empty > .ice-rest-catalog.yaml
ice-rest-catalog

# insert data into catalog
ice insert ns1.table1 -p file://iris.parquet

# check the data
ice scan ns1.table1

# clean up
ice delete-table ns1.table1
ice delete-namespace ns1

# delete S3 Table bucket
aws s3tables delete-table-bucket --table-bucket-arn "$CATALOG_BUCKET_ARN"
```
