# examples/s3watch

This example demonstrates how ice can be used to continuously add files to the catalog as they are being uploaded to s3
bucket. It works by making ice listen for S3 object creation events via SQS queue.  

1. Allocate AWS resources.

```shell
devbox shell

# auth into AWS
#
# either create a file named "aws.credentials" containing
#
#   export AWS_ACCESS_KEY_ID=...
#   export AWS_SECRET_ACCESS_KEY=...
#   export AWS_SESSION_TOKEN=...
#   export AWS_REGION=us-west-2
#
# and then load it as shown below or use any other method
source aws.credentials

# create s3 bucket + configure notification queue
terraform init
terraform apply

# save terraform output for easy loading
echo $"
export CATALOG_S3_BUCKET_NAME=$(terraform output -raw s3_bucket_name)
export CATALOG_SQS_QUEUE_URL=$(terraform output -raw sqs_queue_url)
" > tf.export
```

2. Start Iceberg REST Catalog. 

```shell
devbox shell

source aws.credentials
source tf.export

# generate config
cat .ice-rest-catalog.envsubst.yaml | \
  envsubst -no-unset -no-empty > .ice-rest-catalog.yaml

# run
ice-rest-catalog
```

3. Start `ice insert` in watch mode.

```shell
devbox shell

source aws.credentials # for sqs:ReceiveMessages
source tf.export

# run
ice insert flowers.iris -p --no-copy --skip-duplicates \
  s3://$CATALOG_S3_BUCKET_NAME/flowers/iris/external-data/*.parquet \
  --watch="$CATALOG_SQS_QUEUE_URL"
```

4. Put some data into s3 bucket any way you want, e.g. using `aws s3 cp`.

```shell
devbox shell

source aws.credentials
source tf.export

# upload data to s3
aws s3 cp iris.parquet s3://$CATALOG_S3_BUCKET_NAME/flowers/iris/external-data/
```

5. Query data from ClickHouse.

```shell
devbox shell

source tf.export

clickhouse local -q $"
SET allow_experimental_database_iceberg = 1;

-- (re)create iceberg db  
DROP DATABASE IF EXISTS ice;  

CREATE DATABASE ice
  ENGINE = DataLakeCatalog('http://localhost:5000')
  SETTINGS catalog_type = 'rest',
    auth_header = 'Authorization: Bearer foo', 
    warehouse = 's3://${CATALOG_S3_BUCKET_NAME}';

select count(*) from ice.\`flowers.iris\`;
"
```

6. Clean up. 

```shell
devbox shell

source aws.credentials

terraform destroy
rm -rf data/
```
