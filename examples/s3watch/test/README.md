### Local testing 

The `ice insert --watch` can also be tested locally with a ElasticMQ server (which is SQS compatible)

### Start the ElasticMQ server
`docker compose up`

### Start ice in insert mode for local ElasticMQ server
The `--watch-debug-addr` is used to expose the debug endpoint for the watch process.
```
ice insert flowers.iris -p --no-copy --skip-duplicates \
  s3://bucket1/flowers/iris/external-data/ \
  --watch="http://localhost:9324/000000000000/s3-events" \
  --watch-endpoint="http://localhost:9324"
  --watch-debug-addr=0.0.0.0:5002
  ```
  
### Insert a S3 notification message(test) to ElasticMQ
```
export AWS_ACCESS_KEY_ID=x
export AWS_SECRET_ACCESS_KEY=x
export AWS_REGION=us-east-1

aws --endpoint-url http://localhost:9324 sqs send-message --queue-url http://localhost:9324/000000000000/s3-events --message-body '{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"bucket1"},"object":{"key":"flowers/iris/external-data/iris.parquet"}}}]}'
```
