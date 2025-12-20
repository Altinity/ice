### Local testing 

The `ice insert --watch` can also be tested locally with a ElasticMQ server (which is SQS compatible)

Start the ElasticMQ server
`docker compose up`

Start ice in insert mode.
`insert flowers.iris -p --no-copy --skip-duplicates s3://bucket1/flowers/iris/external-data/ --watch="http://localhost:9324/000000000000/s3-events"`

# for local ElasticMQ server
`ice insert flowers.iris -p --no-copy --skip-duplicates \
  s3://bucket1/flowers/iris/external-data/ \
  --watch="http://localhost:9324/000000000000/s3-events" \
  --watch-endpoint="http://localhost:9324"
  `
  
Insert a S3 notification message(test) to ElasticMQ
```
export AWS_ACCESS_KEY_ID=x
export AWS_SECRET_ACCESS_KEY=x
export AWS_REGION=us-east-1

aws --endpoint-url http://localhost:9324 sqs send-message \
  --queue-url http://localhost:9324/000000000000/s3-events \
  --message-body '{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"bucket1"},"object":{"key":"flowers/iris/external-data/iris.parquet"}}}]}'
{
    "MD5OfMessageBody": "0ca3828dbdd1604d4b22fdfcb1226996",
    "MessageId": "570bfd40-c0be-49f8-8119-25b74aad0894"
}
```
