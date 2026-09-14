# How `ice insert --watch` Works

`ice insert --watch` long-polls an SQS queue for S3 object-create events and appends matching Parquet files to an Iceberg table **by reference** (no data copy).

Implemented in [`InsertWatch.java`](../ice/src/main/java/com/altinity/ice/cli/internal/cmd/InsertWatch.java). AWS example: [`examples/s3watch`](../examples/s3watch/README.md). Local ElasticMQ: [`examples/s3watch/test`](../examples/s3watch/test/README.md).

## Usage

Requires `--no-copy` (register existing S3 objects) and `--skip-duplicates` (re-delivered messages must not fail). File arguments are **match patterns**, not a one-time list.

```shell
ice insert flowers.iris -p --no-copy --skip-duplicates \
  s3://$BUCKET/flowers/iris/external-data/*.parquet \
  --watch="$SQS_QUEUE_URL"
```

| Flag | Purpose |
|------|---------|
| `--watch=<url>` | SQS queue URL. |
| `--watch-endpoint=<url>` | Custom SQS endpoint (ElasticMQ, LocalStack). |
| `--watch-fire-once` | One poll cycle, then exit. |
| `--watch-debug-addr=<host:port>` | `/metrics`, `/healtz`, `/livez`, `/readyz`. Enables Prometheus metrics. |
| `--watch-commit-schedule=<expr>` | Accumulate files and commit them as one snapshot on this [skedule](https://github.com/shyiko/skedule) schedule, e.g. `"every 5 minutes"`, `"every day 02:00"`. Default: commit on every poll. |
| `--watch-max-files=<n>` | Commit as soon as this many files are accumulated (default: no limit). |
| `--watch-max-bytes=<n>` | Commit as soon as accumulated files add up to this many bytes (default: no limit). |
| `-p` | Create the table from the first matched file if it does not exist. |

Watch mode also sets `ignoreNotFound=true` so a deleted object does not fail the batch.

## Flow

1. **Poll.** Long-poll SQS (`waitTime=20s`, max 10 messages), then short-poll drain until empty or 100 messages.
2. **Filter.** Parse each body as a raw S3 event (`Records[]`, not SNS-wrapped). Keep `ObjectCreated:*` events whose `s3://bucket/key` matches an input glob. Same key in one drain is inserted once.
3. **Accumulate / insert.** Matched files are held until a flush trigger fires (`--watch-commit-schedule`, `--watch-max-files`, `--watch-max-bytes`, or immediately when none of those are set). Then call the same `Insert.run` as a one-shot insert: read Parquet footers, skip paths already in the snapshot, append `DataFile`s pointing at the existing URIs, commit one snapshot for the whole accumulated batch. With `-p`, table create is delayed until the first matching file arrives.
4. **Ack.** Delete unmatched SQS messages immediately. Delete matched messages only after the accumulated batch commits. On insert/receive failure, do not delete; sleep 20s and retry after visibility timeout.

`--no-copy` requires objects under `table.location()` unless `--force-no-copy` is set.

## Notes

- **At-least-once.** A crash after commit but before delete re-delivers the same keys; `--skip-duplicates` makes that a no-op.
- **Direct S3 → SQS only.** An SNS envelope has no top-level `Records`; those events are ignored.
- **No metadata/data exclusion.** Point the glob at the landing prefix, not the table warehouse root.
- **Visibility timeout.** While files are accumulated, the watcher extends SQS visibility for the held messages (at least 60s, doubled remaining time until the next scheduled commit, capped at 12h). Size the queue default larger than a worst-case insert of a single poll batch.
- Metrics (when `--watch-debug-addr` is set) are listed in [`examples/grafana/METRICS.md`](../examples/grafana/METRICS.md).
