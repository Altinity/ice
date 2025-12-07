/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.cmd;

import com.altinity.ice.cli.internal.metrics.InsertWatchMetrics;
import com.altinity.ice.internal.io.Matcher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class InsertWatch {

  private static final Logger logger = LoggerFactory.getLogger(InsertWatch.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static void run(
      RESTCatalog catalog,
      TableIdentifier nsTable,
      String[] input,
      String sqsQueueURL,
      boolean terminateAfterOneBatch,
      boolean createTableIfNotExists,
      Insert.Options options)
      throws IOException, InterruptedException {
    run(
        catalog,
        nsTable,
        input,
        sqsQueueURL,
        terminateAfterOneBatch,
        createTableIfNotExists,
        options,
        false);
  }

  public static void run(
      RESTCatalog catalog,
      TableIdentifier nsTable,
      String[] input,
      String sqsQueueURL,
      boolean terminateAfterOneBatch,
      boolean createTableIfNotExists,
      Insert.Options options,
      boolean metricsEnabled)
      throws IOException, InterruptedException {

    if (!options.noCopy() || !options.skipDuplicates()) {
      throw new IllegalArgumentException(
          "--watch currently requires --no-copy and --skip-duplicates");
    }

    if (input.length == 0) {
      throw new IllegalArgumentException("At least one input required");
    }

    var matchers = Arrays.stream(input).map(Matcher::from).toList();
    logger.info("Watching for files matching: {}", Arrays.toString(input));

    // Initialize metrics if enabled
    InsertWatchMetrics metrics = metricsEnabled ? InsertWatchMetrics.getInstance() : null;
    String tableLabel = nsTable.toString();
    String queueLabel = sqsQueueURL;

    final SqsClient sqs = buildSqsClient(sqsQueueURL);
    ReceiveMessageRequest req =
        ReceiveMessageRequest.builder()
            .queueUrl(sqsQueueURL)
            .maxNumberOfMessages(10) // 10 is max
            .waitTimeSeconds(20) // 20 is max
            .build();

    ReceiveMessageRequest tailReq =
        ReceiveMessageRequest.builder()
            .queueUrl(sqsQueueURL)
            .maxNumberOfMessages(10) // 10 is max
            .waitTimeSeconds(0)
            .build();

    logger.info("Pulling messages from {}", sqsQueueURL);

    Supplier<Duration> backoff = () -> Duration.ofSeconds(20);
    Runnable resetBackoff =
        () -> {
          // TODO: implement
        };

    //noinspection LoopConditionNotUpdatedInsideLoop
    do {
      List<Message> batch = new LinkedList<>();
      try {
        var messages = sqs.receiveMessage(req).messages();
        batch.addAll(messages);
      } catch (SdkException e) {
        if (metrics != null) {
          metrics.recordSqsReceiveError(tableLabel, queueLabel);
          metrics.recordRetryAttempt(tableLabel, queueLabel);
        }
        if (!e.retryable()) {
          throw e; // TODO: should we really?
        }
        Duration delay = backoff.get();
        logger.error("Failed to pull messages from the SQS queue (retry in {})", delay, e);
        Thread.sleep(delay);
        continue;
      }
      if (!batch.isEmpty()) {
        try {
          var maxBatchSize = 100; // FIXME: make configurable

          List<Message> tailMessages;
          do {
            tailMessages = sqs.receiveMessage(tailReq).messages();
            batch.addAll(tailMessages);
          } while (!tailMessages.isEmpty() && batch.size() < maxBatchSize);

          if (metrics != null) {
            metrics.recordMessagesReceived(tableLabel, queueLabel, batch.size());
          }

          logger.info("Processing {} message(s)", batch.size());
          // FIXME: handle files not found

          var insertBatch = filter(batch, matchers, metrics, tableLabel, queueLabel);
          if (!insertBatch.isEmpty()) {
            logger.info("Inserting {}", insertBatch);

            try {
              Insert.run(catalog, nsTable, insertBatch.toArray(String[]::new), options);
              if (metrics != null) {
                metrics.recordFilesInserted(tableLabel, queueLabel, insertBatch.size());
                metrics.recordTransactionSuccess(tableLabel, queueLabel);
              }
            } catch (NoSuchTableException e) {
              if (!createTableIfNotExists) {
                if (metrics != null) {
                  metrics.recordTransactionFailed(tableLabel, queueLabel);
                }
                throw e;
              }
              boolean retryInsert = true;
              try {
                CreateTable.run(
                    catalog,
                    nsTable,
                    insertBatch.iterator().next(),
                    null,
                    true,
                    options.useVendedCredentials(),
                    options.s3NoSignRequest(),
                    null,
                    null);
              } catch (NotFoundException nfe) {
                if (!options.ignoreNotFound()) {
                  if (metrics != null) {
                    metrics.recordTransactionFailed(tableLabel, queueLabel);
                  }
                  throw nfe;
                }
                logger.info("Table not created ({} don't exist)", insertBatch);
                retryInsert = false;
              }
              if (retryInsert) {
                Insert.run(catalog, nsTable, insertBatch.toArray(String[]::new), options);
                if (metrics != null) {
                  metrics.recordFilesInserted(tableLabel, queueLabel, insertBatch.size());
                  metrics.recordTransactionSuccess(tableLabel, queueLabel);
                }
              }
            }
          }

          confirmProcessed(sqs, sqsQueueURL, batch, metrics, tableLabel, queueLabel);
        } catch (InterruptedException e) {
          // terminate
          Thread.currentThread().interrupt();
          throw new InterruptedException();
        } catch (Exception e) {
          if (metrics != null) {
            metrics.recordTransactionFailed(tableLabel, queueLabel);
            metrics.recordRetryAttempt(tableLabel, queueLabel);
          }
          Duration delay = backoff.get();
          logger.error("Failed to process batch of messages (retry in {})", delay, e);
          Thread.sleep(delay);
          continue;
        }
      }
      resetBackoff.run();
    } while (!terminateAfterOneBatch);
  }

  private static Collection<String> filter(
      List<Message> messages,
      Collection<Matcher> matchers,
      InsertWatchMetrics metrics,
      String tableLabel,
      String queueLabel) {
    Collection<String> r = new LinkedHashSet<>();
    for (Message message : messages) {
      // Message body() example:
      //
      //  {
      //    "Records": [
      //      {
      //        "eventTime": "2024-07-29T21:12:30.123Z",
      //        "eventName": "ObjectCreated:Put",
      //        "s3": {
      //          "bucket": {
      //            "name": "my-bucket"
      //          },
      //          "object": {
      //            "key": "path/to/my-object.txt",
      //            "size": 12345
      //          }
      //        }
      //      }
      //    ]
      //  }
      JsonNode root;
      try {
        root = objectMapper.readTree(message.body());
      } catch (JsonProcessingException e) {
        logger.error("Failed to parse message#{} body", message.messageId(), e);
        if (metrics != null) {
          metrics.recordMessageParseError(tableLabel, queueLabel);
        }
        // TODO: dlq?
        continue;
      }
      // TODO: use type
      for (JsonNode record : root.path("Records")) {
        if (metrics != null) {
          metrics.recordEventsReceived(tableLabel, queueLabel, 1);
        }
        String eventName = record.path("eventName").asText();
        String bucketName = record.at("/s3/bucket/name").asText();
        String objectKey =
            URLDecoder.decode(record.at("/s3/object/key").asText(), StandardCharsets.UTF_8);
        var target = String.format("s3://%s/%s", bucketName, objectKey);
        logger.info("Received S3 event: {} -> {}", eventName, target);
        // s3:ObjectCreated:{Put,Post,Copy,CompleteMultipartUpload}
        if (eventName.startsWith("ObjectCreated:")) {
          // TODO: exclude metadata/data dirs by default
          if (matchers.stream().anyMatch(matcher -> matcher.test(target))) {
            r.add(target);
            if (metrics != null) {
              metrics.recordEventMatched(tableLabel, queueLabel);
            }
          } else {
            logger.info("Target did not match any input pattern: {}", target);
            if (metrics != null) {
              metrics.recordEventNotMatched(tableLabel, queueLabel);
            }
          }
        } else {
          if (metrics != null) {
            metrics.recordEventSkipped(tableLabel, queueLabel);
          }
          if (logger.isTraceEnabled()) {
            logger.trace("Message skipped: {} {}", eventName, target);
          }
        }
      }
    }
    return r;
  }

  private static void confirmProcessed(
      SqsClient sqs,
      String sqsQueueURL,
      List<Message> messages,
      InsertWatchMetrics metrics,
      String tableLabel,
      String queueLabel) {
    int failedCount = 0;
    int len = messages.size();
    for (int i = 0; i < len; i = i + 10) {
      List<Message> batch = messages.subList(i, Math.min(i + 10, len));
      DeleteMessageBatchResponse res = deleteMessageBatch(sqs, sqsQueueURL, batch);
      if (res.hasFailed()) {
        List<BatchResultErrorEntry> failed = res.failed();
        failedCount += failed.size();
        if (metrics != null) {
          metrics.recordSqsDeleteError(tableLabel, queueLabel, failed.size());
        }
      }
    }
    if (failedCount > 0) {
      // TODO: pick a better exception class
      throw new RuntimeException(String.format("Failed to confirm %d message(s)", failedCount));
    }
  }

  private static DeleteMessageBatchResponse deleteMessageBatch(
      SqsClient sqs, String sqsQueueURL, List<Message> messages) {
    return sqs.deleteMessageBatch(
        DeleteMessageBatchRequest.builder()
            .queueUrl(sqsQueueURL)
            .entries(
                messages.stream()
                    .map(
                        m ->
                            DeleteMessageBatchRequestEntry.builder()
                                .id(m.messageId())
                                .receiptHandle(m.receiptHandle())
                                .build())
                    .toList())
            .build());
  }

  private static SqsClient buildSqsClient(String sqsQueueURL) {
    SqsClientBuilder builder = SqsClient.builder();

    // Extract endpoint from queue URL for non-AWS endpoints (e.g., ElasticMQ)
    // AWS SQS URLs look like: https://sqs.us-east-1.amazonaws.com/123456789012/queue-name
    // ElasticMQ URLs look like: http://localhost:9324/000000000000/queue-name
    try {
      URI uri = URI.create(sqsQueueURL);
      String host = uri.getHost();
      if (host != null && !host.endsWith(".amazonaws.com")) {
        URI endpoint = new URI(uri.getScheme(), null, host, uri.getPort(), null, null, null);
        logger.info("Using custom SQS endpoint: {}", endpoint);
        builder.endpointOverride(endpoint);
      }
    } catch (Exception e) {
      logger.warn("Failed to parse SQS queue URL for endpoint extraction: {}", e.getMessage());
    }

    return builder.build();
  }
}
