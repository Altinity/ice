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

import com.altinity.ice.cli.internal.cmd.InsertWatchBuffer.BatchOptions;
import com.altinity.ice.cli.internal.cmd.InsertWatchBuffer.FilterResult;
import com.altinity.ice.cli.internal.metrics.InsertWatchMetrics;
import com.altinity.ice.internal.io.Matcher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.skedule.Schedule;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class InsertWatch {

  private static final Logger logger = LoggerFactory.getLogger(InsertWatch.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String QUEUE_TYPE_SQS = "sqs";

  // Maximum number of entries accepted by the SQS batch APIs.
  private static final int SQS_BATCH_LIMIT = 10;

  // Floor for how long accumulated messages are kept invisible.
  private static final int MIN_VISIBILITY_TIMEOUT_SECONDS = 60;
  private static final int MAX_VISIBILITY_TIMEOUT_SECONDS = 43200; // SQS limit (12h)

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
        null,
        terminateAfterOneBatch,
        createTableIfNotExists,
        options,
        BatchOptions.NONE,
        false);
  }

  public static void run(
      RESTCatalog catalog,
      TableIdentifier nsTable,
      String[] input,
      String sqsQueueURL,
      String sqsOverrideEndpoint,
      boolean terminateAfterOneBatch,
      boolean createTableIfNotExists,
      Insert.Options options,
      BatchOptions batchOptions,
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
    String queueType = QUEUE_TYPE_SQS;

    Schedule schedule =
        batchOptions.commitSchedule() != null
            ? Schedule.parse(batchOptions.commitSchedule())
            : null;
    ZonedDateTime nextCommitAt = schedule != null ? schedule.next(ZonedDateTime.now()) : null;

    if (batchOptions.enabled()) {
      logger.info(
          "Batching commits (schedule: {}, max files: {}, max bytes: {})",
          batchOptions.commitSchedule() != null ? batchOptions.commitSchedule() : "unset",
          batchOptions.maxFiles() > 0 ? String.valueOf(batchOptions.maxFiles()) : "unset",
          batchOptions.maxBytes() > 0
              ? InsertWatchBuffer.formatBytes(batchOptions.maxBytes())
              : "unset");
      if (nextCommitAt != null) {
        logger.info("Next commit scheduled for: {}", nextCommitAt);
      }
    }

    final SqsClient sqs = buildSqsClient(sqsOverrideEndpoint);
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

    final InsertWatchBuffer buffer = new InsertWatchBuffer();

    //noinspection LoopConditionNotUpdatedInsideLoop
    do {
      List<Message> batch = new LinkedList<>();
      try {
        if (metrics != null) {
          metrics.recordPollRequest(tableLabel, queueLabel, queueType);
        }
        var messages = sqs.receiveMessage(req).messages();
        batch.addAll(messages);
      } catch (SdkException e) {
        if (metrics != null) {
          metrics.recordQueueReceiveError(tableLabel, queueLabel, queueType);
          metrics.recordRetryAttempt(tableLabel, queueLabel, queueType);
        }
        if (!e.retryable()) {
          throw e; // TODO: should we really?
        }
        Duration delay = backoff.get();
        logger.error("Failed to pull messages from the SQS queue (retry in {})", delay, e);
        Thread.sleep(delay);
        continue;
      }

      try {
        if (!batch.isEmpty()) {
          var maxBatchSize = 100; // FIXME: make configurable

          List<Message> tailMessages;
          do {
            tailMessages = sqs.receiveMessage(tailReq).messages();
            batch.addAll(tailMessages);
          } while (!tailMessages.isEmpty() && batch.size() < maxBatchSize);

          if (metrics != null) {
            metrics.recordMessagesReceived(tableLabel, queueLabel, queueType, batch.size());
          }

          logger.info("Processing {} message(s)", batch.size());
          // FIXME: handle files not found

          var filtered = filter(batch, matchers, metrics, tableLabel, queueLabel, queueType);

          // These contribute nothing to the next commit, so there is no reason to hold on to
          // them until it happens.
          confirmProcessed(
              sqs,
              sqsQueueURL,
              filtered.unmatchedMessages(),
              metrics,
              tableLabel,
              queueLabel,
              queueType);

          buffer.add(filtered);
        }

        boolean scheduleDue = nextCommitAt != null && !ZonedDateTime.now().isBefore(nextCommitAt);
        String trigger = buffer.flushTrigger(batchOptions, scheduleDue);
        if (trigger == null && terminateAfterOneBatch && !buffer.isEmpty()) {
          trigger = "fire_once";
        }
        if (trigger != null) {
          flush(
              catalog,
              nsTable,
              sqs,
              sqsQueueURL,
              buffer,
              createTableIfNotExists,
              options,
              metrics,
              tableLabel,
              queueLabel,
              queueType,
              trigger);
          if (schedule != null) {
            nextCommitAt = rollForward(schedule, nextCommitAt, ZonedDateTime.now());
            logger.info("Next commit scheduled for: {}", nextCommitAt);
          }
        } else if (!buffer.isEmpty()) {
          keepInvisible(sqs, sqsQueueURL, buffer, nextCommitAt);
          logBufferState(buffer, nextCommitAt);
        } else if (scheduleDue && schedule != null) {
          nextCommitAt = rollForward(schedule, nextCommitAt, ZonedDateTime.now());
        }
        if (metrics != null) {
          metrics.recordBufferState(
              tableLabel, queueLabel, queueType, buffer.fileCount(), buffer.bytes());
        }
      } catch (InterruptedException e) {
        // terminate
        Thread.currentThread().interrupt();
        throw new InterruptedException();
      } catch (Exception e) {
        if (metrics != null) {
          metrics.recordTransactionFailed(tableLabel, queueLabel, queueType);
          metrics.recordRetryAttempt(tableLabel, queueLabel, queueType);
        }
        Duration delay = backoff.get();
        logger.error("Failed to process batch of messages (retry in {})", delay, e);
        Thread.sleep(delay);
        continue;
      }
      resetBackoff.run();
    } while (!terminateAfterOneBatch);
  }

  /**
   * Advances {@code deadline} from itself (not from {@code now}) so that a relative schedule such
   * as {@code every 5 minutes} keeps a fixed cadence instead of sliding forward on every poll.
   */
  private static ZonedDateTime rollForward(Schedule s, ZonedDateTime deadline, ZonedDateTime now) {
    ZonedDateTime r = deadline;
    while (!now.isBefore(r)) {
      r = s.next(r);
    }
    return r;
  }

  /** Commits everything accumulated so far as a single snapshot and acknowledges the messages. */
  private static void flush(
      RESTCatalog catalog,
      TableIdentifier nsTable,
      SqsClient sqs,
      String sqsQueueURL,
      InsertWatchBuffer buffer,
      boolean createTableIfNotExists,
      Insert.Options options,
      InsertWatchMetrics metrics,
      String tableLabel,
      String queueLabel,
      String queueType,
      String trigger)
      throws IOException, InterruptedException {
    String[] files = buffer.fileArray();
    logger.info(
        "Committing {} file(s) ({}) accumulated over {}s (trigger: {})",
        files.length,
        InsertWatchBuffer.formatBytes(buffer.bytes()),
        buffer.age().toSeconds(),
        trigger);
    logger.info("Inserting {}", Arrays.asList(files));

    insert(
        catalog,
        nsTable,
        files,
        createTableIfNotExists,
        options,
        metrics,
        tableLabel,
        queueLabel,
        queueType);

    confirmProcessed(
        sqs, sqsQueueURL, buffer.messageList(), metrics, tableLabel, queueLabel, queueType);

    if (metrics != null) {
      metrics.recordBufferFlush(tableLabel, queueLabel, queueType, trigger);
    }
    buffer.clear();
  }

  private static void insert(
      RESTCatalog catalog,
      TableIdentifier nsTable,
      String[] files,
      boolean createTableIfNotExists,
      Insert.Options options,
      InsertWatchMetrics metrics,
      String tableLabel,
      String queueLabel,
      String queueType)
      throws IOException, InterruptedException {
    try {
      Insert.Result result = Insert.run(catalog, nsTable, files, options);
      if (metrics != null) {
        metrics.recordFilesInserted(tableLabel, queueLabel, queueType, files.length);
        metrics.recordTransactionSuccess(tableLabel, queueLabel, queueType);
      }
      if (!result.ok()) {
        logger.warn(
            "{}/{} file(s) failed to insert in this batch",
            result.totalNumberOfFiles(),
            result.numberOfFilesFailedToInsert());
      }
    } catch (NoSuchTableException e) {
      if (!createTableIfNotExists) {
        if (metrics != null) {
          metrics.recordTransactionFailed(tableLabel, queueLabel, queueType);
        }
        throw e;
      }
      boolean retryInsert = true;
      try {
        CreateTable.run(
            catalog,
            nsTable,
            files[0],
            null,
            true,
            options.useVendedCredentials(),
            options.s3NoSignRequest(),
            null,
            null);
      } catch (NotFoundException nfe) {
        if (!options.ignoreNotFound()) {
          if (metrics != null) {
            metrics.recordTransactionFailed(tableLabel, queueLabel, queueType);
          }
          throw nfe;
        }
        logger.info("Table not created ({} don't exist)", Arrays.asList(files));
        retryInsert = false;
      }
      if (retryInsert) {
        Insert.run(catalog, nsTable, files, options);
        if (metrics != null) {
          metrics.recordFilesInserted(tableLabel, queueLabel, queueType, files.length);
          metrics.recordTransactionSuccess(tableLabel, queueLabel, queueType);
        }
      }
    }
  }

  private static FilterResult filter(
      List<Message> messages,
      Collection<Matcher> matchers,
      InsertWatchMetrics metrics,
      String tableLabel,
      String queueLabel,
      String queueType) {
    Map<String, Long> files = new LinkedHashMap<>();
    List<Message> matched = new ArrayList<>();
    List<Message> unmatched = new ArrayList<>();
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
          metrics.recordMessageParseError(tableLabel, queueLabel, queueType);
        }
        // TODO: dlq?
        unmatched.add(message);
        continue;
      }
      boolean messageMatched = false;
      // TODO: use type
      for (JsonNode record : root.path("Records")) {
        if (metrics != null) {
          metrics.recordEventsReceived(tableLabel, queueLabel, queueType, 1);
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
            files.putIfAbsent(target, record.at("/s3/object/size").asLong(0));
            messageMatched = true;
            if (metrics != null) {
              metrics.recordEventMatched(tableLabel, queueLabel, queueType);
            }
          } else {
            logger.info("Target did not match any input pattern: {}", target);
            if (metrics != null) {
              metrics.recordEventNotMatched(tableLabel, queueLabel, queueType);
            }
          }
        } else {
          if (metrics != null) {
            metrics.recordEventSkipped(tableLabel, queueLabel, queueType);
          }
          if (logger.isTraceEnabled()) {
            logger.trace("Message skipped: {} {}", eventName, target);
          }
        }
      }
      (messageMatched ? matched : unmatched).add(message);
    }
    return new FilterResult(files, matched, unmatched);
  }

  /**
   * Resets the visibility timeout of accumulated messages so that they are not redelivered while
   * waiting for the next commit.
   */
  private static void keepInvisible(
      SqsClient sqs, String sqsQueueURL, InsertWatchBuffer buffer, ZonedDateTime nextCommitAt) {
    int timeout = visibilityTimeoutSeconds(nextCommitAt);
    List<Message> messages = buffer.messageList();
    int len = messages.size();
    for (int i = 0; i < len; i = i + SQS_BATCH_LIMIT) {
      List<Message> chunk = messages.subList(i, Math.min(i + SQS_BATCH_LIMIT, len));
      // A message that stays visible is redelivered and re-accumulated rather than lost, so this
      // is not worth failing the batch over.
      try {
        ChangeMessageVisibilityBatchResponse res =
            changeMessageVisibilityBatch(sqs, sqsQueueURL, chunk, timeout);
        for (BatchResultErrorEntry f : res.failed()) {
          logger.warn("Failed to extend visibility of message#{}: {}", f.id(), f.message());
        }
      } catch (SdkException e) {
        logger.warn("Failed to extend visibility of {} accumulated message(s)", chunk.size(), e);
      }
    }
  }

  private static int visibilityTimeoutSeconds(ZonedDateTime nextCommitAt) {
    if (nextCommitAt == null) {
      return MIN_VISIBILITY_TIMEOUT_SECONDS;
    }
    long secsUntil = Duration.between(ZonedDateTime.now(), nextCommitAt).toSeconds();
    long v = Math.max(MIN_VISIBILITY_TIMEOUT_SECONDS, secsUntil * 2);
    return (int) Math.min(v, MAX_VISIBILITY_TIMEOUT_SECONDS);
  }

  private static void logBufferState(InsertWatchBuffer buffer, ZonedDateTime nextCommitAt) {
    logger.info(
        "Accumulated {} file(s) ({}) over {}s; next commit at {}",
        buffer.fileCount(),
        InsertWatchBuffer.formatBytes(buffer.bytes()),
        buffer.age().toSeconds(),
        nextCommitAt != null ? nextCommitAt : "n/a");
  }

  private static void confirmProcessed(
      SqsClient sqs,
      String sqsQueueURL,
      List<Message> messages,
      InsertWatchMetrics metrics,
      String tableLabel,
      String queueLabel,
      String queueType) {
    if (messages.isEmpty()) {
      return;
    }
    int failedCount = 0;
    int len = messages.size();
    for (int i = 0; i < len; i = i + SQS_BATCH_LIMIT) {
      List<Message> batch = messages.subList(i, Math.min(i + SQS_BATCH_LIMIT, len));
      DeleteMessageBatchResponse res = deleteMessageBatch(sqs, sqsQueueURL, batch);
      if (res.hasFailed()) {
        List<BatchResultErrorEntry> failed = res.failed();
        failedCount += failed.size();
        if (metrics != null) {
          metrics.recordQueueDeleteError(tableLabel, queueLabel, queueType, failed.size());
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

  private static ChangeMessageVisibilityBatchResponse changeMessageVisibilityBatch(
      SqsClient sqs, String sqsQueueURL, List<Message> messages, int visibilityTimeoutSeconds) {
    return sqs.changeMessageVisibilityBatch(
        ChangeMessageVisibilityBatchRequest.builder()
            .queueUrl(sqsQueueURL)
            .entries(
                messages.stream()
                    .map(
                        m ->
                            ChangeMessageVisibilityBatchRequestEntry.builder()
                                .id(m.messageId())
                                .receiptHandle(m.receiptHandle())
                                .visibilityTimeout(visibilityTimeoutSeconds)
                                .build())
                    .toList())
            .build());
  }

  private static SqsClient buildSqsClient(String sqsOverrideEndpoint) {
    SqsClientBuilder builder = SqsClient.builder();

    // Use explicit endpoint if provided (e.g. for LocalStack)
    // Otherwise, AWS SDK will use AWS_ENDPOINT_URL_SQS env var or default AWS endpoints
    if (sqsOverrideEndpoint != null && !sqsOverrideEndpoint.isEmpty()) {
      URI endpoint = URI.create(sqsOverrideEndpoint);
      logger.info("Using custom SQS endpoint: {}", endpoint);
      builder.endpointOverride(endpoint);
    }

    return builder.build();
  }
}
