/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.metrics;

import io.prometheus.metrics.core.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prometheus metrics for the InsertWatch (S3 watch) functionality.
 *
 * <p>This class uses a singleton pattern because Prometheus metrics can only be registered once per
 * JVM.
 */
public class InsertWatchMetrics {

  private static final Logger logger = LoggerFactory.getLogger(InsertWatchMetrics.class);

  private static class Holder {
    private static final InsertWatchMetrics INSTANCE = new InsertWatchMetrics();
  }

  // ==========================================================================
  // Metric Names
  // ==========================================================================

  private static final String LABEL_TABLE = "table";
  private static final String LABEL_QUEUE = "queue";
  private static final String LABEL_QUEUE_TYPE = "queue_type";

  private static final String[] WATCH_LABELS = {LABEL_TABLE, LABEL_QUEUE, LABEL_QUEUE_TYPE};

  // Messages/Files processed
  private static final String MESSAGES_RECEIVED_TOTAL_NAME = "ice_watch_messages_received_total";
  private static final String MESSAGES_RECEIVED_TOTAL_HELP =
      "Total number of SQS messages received";

  private static final String EVENTS_RECEIVED_TOTAL_NAME = "ice_watch_events_received_total";
  private static final String EVENTS_RECEIVED_TOTAL_HELP =
      "Total number of S3 events received (one message may contain multiple events)";

  private static final String EVENTS_MATCHED_TOTAL_NAME = "ice_watch_events_matched_total";
  private static final String EVENTS_MATCHED_TOTAL_HELP =
      "Total number of S3 events that matched the pattern";

  private static final String EVENTS_NOT_MATCHED_TOTAL_NAME = "ice_watch_events_not_matched_total";
  private static final String EVENTS_NOT_MATCHED_TOTAL_HELP =
      "Total number of S3 events that did not match any input pattern";

  private static final String EVENTS_SKIPPED_TOTAL_NAME = "ice_watch_events_skipped_total";
  private static final String EVENTS_SKIPPED_TOTAL_HELP =
      "Total number of S3 events skipped (non-ObjectCreated events)";

  // Files inserted
  private static final String FILES_INSERTED_TOTAL_NAME = "ice_watch_files_inserted_total";
  private static final String FILES_INSERTED_TOTAL_HELP =
      "Total number of files successfully inserted into the catalog";

  // Transactions
  private static final String TRANSACTIONS_TOTAL_NAME = "ice_watch_transactions_total";
  private static final String TRANSACTIONS_TOTAL_HELP =
      "Total number of insert transactions committed";

  private static final String TRANSACTIONS_FAILED_TOTAL_NAME =
      "ice_watch_transactions_failed_total";
  private static final String TRANSACTIONS_FAILED_TOTAL_HELP =
      "Total number of insert transactions that failed";

  // Retry state
  private static final String RETRY_ATTEMPTS_TOTAL_NAME = "ice_watch_retry_attempts_total";
  private static final String RETRY_ATTEMPTS_TOTAL_HELP =
      "Total number of retry attempts due to failures";

  // Queue errors (SQS, Kafka, etc.)
  private static final String QUEUE_RECEIVE_ERRORS_TOTAL_NAME =
      "ice_watch_queue_receive_errors_total";
  private static final String QUEUE_RECEIVE_ERRORS_TOTAL_HELP =
      "Total number of errors when receiving messages from queue";

  private static final String QUEUE_DELETE_ERRORS_TOTAL_NAME =
      "ice_watch_queue_delete_errors_total";
  private static final String QUEUE_DELETE_ERRORS_TOTAL_HELP =
      "Total number of errors when deleting/acknowledging messages from queue";

  // Parse errors
  private static final String MESSAGE_PARSE_ERRORS_TOTAL_NAME =
      "ice_watch_message_parse_errors_total";
  private static final String MESSAGE_PARSE_ERRORS_TOTAL_HELP =
      "Total number of message parsing errors";

  // ==========================================================================
  // Metrics
  // ==========================================================================

  private final Counter messagesReceivedTotal;
  private final Counter eventsReceivedTotal;
  private final Counter eventsMatchedTotal;
  private final Counter eventsNotMatchedTotal;
  private final Counter eventsSkippedTotal;
  private final Counter filesInsertedTotal;
  private final Counter transactionsTotal;
  private final Counter transactionsFailedTotal;
  private final Counter retryAttemptsTotal;
  private final Counter queueReceiveErrorsTotal;
  private final Counter queueDeleteErrorsTotal;
  private final Counter messageParseErrorsTotal;

  /** Returns the singleton instance of the metrics reporter. */
  public static InsertWatchMetrics getInstance() {
    return Holder.INSTANCE;
  }

  private InsertWatchMetrics() {
    this.messagesReceivedTotal =
        Counter.builder()
            .name(MESSAGES_RECEIVED_TOTAL_NAME)
            .help(MESSAGES_RECEIVED_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    this.eventsReceivedTotal =
        Counter.builder()
            .name(EVENTS_RECEIVED_TOTAL_NAME)
            .help(EVENTS_RECEIVED_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    this.eventsMatchedTotal =
        Counter.builder()
            .name(EVENTS_MATCHED_TOTAL_NAME)
            .help(EVENTS_MATCHED_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    this.eventsNotMatchedTotal =
        Counter.builder()
            .name(EVENTS_NOT_MATCHED_TOTAL_NAME)
            .help(EVENTS_NOT_MATCHED_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    this.eventsSkippedTotal =
        Counter.builder()
            .name(EVENTS_SKIPPED_TOTAL_NAME)
            .help(EVENTS_SKIPPED_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    this.filesInsertedTotal =
        Counter.builder()
            .name(FILES_INSERTED_TOTAL_NAME)
            .help(FILES_INSERTED_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    this.transactionsTotal =
        Counter.builder()
            .name(TRANSACTIONS_TOTAL_NAME)
            .help(TRANSACTIONS_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    this.transactionsFailedTotal =
        Counter.builder()
            .name(TRANSACTIONS_FAILED_TOTAL_NAME)
            .help(TRANSACTIONS_FAILED_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    this.retryAttemptsTotal =
        Counter.builder()
            .name(RETRY_ATTEMPTS_TOTAL_NAME)
            .help(RETRY_ATTEMPTS_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    this.queueReceiveErrorsTotal =
        Counter.builder()
            .name(QUEUE_RECEIVE_ERRORS_TOTAL_NAME)
            .help(QUEUE_RECEIVE_ERRORS_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    this.queueDeleteErrorsTotal =
        Counter.builder()
            .name(QUEUE_DELETE_ERRORS_TOTAL_NAME)
            .help(QUEUE_DELETE_ERRORS_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    this.messageParseErrorsTotal =
        Counter.builder()
            .name(MESSAGE_PARSE_ERRORS_TOTAL_NAME)
            .help(MESSAGE_PARSE_ERRORS_TOTAL_HELP)
            .labelNames(WATCH_LABELS)
            .register();

    logger.info("InsertWatch Prometheus metrics initialized");
  }

  public void recordMessagesReceived(String table, String queue, String queueType, int count) {
    messagesReceivedTotal.labelValues(table, queue, queueType).inc(count);
  }

  public void recordEventsReceived(String table, String queue, String queueType, int count) {
    eventsReceivedTotal.labelValues(table, queue, queueType).inc(count);
  }

  public void recordEventMatched(String table, String queue, String queueType) {
    eventsMatchedTotal.labelValues(table, queue, queueType).inc();
  }

  public void recordEventNotMatched(String table, String queue, String queueType) {
    eventsNotMatchedTotal.labelValues(table, queue, queueType).inc();
  }

  public void recordEventSkipped(String table, String queue, String queueType) {
    eventsSkippedTotal.labelValues(table, queue, queueType).inc();
  }

  public void recordFilesInserted(String table, String queue, String queueType, int count) {
    filesInsertedTotal.labelValues(table, queue, queueType).inc(count);
  }

  public void recordTransactionSuccess(String table, String queue, String queueType) {
    transactionsTotal.labelValues(table, queue, queueType).inc();
  }

  public void recordTransactionFailed(String table, String queue, String queueType) {
    transactionsFailedTotal.labelValues(table, queue, queueType).inc();
  }

  public void recordRetryAttempt(String table, String queue, String queueType) {
    retryAttemptsTotal.labelValues(table, queue, queueType).inc();
  }

  public void recordQueueReceiveError(String table, String queue, String queueType) {
    queueReceiveErrorsTotal.labelValues(table, queue, queueType).inc();
  }

  public void recordQueueDeleteError(String table, String queue, String queueType, int count) {
    queueDeleteErrorsTotal.labelValues(table, queue, queueType).inc(count);
  }

  public void recordMessageParseError(String table, String queue, String queueType) {
    messageParseErrorsTotal.labelValues(table, queue, queueType).inc();
  }
}
