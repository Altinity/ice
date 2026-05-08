/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.config;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.apache.iceberg.TableProperties;

/**
 * Server-side tuning for the REST catalog commit retry loop (OCC compare-and-swap failures).
 *
 * <p>Defaults match Iceberg's {@link TableProperties} commit retry defaults.
 */
public record CommitRetryConfig(
    @JsonPropertyDescription(
            "Number of retries on CommitFailedException (default: Iceberg commit.retry.num-retries = 4)")
        int numRetries,
    @JsonPropertyDescription(
            "Minimum backoff between retries in ms (default: Iceberg commit.retry.min-wait-ms)")
        long minWaitMs,
    @JsonPropertyDescription(
            "Maximum backoff between retries in ms (default: Iceberg commit.retry.max-wait-ms)")
        long maxWaitMs,
    @JsonPropertyDescription(
            "Total time budget for the retry loop in ms (default: Iceberg commit.retry.total-timeout-ms)")
        long totalTimeoutMs) {

  public CommitRetryConfig {
    if (numRetries <= 0) {
      numRetries = TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
    }
    if (minWaitMs <= 0) {
      minWaitMs = TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
    }
    if (maxWaitMs <= 0) {
      maxWaitMs = TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
    }
    if (totalTimeoutMs <= 0) {
      totalTimeoutMs = TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;
    }
  }

  public static CommitRetryConfig defaults() {
    return new CommitRetryConfig(
        TableProperties.COMMIT_NUM_RETRIES_DEFAULT,
        TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
        TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
        TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT);
  }
}
