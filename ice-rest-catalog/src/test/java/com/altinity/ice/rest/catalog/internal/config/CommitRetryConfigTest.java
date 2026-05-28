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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.TableProperties;
import org.junit.Test;

public class CommitRetryConfigTest {

  @Test
  public void defaultsMatchIcebergTableProperties() {
    CommitRetryConfig d = CommitRetryConfig.defaults();
    assertThat(d.numRetries()).isEqualTo(TableProperties.COMMIT_NUM_RETRIES_DEFAULT);
    assertThat(d.minWaitMs()).isEqualTo(TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT);
    assertThat(d.maxWaitMs()).isEqualTo(TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT);
    assertThat(d.totalTimeoutMs()).isEqualTo(TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT);
  }

  @Test
  public void zeroOrNegativeFieldsFallBackToDefaults() {
    CommitRetryConfig c = new CommitRetryConfig(0, 0, 0, 0);
    assertThat(c.numRetries()).isEqualTo(TableProperties.COMMIT_NUM_RETRIES_DEFAULT);
    assertThat(c.minWaitMs()).isEqualTo(TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT);
    assertThat(c.maxWaitMs()).isEqualTo(TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT);
    assertThat(c.totalTimeoutMs()).isEqualTo(TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT);
  }

  @Test
  public void explicitPositiveValuesPreserved() {
    CommitRetryConfig c = new CommitRetryConfig(20, 50L, 10_000L, 500_000L);
    assertThat(c.numRetries()).isEqualTo(20);
    assertThat(c.minWaitMs()).isEqualTo(50L);
    assertThat(c.maxWaitMs()).isEqualTo(10_000L);
    assertThat(c.totalTimeoutMs()).isEqualTo(500_000L);
  }
}
