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

/**
 * Optional per-table etcd commit lock for {@code ice-rest-catalog} when using the etcd metastore.
 *
 * <p>Serializes commits to the same table so concurrent writers do not lose optimistic concurrency
 * races indefinitely.
 */
public record CommitLockConfig(
    @JsonPropertyDescription(
            "Enable etcd mutual-exclusion lock around table commits (etcd backend only; default false)")
        boolean enabled,
    @JsonPropertyDescription(
            "Lease TTL for the lock in seconds (must exceed slow commits; default 30)")
        long leaseTtlSeconds,
    @JsonPropertyDescription("Max time to wait to acquire the lock in milliseconds (default 30000)")
        long acquireTimeoutMs) {

  public CommitLockConfig {
    if (leaseTtlSeconds <= 0) {
      leaseTtlSeconds = 30;
    }
    if (acquireTimeoutMs <= 0) {
      acquireTimeoutMs = 30_000L;
    }
  }

  public static CommitLockConfig defaults() {
    return new CommitLockConfig(false, 30, 30_000L);
  }
}
