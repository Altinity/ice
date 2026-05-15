/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.etcd;

/**
 * Thrown when acquiring the etcd commit lock exceeds the configured timeout. Mapped to HTTP 503 so
 * clients can retry.
 */
public final class CommitLockTimeoutException extends RuntimeException {

  public CommitLockTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
