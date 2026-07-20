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

import com.altinity.ice.internal.strings.Strings;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

/**
 * Per-request / per-client correlation configuration.
 *
 * <p>Adds a stable {@code clientId} (a deterministic per-identity fingerprint grouping all calls
 * from the same uid + remote address + user-agent, including across separate client runs) and a
 * per-request {@code requestId} to logs (via MDC) and response headers, so a whole client's
 * activity can be grouped from the logs alone. All settings are optional; the feature is on by
 * default and has no external dependencies.
 */
public record TracingConfig(
    @JsonPropertyDescription(
            "Enable request/client correlation ids in logs and response headers (true by default)")
        Boolean enabled,
    @JsonPropertyDescription(
            "Header carrying the per-client id echoed by Iceberg clients (X-Ice-Client-Id)")
        String clientIdHeader,
    @JsonPropertyDescription("Header carrying the per-request id (X-Request-Id)")
        String requestIdHeader,
    @JsonPropertyDescription(
            "Advertise the resolved (deterministic fingerprint) client id via the /v1/config response so Iceberg Java/PyIceberg clients echo the same id on every request, keeping it stable across runs (true by default)")
        Boolean advertiseClientId) {

  public static final String DEFAULT_CLIENT_ID_HEADER = "X-Ice-Client-Id";
  public static final String DEFAULT_REQUEST_ID_HEADER = "X-Request-Id";

  public static TracingConfig defaults() {
    return new TracingConfig(null, null, null, null);
  }

  public boolean enabledOrDefault() {
    return enabled == null || enabled;
  }

  public String clientIdHeaderOrDefault() {
    return !Strings.isNullOrEmpty(clientIdHeader) ? clientIdHeader : DEFAULT_CLIENT_ID_HEADER;
  }

  public String requestIdHeaderOrDefault() {
    return !Strings.isNullOrEmpty(requestIdHeader) ? requestIdHeader : DEFAULT_REQUEST_ID_HEADER;
  }

  public boolean advertiseClientIdOrDefault() {
    return advertiseClientId == null || advertiseClientId;
  }
}
