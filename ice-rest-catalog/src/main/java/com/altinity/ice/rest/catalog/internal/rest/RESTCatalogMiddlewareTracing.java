/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.rest;

import com.altinity.ice.rest.catalog.internal.auth.Session;
import java.util.Map;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.slf4j.MDC;

/**
 * Advertises the client id to Iceberg clients via the {@code /v1/config} handshake.
 *
 * <p>The advertised value is the id already resolved for the config request (read from the {@link
 * MDC}) - a deterministic per-identity fingerprint ({@code fp-...}) derived from uid + remote
 * address + user-agent. It is returned as a {@code header.<clientIdHeader>} override. Iceberg Java
 * and PyIceberg merge config overrides and re-send matching {@code header.*} properties on every
 * subsequent request, so all calls from that identity share one {@code clientId} - and because the
 * value is deterministic (not a random per-instance id), it stays stable across separate
 * short-lived client runs. Clients that ignore config overrides (e.g. ClickHouse) fall back to the
 * same server-side fingerprint anyway.
 */
public class RESTCatalogMiddlewareTracing extends RESTCatalogMiddleware {

  private static final String HEADER_PREFIX = "header.";

  private final String clientIdHeader;

  public RESTCatalogMiddlewareTracing(RESTCatalogHandler next, String clientIdHeader) {
    super(next);
    this.clientIdHeader = clientIdHeader;
  }

  @Override
  public <T extends RESTResponse> T handle(
      Session session,
      Route route,
      Map<String, String> vars,
      Object requestBody,
      Class<T> responseType) {
    T restResponse = this.next.handle(session, route, vars, requestBody, responseType);
    if (restResponse instanceof ConfigResponse configResponse) {
      // Advertise the id already resolved for this config request (the stable fingerprint), so
      // echoing clients keep using the same deterministic id across runs.
      String clientId = MDC.get(RequestTracing.MDC_CLIENT_ID);
      if (clientId != null && !clientId.isEmpty()) {
        configResponse.overrides().putIfAbsent(HEADER_PREFIX + clientIdHeader, clientId);
      }
    }
    return restResponse;
  }
}
