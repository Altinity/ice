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

/**
 * Advertises a per-client id to Iceberg clients via the {@code /v1/config} handshake.
 *
 * <p>The id is returned as a {@code header.<clientIdHeader>} override. Iceberg Java and PyIceberg
 * merge config overrides and re-send matching {@code header.*} properties on every subsequent
 * request, so all calls from that client instance share one {@code clientId}. Clients that ignore
 * config overrides (e.g. ClickHouse) simply fall back to the server-side fingerprint.
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
      configResponse
          .overrides()
          .putIfAbsent(HEADER_PREFIX + clientIdHeader, RequestTracing.newClientId());
    }
    return restResponse;
  }
}
