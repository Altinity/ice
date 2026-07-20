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

import com.altinity.ice.internal.strings.Strings;
import com.altinity.ice.rest.catalog.internal.auth.Session;
import com.altinity.ice.rest.catalog.internal.config.TracingConfig;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.CRC32;
import org.slf4j.MDC;

/**
 * Resolves and propagates per-client / per-request correlation identifiers.
 *
 * <p>{@code clientId} groups every call from a single identity; {@code requestId} is unique per
 * HTTP request. Both are placed in the SLF4J {@link MDC} (rendered by the logback pattern) and
 * echoed back as response headers.
 *
 * <p>Client id resolution:
 *
 * <ul>
 *   <li>If the request carries the client-id header, that value is used. Iceberg Java / PyIceberg
 *       echo it once the server advertises it via the {@code /v1/config} response, so all calls
 *       from that client re-send the same value.
 *   <li>Otherwise a stable fingerprint derived from the auth token, remote address and User-Agent
 *       is used. This covers the {@code /v1/config} request itself (before the header is echoed)
 *       and clients (e.g. ClickHouse) that do not echo config headers.
 * </ul>
 *
 * <p>The advertised id is this same fingerprint (see {@code RESTCatalogMiddlewareTracing}), so it
 * is deterministic per identity and stable across separate client runs, giving identity-level
 * grouping.
 */
public final class RequestTracing {

  public static final String MDC_CLIENT_ID = "clientId";
  public static final String MDC_REQUEST_ID = "requestId";

  private final boolean enabled;
  private final String clientIdHeader;
  private final String requestIdHeader;

  public RequestTracing(TracingConfig config) {
    TracingConfig c = config != null ? config : TracingConfig.defaults();
    this.enabled = c.enabledOrDefault();
    this.clientIdHeader = c.clientIdHeaderOrDefault();
    this.requestIdHeader = c.requestIdHeaderOrDefault();
  }

  public boolean enabled() {
    return enabled;
  }

  public String clientIdHeader() {
    return clientIdHeader;
  }

  /** Client id from the request header, falling back to a stable fingerprint. */
  public String resolveClientId(HttpServletRequest request, Session session) {
    String fromHeader = request.getHeader(clientIdHeader);
    if (!Strings.isNullOrEmpty(fromHeader)) {
      return fromHeader;
    }
    return fingerprint(request, session);
  }

  /** Request id from the request header, falling back to a freshly generated UUID. */
  public String resolveRequestId(HttpServletRequest request) {
    String fromHeader = request.getHeader(requestIdHeader);
    if (!Strings.isNullOrEmpty(fromHeader)) {
      return fromHeader;
    }
    return UUID.randomUUID().toString();
  }

  public void begin(HttpServletResponse response, String clientId, String requestId) {
    MDC.put(MDC_CLIENT_ID, clientId);
    MDC.put(MDC_REQUEST_ID, requestId);
    if (response != null && !response.isCommitted()) {
      response.setHeader(clientIdHeader, clientId);
      response.setHeader(requestIdHeader, requestId);
    }
  }

  public void end() {
    MDC.remove(MDC_CLIENT_ID);
    MDC.remove(MDC_REQUEST_ID);
  }

  private static String fingerprint(HttpServletRequest request, Session session) {
    String uid = session != null ? session.uid() : "anonymous";
    String remote = request.getRemoteAddr();
    String userAgent = request.getHeader("User-Agent");
    String raw =
        (uid != null ? uid : "")
            + "|"
            + (remote != null ? remote : "")
            + "|"
            + (userAgent != null ? userAgent : "");
    CRC32 crc = new CRC32();
    crc.update(raw.getBytes(StandardCharsets.UTF_8));
    return "fp-" + Long.toHexString(crc.getValue());
  }
}
