/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.metrics;

import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.*;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import java.util.concurrent.TimeUnit;

/**
 * Prometheus metrics for HTTP/REST endpoint monitoring.
 *
 * <p>This class uses a singleton pattern because Prometheus metrics can only be registered once per
 * JVM.
 */
public class HttpMetrics {

  // Singleton instance
  private static volatile HttpMetrics instance;
  private static final Object lock = new Object();

  private final Counter requestsTotal;
  private final Counter responsesTotal;
  private final Histogram requestDuration;
  private final Gauge requestsInFlight;

  /** Returns the singleton instance of the HTTP metrics. */
  public static HttpMetrics getInstance() {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance = new HttpMetrics();
        }
      }
    }
    return instance;
  }

  private HttpMetrics() {
    this.requestsTotal =
        Counter.builder()
            .name(HTTP_REQUESTS_TOTAL_NAME)
            .help(HTTP_REQUESTS_TOTAL_HELP)
            .labelNames(HTTP_REQUEST_LABELS)
            .register();

    this.responsesTotal =
        Counter.builder()
            .name(HTTP_RESPONSES_TOTAL_NAME)
            .help(HTTP_RESPONSES_TOTAL_HELP)
            .labelNames(HTTP_RESPONSE_LABELS)
            .register();

    this.requestDuration =
        Histogram.builder()
            .name(HTTP_REQUEST_DURATION_NAME)
            .help(HTTP_REQUEST_DURATION_HELP)
            .labelNames(HTTP_REQUEST_LABELS)
            .classicUpperBounds(HTTP_DURATION_BUCKETS)
            .register();

    this.requestsInFlight =
        Gauge.builder()
            .name(HTTP_REQUESTS_IN_FLIGHT_NAME)
            .help(HTTP_REQUESTS_IN_FLIGHT_HELP)
            .register();
  }

  /**
   * Record the start of a request. Call this at the beginning of request handling.
   *
   * @param method HTTP method (GET, POST, etc.)
   * @param route Route name (e.g., LOAD_TABLE, LIST_NAMESPACES)
   */
  public void recordRequestStart(String method, String route) {
    requestsTotal.labelValues(method, route).inc();
    requestsInFlight.inc();
  }

  /**
   * Record the completion of a request. Call this at the end of request handling.
   *
   * @param method HTTP method (GET, POST, etc.)
   * @param route Route name (e.g., LOAD_TABLE, LIST_NAMESPACES)
   * @param statusCode HTTP status code (200, 404, 500, etc.)
   * @param startTimeNanos Start time from System.nanoTime()
   */
  public void recordRequestEnd(String method, String route, int statusCode, long startTimeNanos) {
    requestsInFlight.dec();

    // Record duration
    double durationSeconds =
        (System.nanoTime() - startTimeNanos) / (double) TimeUnit.SECONDS.toNanos(1);
    requestDuration.labelValues(method, route).observe(durationSeconds);

    // Record response by status code
    responsesTotal.labelValues(method, route, Integer.toString(statusCode)).inc();
  }

  /**
   * Helper class for timing requests using try-with-resources.
   *
   * <p>Usage:
   *
   * <pre>{@code
   * try (var timer = httpMetrics.startRequest("GET", "LOAD_TABLE")) {
   *   // handle request
   *   timer.setStatusCode(200);
   * }
   * }</pre>
   */
  public RequestTimer startRequest(String method, String route) {
    return new RequestTimer(this, method, route);
  }

  /** Auto-closeable timer for request duration tracking. */
  public static class RequestTimer implements AutoCloseable {
    private final HttpMetrics metrics;
    private final String method;
    private final String route;
    private final long startTimeNanos;
    private int statusCode = 200;

    RequestTimer(HttpMetrics metrics, String method, String route) {
      this.metrics = metrics;
      this.method = method;
      this.route = route;
      this.startTimeNanos = System.nanoTime();
      metrics.recordRequestStart(method, route);
    }

    /** Set the status code before closing. Default is 200. */
    public void setStatusCode(int statusCode) {
      this.statusCode = statusCode;
    }

    @Override
    public void close() {
      metrics.recordRequestEnd(method, route, statusCode, startTimeNanos);
    }
  }
}
