/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.altinity.ice.rest.catalog.internal.rest;

import com.altinity.ice.rest.catalog.internal.auth.Session;
import com.altinity.ice.rest.catalog.internal.metrics.HttpMetrics;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTCatalogServlet extends HttpServlet {

  private static final Logger logger = LoggerFactory.getLogger(RESTCatalogServlet.class);

  private static final Map<Class<? extends Exception>, Integer> STATUS_CODE_BY_EXCEPTION =
      ImmutableMap.<Class<? extends Exception>, Integer>builder()
          .put(IllegalArgumentException.class, 400)
          .put(ValidationException.class, 400)
          .put(NamespaceNotEmptyException.class, 400)
          .put(NotAuthorizedException.class, 401)
          .put(ForbiddenException.class, 403)
          .put(NoSuchNamespaceException.class, 404)
          .put(NoSuchTableException.class, 404)
          .put(NoSuchViewException.class, 404)
          .put(NoSuchIcebergTableException.class, 404)
          .put(UnsupportedOperationException.class, 406)
          .put(AlreadyExistsException.class, 409)
          .put(CommitFailedException.class, 409)
          .put(UnprocessableEntityException.class, 422)
          .put(CommitStateUnknownException.class, 500)
          .buildOrThrow();

  private final RESTCatalogHandler restCatalogAdapter;
  private final HttpMetrics httpMetrics;

  public RESTCatalogServlet(RESTCatalogHandler restCatalogAdapter) {
    this.restCatalogAdapter = restCatalogAdapter;
    this.httpMetrics = HttpMetrics.getInstance();
  }

  protected void handle(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    HTTPRequest.HTTPMethod method = HTTPRequest.HTTPMethod.valueOf(request.getMethod());
    String path = request.getRequestURI().substring(1);

    Pair<Route, Map<String, String>> routeContext = Route.from(method, path);
    if (routeContext == null) {
      // Track unknown route requests
      try (var timer = httpMetrics.startRequest(method.name(), "UNKNOWN")) {
        timer.setStatusCode(HttpServletResponse.SC_BAD_REQUEST);
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        var res =
            ErrorResponse.builder()
                .responseCode(400)
                .withType("BadRequestException")
                .withMessage(String.format("No route for %s %s", method, path))
                .build();
        byte[] responseBytes = RESTObjectMapper.mapper().writeValueAsBytes(res);
        timer.setResponseSize(responseBytes.length);
        response.getOutputStream().write(responseBytes);
      }
      return;
    }

    Route route = routeContext.first();

    // Track request with metrics
    try (var timer = httpMetrics.startRequest(method.name(), route.name())) {
      Session session = Session.from(request);
      String userToLog = "";
      if (session != null) {
        userToLog = "@" + session.uid() + " ";
      }
      logger.info("{}{} {}", userToLog, method, path);

      Map<String, String> pathParams = routeContext.second();

      // FIXME: this should be in RESTCatalogAdapter, not here
      Object requestBody = null;
      if (route.requestClass() != null) {
        requestBody =
            RESTObjectMapper.mapper().readValue(request.getReader(), route.requestClass());
      } else if (route == Route.TOKENS) {
        requestBody = RESTUtil.decodeFormData(CharStreams.toString(request.getReader()));
      }

      Map<String, String> queryParams =
          request.getParameterMap().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()[0]));

      Map<String, String> params =
          ImmutableMap.<String, String>builder().putAll(pathParams).putAll(queryParams).build();

      RESTResponse responseBody;
      try {
        responseBody =
            restCatalogAdapter.handle(session, route, params, requestBody, route.responseClass());
      } catch (Exception e) {
        ErrorResponse error =
            ErrorResponse.builder()
                .responseCode(STATUS_CODE_BY_EXCEPTION.getOrDefault(e.getClass(), 500))
                .withType(e.getClass().getSimpleName())
                .withMessage(e.getMessage())
                .build();

        if (error.code() < 500) {
          logger.warn("{}{} {}: {}", userToLog, method, path, e.getMessage());
        } else {
          logger.error("{}{} {}", userToLog, method, path, e);
        }

        timer.setStatusCode(error.code());
        response.setStatus(error.code());
        response.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        byte[] errorBytes = RESTObjectMapper.mapper().writeValueAsBytes(error);
        timer.setResponseSize(errorBytes.length);
        response.getOutputStream().write(errorBytes);
        return;
      }

      timer.setStatusCode(HttpServletResponse.SC_OK);
      response.setStatus(HttpServletResponse.SC_OK);
      response.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
      if (responseBody != null) {
        byte[] responseBytes = RESTObjectMapper.mapper().writeValueAsBytes(responseBody);
        timer.setResponseSize(responseBytes.length);
        response.getOutputStream().write(responseBytes);
      }
    }
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    handle(request, response);
  }

  @Override
  protected void doHead(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    handle(request, response);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    handle(request, response);
  }

  @Override
  protected void doDelete(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    handle(request, response);
  }
}
