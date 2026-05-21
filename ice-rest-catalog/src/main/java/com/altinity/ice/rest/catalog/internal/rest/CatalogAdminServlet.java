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
import com.altinity.ice.rest.catalog.internal.cmd.CatalogAdminService;
import com.altinity.ice.rest.catalog.internal.cmd.CatalogImportResult;
import com.altinity.ice.rest.catalog.internal.cmd.CatalogSnapshot;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Admin HTTP API for catalog registry export/import (etcd-backed catalogs only). */
public class CatalogAdminServlet extends HttpServlet {

  private static final Logger logger = LoggerFactory.getLogger(CatalogAdminServlet.class);

  private static final String PATH_EXPORT = "admin/v1/catalog-export";
  private static final String PATH_IMPORT = "admin/v1/catalog-import";

  private final Catalog catalog;
  private final String catalogName;

  public CatalogAdminServlet(Catalog catalog, String catalogName) {
    this.catalog = catalog;
    this.catalogName = catalogName;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    handle(request, response);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    handle(request, response);
  }

  private void handle(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String path = normalizePath(request.getRequestURI());
    try {
      if (PATH_EXPORT.equals(path) && "GET".equals(request.getMethod())) {
        handleExport(request, response);
        return;
      }
      if (PATH_IMPORT.equals(path) && "POST".equals(request.getMethod())) {
        handleImport(request, response);
        return;
      }
      writeError(
          response,
          HttpServletResponse.SC_NOT_FOUND,
          "NotFoundException",
          "No route for " + request.getMethod() + " " + path);
    } catch (IllegalArgumentException e) {
      writeError(response, HttpServletResponse.SC_BAD_REQUEST, "BadRequestException", e.getMessage());
    } catch (Exception e) {
      logger.error("{} {}", request.getMethod(), path, e);
      writeError(
          response,
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          e.getClass().getSimpleName(),
          e.getMessage());
    }
  }

  private void handleExport(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String namespace = request.getParameter("namespace");
    CatalogSnapshot snapshot = CatalogAdminService.export(catalog, catalogName, namespace);
    writeJson(response, HttpServletResponse.SC_OK, snapshot);
    logger.info(
        "Exported {} namespace(s) and {} table(s)",
        snapshot.namespaces().size(),
        snapshot.tables().size());
  }

  private void handleImport(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    boolean dryRun = Boolean.parseBoolean(request.getParameter("dry-run"));
    boolean overwrite = Boolean.parseBoolean(request.getParameter("overwrite"));
    String body = CharStreams.toString(request.getReader());
    if (Strings.isNullOrEmpty(body)) {
      throw new IllegalArgumentException("Request body is required");
    }
    CatalogSnapshot snapshot = RESTObjectMapper.mapper().readValue(body, CatalogSnapshot.class);
    CatalogImportResult result =
        CatalogAdminService.importSnapshot(catalog, snapshot, dryRun, overwrite);
    writeJson(response, HttpServletResponse.SC_OK, result);
    logger.info(
        "Import{}: {} created, {} skipped, {} overwritten",
        dryRun ? " (dry-run)" : "",
        result.created(),
        result.skipped(),
        result.overwritten());
  }

  private static String normalizePath(String requestUri) {
    if (requestUri == null || requestUri.isEmpty()) {
      return "";
    }
    return requestUri.startsWith("/") ? requestUri.substring(1) : requestUri;
  }

  private static void writeJson(HttpServletResponse response, int status, Object body)
      throws IOException {
    byte[] bytes = RESTObjectMapper.mapper().writeValueAsBytes(body);
    response.setStatus(status);
    response.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
    response.getOutputStream().write(bytes);
  }

  private static void writeError(
      HttpServletResponse response, int status, String type, String message) throws IOException {
    ErrorResponse error =
        ErrorResponse.builder()
            .responseCode(status)
            .withType(type)
            .withMessage(message)
            .build();
    byte[] bytes = RESTObjectMapper.mapper().writeValueAsBytes(error);
    response.setStatus(status);
    response.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
    response.getOutputStream().write(bytes);
  }
}
