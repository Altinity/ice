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
import com.altinity.ice.rest.catalog.internal.config.Config;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.HandlerWrapper;

public class RESTCatalogAuthorizationHandler extends HandlerWrapper {

  private static final Set<String> READ_ONLY_METHODS =
      new ImmutableSet.Builder<String>().add("GET").add("HEAD").build();

  private final Config.Token[] tokens;
  private final Session anonymousSession;

  public RESTCatalogAuthorizationHandler(Config.Token[] tokens, Session anonymousSession) {
    this.tokens = tokens;
    this.anonymousSession = anonymousSession;
  }

  @Override
  public void handle(
      String target, Request baseRequest, HttpServletRequest req, HttpServletResponse res)
      throws IOException, ServletException {
    var auth = req.getHeader("authorization");
    String prefix = "bearer ";
    if (auth != null && auth.toLowerCase().startsWith(prefix)) {
      var providedToken = auth.substring(prefix.length());
      for (var token : tokens) { // FIXME: slow
        if (java.security.MessageDigest.isEqual(
            providedToken.getBytes(), token.value().getBytes())) {
          Config.AccessConfig o = token.accessConfig();
          Session session = new Session(token.resourceName(), o.readOnly(), o.awsAssumeRoleARN());
          next(target, baseRequest, req, res, session);
          return;
        }
      }
      sendForbidden(baseRequest, res, "Invalid token");
    } else if (anonymousSession != null) {
      next(target, baseRequest, req, res, anonymousSession);
      return;
    }
    sendError(
        baseRequest,
        res,
        HttpServletResponse.SC_UNAUTHORIZED,
        NotAuthorizedException.class.getSimpleName(),
        "Unauthorized");
    // TODO: AsyncDelayHandler
  }

  private void next(
      String target,
      Request baseRequest,
      HttpServletRequest req,
      HttpServletResponse res,
      Session s)
      throws ServletException, IOException {
    if (s.readOnly() && !READ_ONLY_METHODS.contains(req.getMethod())) {
      sendForbidden(baseRequest, res, req.getMethod() + " not allowed");
      return;
    }
    s.applyTo(req);
    super.handle(target, baseRequest, req, res);
  }

  private static void sendForbidden(Request baseRequest, HttpServletResponse res, String message)
      throws IOException {
    sendError(
        baseRequest,
        res,
        HttpServletResponse.SC_FORBIDDEN,
        NotAuthorizedException.class.getSimpleName(),
        message);
  }

  private static void sendError(
      Request baseRequest, HttpServletResponse res, int statusCode, String type, String message)
      throws IOException {
    res.setStatus(statusCode);
    res.setContentType(MimeTypes.Type.APPLICATION_JSON.asString());
    res.setCharacterEncoding(StandardCharsets.UTF_8.name());
    ErrorResponse error =
        ErrorResponse.builder()
            .responseCode(statusCode)
            .withType(type)
            .withMessage(message)
            .build();
    try (PrintWriter w = res.getWriter()) {
      w.write(RESTObjectMapper.mapper().writeValueAsString(error));
    } finally {
      baseRequest.getHttpChannel().sendResponseAndComplete();
    }
  }
}
