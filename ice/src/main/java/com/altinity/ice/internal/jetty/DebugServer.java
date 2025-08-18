/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.internal.jetty;

import io.prometheus.metrics.exporter.servlet.jakarta.PrometheusMetricsServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class DebugServer {

  public static Server create(String host, int port) {
    var mux = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    mux.insertHandler(new GzipHandler());

    mux.addServlet(new ServletHolder(new PrometheusMetricsServlet()), "/metrics");
    var h =
        new ServletHolder(
            new HttpServlet() {
              @Override
              protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                  throws IOException {
                resp.setStatus(HttpServletResponse.SC_OK);
                resp.setContentType(MimeTypes.Type.TEXT_PLAIN.asString());
                resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
                try (PrintWriter w = resp.getWriter()) {
                  w.write("OK");
                }
              }
            });
    mux.addServlet(h, "/healthz");

    // TODO: provide proper impl
    mux.addServlet(h, "/livez");
    mux.addServlet(h, "/readyz");

    var s = new Server();
    overrideJettyDefaults(s);
    s.setHandler(mux);

    ServerConnector connector = new ServerConnector(s);
    connector.setHost(host);
    connector.setPort(port);
    s.addConnector(connector);

    return s;
  }

  private static void overrideJettyDefaults(Server s) {
    ServerConfig.setQuiet(s);
    s.setErrorHandler(new PlainErrorHandler());
  }
}
