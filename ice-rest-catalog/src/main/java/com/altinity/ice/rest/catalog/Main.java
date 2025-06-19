/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog;

import com.altinity.ice.internal.picocli.VersionProvider;
import com.altinity.ice.internal.strings.Strings;
import com.altinity.ice.rest.catalog.internal.auth.Session;
import com.altinity.ice.rest.catalog.internal.aws.CredentialsProvider;
import com.altinity.ice.rest.catalog.internal.config.Config;
import com.altinity.ice.rest.catalog.internal.etcd.EtcdCatalog;
import com.altinity.ice.rest.catalog.internal.jetty.PlainErrorHandler;
import com.altinity.ice.rest.catalog.internal.jetty.ServerConfig;
import com.altinity.ice.rest.catalog.internal.maintenance.MaintenanceScheduler;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogAdapter;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogAuthorizationHandler;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogHandler;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogMiddlewareTableAWCredentials;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogMiddlewareTableConfig;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import io.prometheus.metrics.exporter.servlet.jakarta.PrometheusMetricsServlet;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import picocli.CommandLine;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

@CommandLine.Command(
  name = "ice-rest-catalog",
  description = "Iceberg REST Catalog.",
  mixinStandardHelpOptions = true,
  scope = CommandLine.ScopeType.INHERIT,
  versionProvider = VersionProvider.class,
  subcommands = com.altinity.ice.cli.Main.class)
public final class Main implements Callable<Integer> {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  @CommandLine.Option(
    names = {"-c", "--config"},
    description = "/path/to/config.yaml ($CWD/.ice-rest-catalog.yaml by default)")
  String configFile;

  public String configFile() {
    if (Strings.isNullOrEmpty(configFile)) {
      return System.getenv("ICE_REST_CATALOG_CONFIG");
    }
    return configFile;
  }

  private Main() {}

  private static Server createServer(
    String host, int port, Catalog catalog, Config config, Map<String, String> icebergConfig) {
    var s = createBaseServer(catalog, config, icebergConfig, true);
    ServerConnector connector = new ServerConnector(s);
    connector.setHost(host);
    connector.setPort(port);
    s.addConnector(connector);
    return s;
  }

  private static Server createAdminServer(
    String host, int port, Catalog catalog, Config config, Map<String, String> icebergConfig) {
    var s = createBaseServer(catalog, config, icebergConfig, false);
    ServerConnector connector = new ServerConnector(s);
    connector.setHost(host);
    connector.setPort(port);
    s.addConnector(connector);
    return s;
  }

  private static Server createBaseServer(
    Catalog catalog, Config config, Map<String, String> icebergConfig, boolean requireAuth) {
    var mux = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    mux.insertHandler(new GzipHandler());
    // TODO: RequestLogHandler
    // TODO: StatisticsHandler
    // TODO: ShutdownHandler

    RESTCatalogHandler restCatalogAdapter;
    String warehouse = icebergConfig.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");
    boolean awsAuth =
      warehouse.startsWith("s3://")
        || warehouse.startsWith("arn:aws:s3tables:"); // FIXME: arn:aws-cn:s3tables
    if (requireAuth) {
      mux.insertHandler(createAuthorizationHandler(config.bearerTokens(), config));

      restCatalogAdapter = new RESTCatalogAdapter(catalog);
      var loadTableConfig = config.toIcebergLoadTableConfig();
      if (!loadTableConfig.isEmpty()) {
        restCatalogAdapter =
          new RESTCatalogMiddlewareTableConfig(restCatalogAdapter, loadTableConfig);
      }

      if (awsAuth) {
        Map<String, AwsCredentialsProvider> awsCredentialsProviders =
          createAwsCredentialsProviders(config.bearerTokens(), config, icebergConfig);
        restCatalogAdapter =
          new RESTCatalogMiddlewareTableAWCredentials(
            restCatalogAdapter, awsCredentialsProviders::get);
      }
    } else {
      restCatalogAdapter = new RESTCatalogAdapter(catalog);
      var loadTableConfig = config.toIcebergLoadTableConfig();
      if (!loadTableConfig.isEmpty()) {
        restCatalogAdapter =
          new RESTCatalogMiddlewareTableConfig(restCatalogAdapter, loadTableConfig);
      }

      if (awsAuth) {
        DefaultCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.create();
        restCatalogAdapter =
          new RESTCatalogMiddlewareTableAWCredentials(
            restCatalogAdapter, uid -> awsCredentialsProvider);
      }
    }

    var h = new ServletHolder(new RESTCatalogServlet(restCatalogAdapter));
    mux.addServlet(h, "/*");

    var s = new Server();
    overrideJettyDefaults(s);
    s.setHandler(mux);
    return s;
  }

  private static Map<String, AwsCredentialsProvider> createAwsCredentialsProviders(
    Config.Token[] tokens, Config config, Map<String, String> icebergConfig) {
    AwsCredentialsProvider awsCredentialsProvider;
    if (icebergConfig.containsKey(S3FileIOProperties.ACCESS_KEY_ID)) {
      if (icebergConfig.containsKey(S3FileIOProperties.SESSION_TOKEN)) {
        awsCredentialsProvider =
          StaticCredentialsProvider.create(
            AwsSessionCredentials.create(
              icebergConfig.get(S3FileIOProperties.ACCESS_KEY_ID),
              icebergConfig.get(S3FileIOProperties.SECRET_ACCESS_KEY),
              icebergConfig.get(S3FileIOProperties.SESSION_TOKEN)));
      } else {
        awsCredentialsProvider =
          StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
              icebergConfig.get(S3FileIOProperties.ACCESS_KEY_ID),
              icebergConfig.get(S3FileIOProperties.SECRET_ACCESS_KEY)));
      }
    } else {
      awsCredentialsProvider = DefaultCredentialsProvider.create();
    }
    Map<String, AwsCredentialsProvider> awsCredentialsProviders = new HashMap<>();
    for (Config.Token token : tokens) {
      awsCredentialsProviders.put(
        token.resourceName(),
        !token.accessConfig().hasAWSAssumeRoleARN()
          ? awsCredentialsProvider
          : CredentialsProvider.assumeRule(
          awsCredentialsProvider,
          token.accessConfig().awsAssumeRoleARN(),
          getUserAgentForToken(token)));
    }
    if (config.anonymousAccess().enabled()) {
      var token = new Config.Token("anonymous", "", config.anonymousAccess().accessConfig());
      awsCredentialsProviders.put(
        token.resourceName(),
        !token.accessConfig().hasAWSAssumeRoleARN()
          ? awsCredentialsProvider
          : CredentialsProvider.assumeRule(
          awsCredentialsProvider,
          token.accessConfig().awsAssumeRoleARN(),
          getUserAgentForToken(token)));
    }
    return awsCredentialsProviders;
  }

  private static String getUserAgentForToken(Config.Token token) {
    return !Strings.isNullOrEmpty(token.name())
      ? "ice-rest-catalog." + token.name()
      : "ice-rest-catalog";
  }

  private static RESTCatalogAuthorizationHandler createAuthorizationHandler(
    Config.Token[] tokens, Config config) {
    Session anonymousSession = null;
    if (config.anonymousAccess().enabled()) {
      var t = new Config.Token("anonymous", "", config.anonymousAccess().accessConfig());
      Config.AccessConfig o = t.accessConfig();
      anonymousSession = new Session(t.resourceName(), o.readOnly(), o.awsAssumeRoleARN());
    }
    if (tokens.length == 0 && anonymousSession == null) {
      throw new IllegalArgumentException(
        "invalid config: either set anonymousAccess.enabled to true or provide tokens via bearerTokens");
    }
    return new RESTCatalogAuthorizationHandler(tokens, anonymousSession);
  }

  private static Server createDebugServer(String host, int port) {
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

  @Override
  public Integer call() throws Exception {
    var config = Config.load(configFile());

    var icebergConfig = config.toIcebergConfig();
    logger.debug(
      "Iceberg configuration: {}",
      icebergConfig.entrySet().stream()
        .map(e -> !e.getKey().contains("key") ? e.getKey() + "=" + e.getValue() : e.getKey())
        .sorted()
        .collect(Collectors.joining(", ")));
    ObjectMapper om = new ObjectMapper();
    for (Config.Token t : config.bearerTokens()) {
      if (Strings.isNullOrEmpty(t.name())) {
        logger.info(
          "Catalog accessible via bearer token named \"{}\" (config: {})",
          Objects.requireNonNullElse(t.name(), ""),
          om.writeValueAsString(t.accessConfig()));
      } else {
        logger.info(
          "Catalog accessible via bearer token (config: {})",
          om.writeValueAsString(t.accessConfig()));
      }
    }
    if (config.anonymousAccess().enabled()) {
      logger.warn(
        "Anonymous access enabled (config: {})",
        om.writeValueAsString(config.anonymousAccess().accessConfig()));
    }

    // FIXME: remove
    if (config.s3() != null) {
      var awsRegion = config.s3().region();
      if (!awsRegion.isEmpty()) {
        System.setProperty("aws.region", awsRegion);
      }
    }

    // TODO: ensure all http handlers are hooked in
    JvmMetrics.builder().register();

    String catalogImpl = icebergConfig.get(CatalogProperties.CATALOG_IMPL);
    Catalog catalog;
    if (EtcdCatalog.class.getName().equals(catalogImpl)) {
      catalog = newEctdCatalog(icebergConfig);
    } else {
      catalog = CatalogUtil.buildIcebergCatalog("default", icebergConfig, null);
    }

    // Initialize and start the maintenance scheduler
    initializeMaintenanceScheduler(catalog, config);

    // TODO: replace with uds (jetty-unixdomain-server is all that is needed here but in ice you'll
    // need to implement custom org.apache.iceberg.rest.RESTClient)
    if (!Strings.isNullOrEmpty(config.adminAddr())) {
      HostAndPort adminHostAndPort = HostAndPort.fromString(config.adminAddr());
      Server adminServer =
        createAdminServer(
          adminHostAndPort.getHost(),
          adminHostAndPort.getPort(),
          catalog,
          config,
          icebergConfig);
      adminServer.start();
      logger.warn("Serving admin endpoint at http://{}/v1/{config,*}", adminHostAndPort);
    }

    HostAndPort hostAndPort = HostAndPort.fromString(config.addr());
    Server httpServer =
      createServer(hostAndPort.getHost(), hostAndPort.getPort(), catalog, config, icebergConfig);
    httpServer.start();
    logger.info("Serving http://{}/v1/{config,*}", hostAndPort);

    // FIXME: exception here does not terminate the process
    HostAndPort debugHostAndPort = HostAndPort.fromString(config.debugAddr());
    createDebugServer(debugHostAndPort.getHost(), debugHostAndPort.getPort()).start();
    logger.info("Serving http://{}/{metrics,healtz,livez,readyz}", debugHostAndPort);

    httpServer.join();
    return 0;
  }

  private void initializeMaintenanceScheduler(Catalog catalog, Config config) {
    if (Strings.isNullOrEmpty(config.maintenanceSchedule())) {
      logger.info("Catalog maintenance disabled (no maintenance schedule specified)");
      return;
    }
    try {
      MaintenanceScheduler scheduler =
        new MaintenanceScheduler(
          catalog, config.maintenanceSchedule(), config.snapshotTTLInDays());
      scheduler.startScheduledMaintenance();
      logger.info(
        "Maintenance scheduler initialized with schedule: {}", config.maintenanceSchedule());
    } catch (Exception e) {
      logger.error("Failed to initialize maintenance scheduler", e);
      throw new RuntimeException(e);
    }
  }

  private static Catalog newEctdCatalog(Map<String, String> config) {
    // TODO: remove; params all verified by config
    String uri = config.getOrDefault(CatalogProperties.URI, "etcd:http://localhost:2379");
    Preconditions.checkArgument(
      !Strings.isNullOrEmpty(uri), "etcd catalog: \"%s\" required", CatalogProperties.URI);

    String inputWarehouseLocation = config.get(CatalogProperties.WAREHOUSE_LOCATION);
    Preconditions.checkArgument(
      !Strings.isNullOrEmpty(inputWarehouseLocation),
      "etcd catalog: \"%s\" required",
      CatalogProperties.WAREHOUSE_LOCATION);

    String ioImpl = config.get(CatalogProperties.FILE_IO_IMPL);
    Preconditions.checkArgument(
      !Strings.isNullOrEmpty(ioImpl),
      "etcd catalog: \"%s\" required",
      CatalogProperties.FILE_IO_IMPL);

    var io = CatalogUtil.loadFileIO(ioImpl, config, null);
    return new EtcdCatalog(
      "default", Strings.removePrefix(uri, "etcd:"), inputWarehouseLocation, io);
  }

  public static void main(String[] args) throws Exception {
    SLF4JBridgeHandler.install();
    CommandLine cmd = new CommandLine(new Main());
    cmd.setExecutionExceptionHandler(
      (Exception ex, CommandLine self, CommandLine.ParseResult res) -> {
        logger.error("Fatal", ex);
        return 1;
      });
    int exitCode = cmd.execute(args);
    System.exit(exitCode);
  }
}
