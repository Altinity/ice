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

import com.altinity.ice.internal.io.Matcher;
import com.altinity.ice.internal.jetty.DebugServer;
import com.altinity.ice.internal.jetty.PlainErrorHandler;
import com.altinity.ice.internal.jetty.ServerConfig;
import com.altinity.ice.internal.picocli.VersionProvider;
import com.altinity.ice.internal.strings.Strings;
import com.altinity.ice.rest.catalog.internal.auth.Session;
import com.altinity.ice.rest.catalog.internal.aws.CredentialsProvider;
import com.altinity.ice.rest.catalog.internal.config.Config;
import com.altinity.ice.rest.catalog.internal.config.MaintenanceConfig;
import com.altinity.ice.rest.catalog.internal.etcd.EtcdCatalog;
import com.altinity.ice.rest.catalog.internal.maintenance.DataCompaction;
import com.altinity.ice.rest.catalog.internal.maintenance.MaintenanceJob;
import com.altinity.ice.rest.catalog.internal.maintenance.MaintenanceRunner;
import com.altinity.ice.rest.catalog.internal.maintenance.MaintenanceScheduler;
import com.altinity.ice.rest.catalog.internal.maintenance.ManifestCompaction;
import com.altinity.ice.rest.catalog.internal.maintenance.OrphanCleanup;
import com.altinity.ice.rest.catalog.internal.maintenance.SnapshotCleanup;
import com.altinity.ice.rest.catalog.internal.metrics.CatalogMetrics;
import com.altinity.ice.rest.catalog.internal.metrics.PrometheusMetricsReporter;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogAdapter;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogAuthorizationHandler;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogHandler;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogMiddlewareConfig;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogMiddlewareCredentials;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogMiddlewareTableConfig;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogMiddlewareTableCredentials;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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

  // TODO: ice-rest-catalog maintenance list-orphan-files/list-data-compaction-candidates
  @CommandLine.Command(name = "perform-maintenance", description = "Run maintenance job(s).")
  void performMaintenance(
      @CommandLine.Option(
              names = "--job",
              description =
                  "MANIFEST_COMPACTION, DATA_COMPACTION, SNAPSHOT_CLEANUP, ORPHAN_CLEANUP (all by default)")
          MaintenanceConfig.Job[] modes,
      @CommandLine.Option(
              names = "--max-snapshot-age-hours",
              description = "Max snapshot age in hours (5 days by default)")
          Integer maxSnapshotAgeHours,
      @CommandLine.Option(
              names = "--min-snapshots-to-keep",
              description = "Minimum number of snapshots to keep (1 by default)")
          Integer minSnapshotsToKeep,
      @CommandLine.Option(
              names = "--target-file-size-mb",
              description = "The target file size for the table in MB (64 min; 512 by default)")
          Integer targetFileSizeMB,
      @CommandLine.Option(
              names = "--min-input-files",
              description =
                  "The minimum number of files to be compacted if a table partition size is smaller than the target file size (5 by default)")
          Integer minInputFiles,
      @CommandLine.Option(
              names = "--data-compaction-candidate-min-age-in-hours",
              description =
                  "How long to wait before considering file for data compaction (1 day by default; -1 to disable)")
          Integer dataCompactionCandidateMinAgeInHours,
      @CommandLine.Option(
              names = "--orphan-file-retention-period-in-days",
              description =
                  "The number of days to retain orphan files before deleting them (3 by default; -1 to disable)")
          Integer orphanFileRetentionPeriodInDays,
      @CommandLine.Option(
              names = "--orphan-whitelist",
              description = "Orphan whitelist (defaults to */metadata/*, */data/*)")
          String[] orphanWhitelist,
      @CommandLine.Option(names = "--dry-run", description = "Print changes without applying them")
          Boolean dryRun,
      @CommandLine.Option(
              names = "--schedule",
              description =
                  "Maintenance schedule in https://github.com/shyiko/skedule?tab=readme-ov-file#format format, e.g. \"every day 00:00\". Empty schedule means one time run (default)")
          String schedule,
      @CommandLine.Option(
              names = "--debug-addr",
              description = "host:port (0.0.0.0:5001 by default)")
          String debugAddr)
      throws IOException, InterruptedException {
    var config = Config.load(configFile());

    MaintenanceConfig maintenanceConfig = config.maintenance();
    maintenanceConfig =
        new MaintenanceConfig(
            modes != null ? modes : maintenanceConfig.jobs(),
            Objects.requireNonNullElse(
                maxSnapshotAgeHours, maintenanceConfig.maxSnapshotAgeHours()),
            Objects.requireNonNullElse(minSnapshotsToKeep, maintenanceConfig.minSnapshotsToKeep()),
            Objects.requireNonNullElse(targetFileSizeMB, maintenanceConfig.targetFileSizeMB()),
            Objects.requireNonNullElse(minInputFiles, maintenanceConfig.minInputFiles()),
            Objects.requireNonNullElse(
                dataCompactionCandidateMinAgeInHours,
                maintenanceConfig.dataCompactionCandidateMinAgeInHours()),
            Objects.requireNonNullElse(
                orphanFileRetentionPeriodInDays,
                maintenanceConfig.orphanFileRetentionPeriodInDays()),
            Objects.requireNonNullElse(orphanWhitelist, maintenanceConfig.orphanWhitelist()),
            Objects.requireNonNullElse(dryRun, maintenanceConfig.dryRun()));

    var icebergConfig = config.toIcebergConfig();
    logger.debug(
        "Iceberg configuration: {}",
        icebergConfig.entrySet().stream()
            .map(e -> !e.getKey().contains("key") ? e.getKey() + "=" + e.getValue() : e.getKey())
            .sorted()
            .collect(Collectors.joining(", ")));

    var catalog = loadCatalog(config, icebergConfig);

    MaintenanceRunner maintenanceRunner = newMaintenanceRunner(catalog, maintenanceConfig);
    String activeSchedule =
        !Strings.isNullOrEmpty(schedule) ? schedule : config.maintenanceSchedule();
    if (Strings.isNullOrEmpty(activeSchedule)) {
      maintenanceRunner.run();
    } else {
      // TODO: ensure all http handlers are hooked in
      JvmMetrics.builder().register();

      HostAndPort debugHostAndPort =
          HostAndPort.fromString(
              !Strings.isNullOrEmpty(debugAddr) ? debugAddr : config.debugAddr());
      Server debugServer =
          DebugServer.create(debugHostAndPort.getHost(), debugHostAndPort.getPort());
      try {
        debugServer.start();
      } catch (Exception e) {
        throw new RuntimeException(e); // TODO: find a better one
      }
      logger.info("Serving http://{}/{metrics,healtz,livez,readyz}", debugHostAndPort);

      startMaintenanceScheduler(activeSchedule, maintenanceRunner);

      debugServer.join();
    }
  }

  private static void initializeCatalogMetrics(Catalog catalog) {
    try {
      CatalogMetrics metrics = CatalogMetrics.getInstance();
      String catalogName = catalog.name();

      // Count namespaces
      if (catalog instanceof SupportsNamespaces nsCatalog) {
        long namespaceCount = nsCatalog.listNamespaces().size();
        metrics.setNamespacesTotal(catalogName, namespaceCount);
        logger.info("Initialized namespace count: {}", namespaceCount);

        // Count tables across all namespaces
        long tableCount = 0;
        for (Namespace ns : nsCatalog.listNamespaces()) {
          tableCount += catalog.listTables(ns).size();
        }
        metrics.setTablesTotal(catalogName, tableCount);
        logger.info("Initialized table count: {}", tableCount);
      }
    } catch (Exception e) {
      logger.warn("Failed to initialize catalog metrics: {}", e.getMessage());
    }
  }

  private static Server createServer(
      String host,
      int port,
      Catalog catalog,
      Config config,
      Map<String, String> icebergConfig,
      PrometheusMetricsReporter metricsReporter) {
    var s = createBaseServer(catalog, config, icebergConfig, true, metricsReporter);
    ServerConnector connector = new ServerConnector(s);
    connector.setHost(host);
    connector.setPort(port);
    s.addConnector(connector);
    return s;
  }

  private static Server createAdminServer(
      String host,
      int port,
      Catalog catalog,
      Config config,
      Map<String, String> icebergConfig,
      PrometheusMetricsReporter metricsReporter) {
    var s = createBaseServer(catalog, config, icebergConfig, false, metricsReporter);
    ServerConnector connector = new ServerConnector(s);
    connector.setHost(host);
    connector.setPort(port);
    s.addConnector(connector);
    return s;
  }

  private static Server createBaseServer(
      Catalog catalog,
      Config config,
      Map<String, String> icebergConfig,
      boolean requireAuth,
      PrometheusMetricsReporter metricsReporter) {
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
      var globalConfig = config.toIcebergConfigDefaults();
      if (!globalConfig.isEmpty()) {
        restCatalogAdapter = new RESTCatalogMiddlewareConfig(restCatalogAdapter, globalConfig);
      }
      var loadTableConfig = config.toIcebergLoadTableConfig();
      if (!loadTableConfig.isEmpty()) {
        restCatalogAdapter =
            new RESTCatalogMiddlewareTableConfig(restCatalogAdapter, loadTableConfig);
      }

      if (awsAuth) {
        Map<String, AwsCredentialsProvider> awsCredentialsProviders =
            createAwsCredentialsProviders(config.bearerTokens(), config, icebergConfig);
        Function<String, AwsCredentialsProvider> auth = awsCredentialsProviders::get;
        restCatalogAdapter =
            new RESTCatalogMiddlewareTableCredentials(
                new RESTCatalogMiddlewareCredentials(restCatalogAdapter, auth), auth);
      }
    } else {
      restCatalogAdapter = new RESTCatalogAdapter(catalog);
      var globalConfig = config.toIcebergConfigDefaults();
      if (!globalConfig.isEmpty()) {
        restCatalogAdapter = new RESTCatalogMiddlewareConfig(restCatalogAdapter, globalConfig);
      }
      var loadTableConfig = config.toIcebergLoadTableConfig();
      if (!loadTableConfig.isEmpty()) {
        restCatalogAdapter =
            new RESTCatalogMiddlewareTableConfig(restCatalogAdapter, loadTableConfig);
      }

      if (awsAuth) {
        DefaultCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.create();
        Function<String, AwsCredentialsProvider> auth = uid -> awsCredentialsProvider;
        restCatalogAdapter =
            new RESTCatalogMiddlewareTableCredentials(
                new RESTCatalogMiddlewareCredentials(restCatalogAdapter, auth), auth);
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

    var catalog = loadCatalog(config, icebergConfig);

    // Initialize catalog metrics with current counts
    initializeCatalogMetrics(catalog);

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

    // Initialize and start the maintenance scheduler
    if (!Strings.isNullOrEmpty(config.maintenanceSchedule())) {
      MaintenanceRunner maintenanceRunner = newMaintenanceRunner(catalog, config.maintenance());
      startMaintenanceScheduler(config.maintenanceSchedule(), maintenanceRunner);
      // TODO: http endpoint to trigger
    } else {
      logger.info("Catalog maintenance disabled (no maintenance schedule specified)");
    }

    // Initialize Iceberg metrics reporter for Prometheus (singleton)
    PrometheusMetricsReporter metricsReporter = PrometheusMetricsReporter.getInstance();

    // TODO: ensure all http handlers are hooked in
    JvmMetrics.builder().register();

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
              icebergConfig,
              metricsReporter);
      adminServer.start();
      logger.warn("Serving admin endpoint at http://{}/v1/{config,*}", adminHostAndPort);
    }

    HostAndPort hostAndPort = HostAndPort.fromString(config.addr());
    Server httpServer =
        createServer(
            hostAndPort.getHost(),
            hostAndPort.getPort(),
            catalog,
            config,
            icebergConfig,
            metricsReporter);
    httpServer.start();
    logger.info("Serving http://{}/v1/{config,*}", hostAndPort);

    // FIXME: exception here does not terminate the process
    HostAndPort debugHostAndPort = HostAndPort.fromString(config.debugAddr());
    DebugServer.create(debugHostAndPort.getHost(), debugHostAndPort.getPort()).start();
    logger.info("Serving http://{}/{metrics,healtz,livez,readyz}", debugHostAndPort);

    httpServer.join();
    return 0;
  }

  private Catalog loadCatalog(Config config, Map<String, String> icebergConfig) throws IOException {
    // FIXME: remove
    if (config.s3() != null) {
      var awsRegion = config.s3().region();
      if (!awsRegion.isEmpty()) {
        System.setProperty("aws.region", awsRegion);
      }
    }

    String catalogName = config.name();
    String catalogImpl = icebergConfig.get(CatalogProperties.CATALOG_IMPL);
    Catalog catalog;
    if (EtcdCatalog.class.getName().equals(catalogImpl)) {
      catalog = newEctdCatalog(catalogName, icebergConfig);
    } else {
      catalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergConfig, null);
    }
    return catalog;
  }

  private MaintenanceRunner newMaintenanceRunner(Catalog catalog, MaintenanceConfig config) {
    final boolean dryRun = config.dryRun();
    MaintenanceConfig.Job[] jobNames =
        Objects.requireNonNullElse(config.jobs(), MaintenanceConfig.Job.values());
    List<MaintenanceJob> jobs =
        Arrays.stream(jobNames)
            .distinct()
            .map(
                maintenanceMode ->
                    (MaintenanceJob)
                        switch (maintenanceMode) {
                          case DATA_COMPACTION ->
                              new DataCompaction(
                                  config.targetFileSizeMB() * 1024L * 1024L,
                                  config.minInputFiles(),
                                  TimeUnit.HOURS.toMillis(
                                      config.dataCompactionCandidateMinAgeInHours()),
                                  dryRun);
                          case MANIFEST_COMPACTION -> new ManifestCompaction(dryRun);
                          case ORPHAN_CLEANUP ->
                              new OrphanCleanup(
                                  TimeUnit.DAYS.toMillis(config.orphanFileRetentionPeriodInDays()),
                                  Matcher.from(config.orphanWhitelist()),
                                  dryRun);
                          case SNAPSHOT_CLEANUP ->
                              new SnapshotCleanup(
                                  config.maxSnapshotAgeHours(),
                                  config.minSnapshotsToKeep(),
                                  dryRun);
                        })
            .toList();
    return new MaintenanceRunner(catalog, jobs);
  }

  private void startMaintenanceScheduler(String schedule, MaintenanceRunner maintenanceRunner) {
    if (Strings.isNullOrEmpty(schedule)) {
      logger.info("Catalog maintenance disabled (no maintenance schedule specified)");
      return;
    }
    try {
      MaintenanceScheduler scheduler = new MaintenanceScheduler(schedule, maintenanceRunner);
      scheduler.startScheduledMaintenance();
      logger.info("Maintenance scheduler initialized with schedule: {}", schedule);
    } catch (Exception e) {
      logger.error("Failed to initialize maintenance scheduler", e);
      throw new RuntimeException(e);
    }
  }

  private static Catalog newEctdCatalog(String name, Map<String, String> config) {
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
    return new EtcdCatalog(name, Strings.removePrefix(uri, "etcd:"), inputWarehouseLocation, io);
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
