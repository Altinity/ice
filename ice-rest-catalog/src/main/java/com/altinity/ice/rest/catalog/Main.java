package com.altinity.ice.rest.catalog;

import com.altinity.ice.rest.catalog.internal.jetty.AuthorizationHandler;
import com.altinity.ice.rest.catalog.internal.jetty.PlainErrorHandler;
import com.altinity.ice.rest.catalog.internal.jetty.ServerConfig;
import com.altinity.ice.rest.catalog.internal.maintenance.QuartzMaintenanceScheduler;
import io.prometheus.metrics.exporter.servlet.jakarta.PrometheusMetricsServlet;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogAuthAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.util.PropertyUtil;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

@CommandLine.Command(
    name = "ice-rest-catalog",
    description = "Iceberg REST Catalog.",
    mixinStandardHelpOptions = true,
    scope = CommandLine.ScopeType.INHERIT,
    versionProvider = Main.VersionProvider.class)
public final class Main implements Callable<Integer> {

  static class VersionProvider implements CommandLine.IVersionProvider {
    public String[] getVersion() {
      return new String[] {Main.class.getPackage().getImplementationVersion()};
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  @CommandLine.Option(
      names = {"-c", "--config"},
      description = "/path/to/config.yaml ($CWD/.ice-rest-catalog.yaml by default)")
  String configFile;

  @CommandLine.Option(
      names = {"--maintenance-interval"},
      description = "Maintenance interval in hours (default: 24)")
  static Long maintenanceInterval;

  @CommandLine.Option(
      names = {"--maintenance-time-unit"},
      description = "Maintenance time unit (HOURS, DAYS, MINUTES) (default: HOURS)")
  static String maintenanceTimeUnit;

  @CommandLine.Option(
      names = {"--maintenance-cron"},
      description = "Maintenance cron expression (overrides interval and time-unit)")
  static String maintenanceCron;

  private Main() {}

  private static Server createServer(int port, Catalog catalog, Map<String, String> config) {
    var s = createBaseServer(catalog, config, true);
    ServerConnector connector = new ServerConnector(s);
    connector.setPort(port);
    s.addConnector(connector);
    return s;
  }

  private static Server createAdminServer(int port, Catalog catalog, Map<String, String> config) {
    var s = createBaseServer(catalog, config, false);
    ServerConnector connector = new ServerConnector(s);
    connector.setHost("localhost");
    connector.setPort(port);
    s.addConnector(connector);
    return s;
  }

  private static Server createBaseServer(
      Catalog catalog, Map<String, String> config, boolean requireAuth) {
    var mux = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    mux.insertHandler(new GzipHandler());
    // TODO: RequestLogHandler
    // TODO: StatisticsHandler
    // TODO: ShutdownHandler

    RESTCatalogAdapter restCatalogAdapter;
    var expectedToken = config.getOrDefault("ice.token", "");
    // TODO: force min length
    if (!expectedToken.isEmpty() || !requireAuth) {
      if (requireAuth) {
        mux.insertHandler(new AuthorizationHandler(expectedToken));
      }
      AwsCredentialsProvider credentialsProvider = null;
      // FIXME: may not always mean AWS
      if (config.getOrDefault("warehouse", "").startsWith("s3://")
          && config.getOrDefault("s3.access-key-id", "").isEmpty()) {
        logger.info("Using software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider");
        credentialsProvider = DefaultCredentialsProvider.create();
      }
      restCatalogAdapter = new RESTCatalogAuthAdapter(catalog, config, credentialsProvider);
    } else {
      restCatalogAdapter = new RESTCatalogAdapter(catalog);
    }

    var h = new ServletHolder(new RESTCatalogServlet(restCatalogAdapter));
    mux.addServlet(h, "/*");

    var s = new Server();
    overrideJettyDefaults(s);
    s.setHandler(mux);
    return s;
  }

  private static Server createDebugServer(int port) {
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

    var s = new Server(port);
    overrideJettyDefaults(s);
    s.setHandler(mux);
    return s;
  }

  private static void overrideJettyDefaults(Server s) {
    ServerConfig.setQuiet(s);
    s.setErrorHandler(new PlainErrorHandler());
  }

  @Override
  public Integer call() throws Exception {
    var config = com.altinity.ice.rest.catalog.internal.config.Config.load(configFile);

    var awsRegion = config.getOrDefault("ice.s3.region", "");
    if (!awsRegion.isEmpty()) {
      System.setProperty("aws.region", awsRegion); // FIXME
    }

    // TODO: use "addr" instead
    int port = PropertyUtil.propertyAsInt(config, "ice.rest.catalog.port", 5000);
    ;
    int debugPort = PropertyUtil.propertyAsInt(config, "ice.rest.catalog.debug.port", 5001);

    // TODO: ensure all http handler are hooked in
    JvmMetrics.builder().register();

    Catalog catalog = CatalogUtil.buildIcebergCatalog("rest_backend", config, null);

    // Initialize and start the maintenance scheduler
    initializeMaintenanceScheduler(catalog, config);

    // TODO: replace with uds (jetty-unixdomain-server is all that is needed here but in ice you'll
    // need to implement custom org.apache.iceberg.rest.RESTClient)
    String adminPort = config.get("ice.admin.port");
    if (adminPort != null && !adminPort.isEmpty()) {
      Server adminServer = createAdminServer(Integer.parseInt(adminPort), catalog, config);
      adminServer.start();
      logger.info("Serving admin endpoint at http://localhost:{}/v1/{config,*}", adminPort);
    }

    Server httpServer = createServer(port, catalog, config);
    httpServer.start();
    logger.info("Serving http://0.0.0.0:{}/v1/{config,*}", port);

    // FIXME: exception here does not terminate the process
    createDebugServer(debugPort).start();
    logger.info("Serving http://0.0.0.0:{}/{metrics,healtz,livez,readyz}", debugPort);

    httpServer.join();
    return 0;
  }

  private static void initializeMaintenanceScheduler(Catalog catalog, Map<String, String> config) {
    try {
      // Create Quartz properties if needed
      Properties quartzProperties = new Properties();
      // Add any custom Quartz properties from config
      for (Map.Entry<String, String> entry : config.entrySet()) {
        if (entry.getKey().startsWith("ice.quartz.")) {
          String quartzKey = entry.getKey().substring("ice.quartz.".length());
          quartzProperties.setProperty(quartzKey, entry.getValue());
        }
      }

      // Create the scheduler
      QuartzMaintenanceScheduler scheduler =
          new QuartzMaintenanceScheduler(catalog, quartzProperties);

      // Configure the schedule
      if (maintenanceCron != null && !maintenanceCron.isEmpty()) {
        // Use cron expression if provided
        scheduler.setMaintenanceSchedule(maintenanceCron);
        logger.info("Maintenance schedule set to cron expression: {}", maintenanceCron);
      } else {
        // Use interval and time unit
        Long interval =
            maintenanceInterval != null
                ? maintenanceInterval
                : PropertyUtil.propertyAsLong(config, "ice.maintenance.interval", 24L);

        String timeUnitStr =
            maintenanceTimeUnit != null
                ? maintenanceTimeUnit
                : config.getOrDefault("ice.maintenance.time-unit", "HOURS");

        try {
          TimeUnit timeUnit = TimeUnit.valueOf(timeUnitStr.toUpperCase());
          scheduler.setMaintenanceSchedule(interval, timeUnit);
          logger.info("Maintenance schedule set to: {} {}", interval, timeUnit);
        } catch (IllegalArgumentException e) {
          logger.warn("Invalid maintenance time unit: {}. Using default: HOURS", timeUnitStr);
          scheduler.setMaintenanceSchedule(interval, TimeUnit.HOURS);
        }
      }

      // Start the scheduler
      scheduler.startScheduledMaintenance();
      logger.info("Maintenance scheduler started");

    } catch (SchedulerException e) {
      logger.error("Failed to initialize maintenance scheduler", e);
    }
  }

  public static void main(String[] args) throws Exception {
    int exitCode = new CommandLine(new Main()).execute(args);
    System.exit(exitCode);
  }
}
