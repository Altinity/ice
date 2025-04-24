package com.altinity.ice.rest.catalog;

import com.altinity.ice.rest.catalog.internal.auth.Session;
import com.altinity.ice.rest.catalog.internal.auth.Token;
import com.altinity.ice.rest.catalog.internal.aws.CredentialsProvider;
import com.altinity.ice.rest.catalog.internal.config.Config;
import com.altinity.ice.rest.catalog.internal.jetty.PlainErrorHandler;
import com.altinity.ice.rest.catalog.internal.jetty.ServerConfig;
import com.altinity.ice.rest.catalog.internal.maintenance.MaintenanceScheduler;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogAdapter;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogAuthorizationHandler;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogHandler;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogMiddlewareTableAWCredentials;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogMiddlewareTableConfig;
import com.altinity.ice.rest.catalog.internal.rest.RESTCatalogServlet;
import io.prometheus.metrics.exporter.servlet.jakarta.PrometheusMetricsServlet;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.util.PropertyUtil;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
      names = "--maintenance-interval",
      description =
          "Maintenance interval in human-friendly format (e.g. 'every day', 'every monday 09:00'). Leave empty to disable maintenance.")
  private String maintenanceInterval;

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

    RESTCatalogHandler restCatalogAdapter;
    if (requireAuth) {
      Token[] tokens = parseAccessTokens(config);

      mux.insertHandler(createAuthorizationHandler(tokens, config));

      // Fail-fast.
      Map<String, AwsCredentialsProvider> awsCredentialsProviders =
          createAwsCredentialsProviders(tokens, config);

      restCatalogAdapter = new RESTCatalogAdapter(catalog);
      if (config.containsKey(Config.OPTION_TABLE_CONFIG)) {
        restCatalogAdapter = new RESTCatalogMiddlewareTableConfig(restCatalogAdapter, config);
      }
      restCatalogAdapter =
          new RESTCatalogMiddlewareTableAWCredentials(restCatalogAdapter, awsCredentialsProviders);
    } else {
      restCatalogAdapter = new RESTCatalogAdapter(catalog);
      if (config.containsKey(Config.OPTION_TABLE_CONFIG)) {
        restCatalogAdapter = new RESTCatalogMiddlewareTableConfig(restCatalogAdapter, config);
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
      Token[] tokens, Map<String, String> config) {
    AwsCredentialsProvider awsCredentialsProvider;
    if (config.containsKey(S3FileIOProperties.ACCESS_KEY_ID)) {
      if (config.containsKey(S3FileIOProperties.SESSION_TOKEN)) {
        awsCredentialsProvider =
            StaticCredentialsProvider.create(
                AwsSessionCredentials.create(
                    config.get(S3FileIOProperties.ACCESS_KEY_ID),
                    config.get(S3FileIOProperties.SECRET_ACCESS_KEY),
                    config.get(S3FileIOProperties.SESSION_TOKEN)));
      } else {
        awsCredentialsProvider =
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    config.get(S3FileIOProperties.ACCESS_KEY_ID),
                    config.get(S3FileIOProperties.SECRET_ACCESS_KEY)));
      }
    } else {
      awsCredentialsProvider = DefaultCredentialsProvider.create();
    }
    Map<String, AwsCredentialsProvider> awsCredentialsProviders = new HashMap<>();
    for (Token token : tokens) {
      var roleArn = token.params().getOrDefault(Token.TOKEN_PARAM_AWS_ASSUME_ROLE_ARN, "");
      awsCredentialsProviders.put(
          token.resourceName(),
          roleArn.isEmpty()
              ? awsCredentialsProvider
              : CredentialsProvider.assumeRule(
                  awsCredentialsProvider, roleArn, "ice-rest-catalog/" + token.id()));
    }
    if ("true".equals(config.get(Config.OPTION_ANONYMOUS_ACCESS))) {
      var token =
          Token.parse(
              "anonymous::"
                  + Token.TOKEN_PARAM_READ_ONLY
                  + "&"
                  + config.getOrDefault(Config.OPTION_ANONYMOUS_ACCESS_CONFIG, ""));
      var roleArn = token.params().getOrDefault(Token.TOKEN_PARAM_AWS_ASSUME_ROLE_ARN, "");
      awsCredentialsProviders.put(
          token.id(),
          roleArn.isEmpty()
              ? awsCredentialsProvider
              : CredentialsProvider.assumeRule(
                  awsCredentialsProvider, roleArn, "ice-rest-catalog/" + token.id()));
      logger.info("Enabled anonymous access (config: {})", token.params());
    }
    return awsCredentialsProviders;
  }

  private static Token[] parseAccessTokens(Map<String, String> config) {
    var token = config.getOrDefault(Config.OPTION_TOKEN, "");
    if (token.contains(",")) {
      throw new IllegalArgumentException("invalid config: token cannot contain ,");
    }
    var tokens = config.getOrDefault(Config.OPTION_TOKENS, "");
    if (!token.isEmpty()) {
      tokens = token + "," + tokens;
    }
    Token[] r = Arrays.stream(tokens.split(",")).map(Token::parse).toArray(Token[]::new);
    Set<String> set = Arrays.stream(r).map(Token::id).collect(Collectors.toSet());
    if (set.contains("anonymous")) {
      throw new IllegalArgumentException("invalid config: token alias \"anonymous\" is reserved");
    }
    if (set.size() != r.length) {
      throw new IllegalArgumentException(
          "invalid config: multiple tokens with the same alias (name) found");
    }
    for (Token t : r) {
      logger.info("Enabled access via token named \"{}\" (config: {})", t.id(), t.params());
    }
    return r;
  }

  private static RESTCatalogAuthorizationHandler createAuthorizationHandler(
      Token[] tokens, Map<String, String> config) {
    Session anonymousSession = null;
    if ("true".equals(config.get(Config.OPTION_ANONYMOUS_ACCESS))) {
      var t =
          Token.parse(
              "anonymous::"
                  + Token.TOKEN_PARAM_READ_ONLY
                  + "&"
                  + config.getOrDefault(Config.OPTION_ANONYMOUS_ACCESS_CONFIG, ""));
      anonymousSession = new Session(t.id(), t.params());
    }
    if (tokens.length == 0 && anonymousSession == null) {
      throw new IllegalArgumentException(
          String.format(
              "invalid config: either set %s=true or provide tokens via %s or %s",
              Config.OPTION_ANONYMOUS_ACCESS, Config.OPTION_TOKEN, Config.OPTION_TOKENS));
    }
    return new RESTCatalogAuthorizationHandler(tokens, anonymousSession);
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

    // FIXME: remove
    var awsRegion = config.getOrDefault(Config.OPTION_S3_REGION, "");
    if (!awsRegion.isEmpty()) {
      System.setProperty("aws.region", awsRegion);
    }

    // TODO: use "addr" instead
    int port = PropertyUtil.propertyAsInt(config, Config.OPTION_REST_CATALOG_PORT, 5000);
    ;
    int debugPort = PropertyUtil.propertyAsInt(config, Config.OPTION_REST_CATALOG_DEBUG_PORT, 5001);

    // TODO: ensure all http handler are hooked in
    JvmMetrics.builder().register();

    Catalog catalog = CatalogUtil.buildIcebergCatalog("rest_backend", config, null);

    // Initialize and start the maintenance scheduler
    initializeMaintenanceScheduler(catalog, config);

    // TODO: replace with uds (jetty-unixdomain-server is all that is needed here but in ice you'll
    // need to implement custom org.apache.iceberg.rest.RESTClient)
    String adminPort = config.get(Config.OPTION_ADMIN_PORT);
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

  private void initializeMaintenanceScheduler(Catalog catalog, Map<String, String> config) {
    if (maintenanceInterval == null || maintenanceInterval.trim().isEmpty()) {
      logger.info("Maintenance scheduler is disabled (no maintenance interval specified)");
      return;
    }

    try {
      MaintenanceScheduler scheduler =
          new MaintenanceScheduler(catalog, config, maintenanceInterval);
      scheduler.startScheduledMaintenance();
      logger.info("Maintenance scheduler initialized with interval: {}", maintenanceInterval);
    } catch (Exception e) {
      logger.error("Failed to initialize maintenance scheduler", e);
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    int exitCode = new CommandLine(new Main()).execute(args);
    System.exit(exitCode);
  }
}
