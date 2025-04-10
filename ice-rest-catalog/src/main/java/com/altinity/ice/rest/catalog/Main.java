package com.altinity.ice.rest.catalog;

import io.prometheus.metrics.exporter.servlet.jakarta.PrometheusMetricsServlet;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.io.Files;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogAuthAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.util.PropertyUtil;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
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

  private static final String PREFIX = "ICE_REST_CATALOG_";
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  @CommandLine.Option(
      names = {"-c", "--config"},
      description = "/path/to/config.yaml ($CWD/.ice-rest-catalog.yaml by default)")
  String configFile;

  private Main() {}

  // https://py.iceberg.apache.org/configuration/#setting-configuration-values
  private static Map<String, String> loadConfig(String configFile) throws IOException {
    var p =
        new HashMap<>(
            Map.of(
                CatalogProperties.CATALOG_IMPL,
                org.apache.iceberg.jdbc.JdbcCatalog.class.getName(),
                // org.apache.iceberg.jdbc.JdbcUtil.SCHEMA_VERSION_PROPERTY
                "jdbc.schema-version",
                "V1" // defaults to V0
                ));
    logger.info("config: Defaults set to {}", p);

    boolean defaultConfigFile = Strings.isNullOrEmpty(configFile);
    var file =
        new File(!defaultConfigFile ? configFile : ".ice-rest-catalog.yaml"); // TODO: move out
    try (InputStream in = new FileInputStream(file)) {
      var yaml = new Yaml();
      Map<String, String> config = yaml.load(new BufferedInputStream(in));
      p.putAll(config);
      logger.info("config: Loaded {} from {}", config.keySet(), file);
    } catch (FileNotFoundException e) {
      if (!defaultConfigFile) {
        throw e;
      }
    }

    // Strip ${prefix} from env vars.
    p.putAll(
        new TreeMap<>(System.getenv())
            .entrySet().stream()
                .filter(e -> e.getKey().startsWith(PREFIX))
                .collect(
                    Collectors.toMap(
                        e -> {
                          String k =
                              e.getKey()
                                  .replaceFirst(PREFIX, "")
                                  .replaceAll("__", "-")
                                  .replaceAll("_", ".")
                                  .toLowerCase();
                          logger.info("config: Overriding {} with ${}", k, e.getKey());
                          return k;
                        },
                        Map.Entry::getValue)));

    if (p.getOrDefault("uri", "").toLowerCase().startsWith("jdbc:sqlite:")) {
      // https://github.com/databricks/iceberg-rest-image/issues/39
      p.put(CatalogProperties.CLIENT_POOL_SIZE, "1");
      var uri = URI.create(p.get("uri"));
      String afterScheme = uri.getSchemeSpecificPart();
      String sqliteFilePrefix = "sqlite:file:";
      if (afterScheme.startsWith(sqliteFilePrefix)) {
        var f = URI.create(afterScheme.substring(sqliteFilePrefix.length()));
        if (f.getPath().contains(File.separator)) {
          Files.createParentDirs(new File(f.getPath()));
        }
      }
    }
    if (!p.containsKey("io-impl")
        && p.getOrDefault("warehouse", "").toLowerCase().startsWith("s3://")) {
      p.put(CatalogProperties.FILE_IO_IMPL, org.apache.iceberg.aws.s3.S3FileIO.class.getName());
    }
    if (p.getOrDefault("s3.endpoint", "").toLowerCase().startsWith("http://")) { // TODO: if set?
      p.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
    }

    return p;
  }

  private static Server createServer(int port, Catalog catalog, Map<String, String> config) {
    var mux = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    mux.insertHandler(new GzipHandler());
    // TODO: RequestLogHandler
    // TODO: StatisticsHandler
    // TODO: ShutdownHandler

    RESTCatalogAdapter restCatalogAdapter;

    var expectedToken = config.getOrDefault("ice.token", "");
    // TODO: force min length
    if (!expectedToken.isEmpty()) {
      mux.insertHandler(
          new HandlerWrapper() {

            @Override
            public void handle(
                String target,
                Request baseRequest,
                HttpServletRequest request,
                HttpServletResponse response)
                throws IOException, ServletException {
              var auth = request.getHeader("authorization");
              String prefix = "bearer ";
              String token;
              if (auth != null && auth.toLowerCase().startsWith(prefix)) {
                token = auth.substring(prefix.length());
                if (java.security.MessageDigest.isEqual(
                    token.getBytes(), expectedToken.getBytes())) {
                  super.handle(target, baseRequest, request, response);
                  return;
                }
              } else {
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
                return;
              }
              response.sendError(HttpServletResponse.SC_FORBIDDEN);
              // TODO: AsyncDelayHandler
            }
          });
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

    var s = new Server(port);
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
    Stream.of(s.getConnectors())
        .flatMap(c -> c.getConnectionFactories().stream())
        .filter(cf -> cf instanceof HttpConnectionFactory)
        .forEach(
            cf -> {
              HttpConfiguration hc = ((HttpConnectionFactory) cf).getHttpConfiguration();
              hc.setSendServerVersion(false);
              hc.setSendDateHeader(false);
            });
    s.setErrorHandler(
        new ErrorHandler() {

          @Override
          public void handle(
              String target, Request baseRequest, HttpServletRequest req, HttpServletResponse resp)
              throws IOException, ServletException {
            resp.setStatus(resp.getStatus());
            resp.setContentType(MimeTypes.Type.TEXT_PLAIN.asString());
            resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
            try (PrintWriter w = resp.getWriter()) {
              w.write(baseRequest.getResponse().getReason());
            } finally {
              baseRequest.getHttpChannel().sendResponseAndComplete();
              baseRequest.setHandled(true);
            }
          }
        });
  }

  @Override
  public Integer call() throws Exception {
    var config = loadConfig(configFile);

    var awsRegion = config.getOrDefault("ice.s3.region", "");
    if (!awsRegion.isEmpty()) {
      System.setProperty("aws.region", awsRegion);
    }

    // TODO: use "addr" instead
    int port = PropertyUtil.propertyAsInt(config, "ice.rest.catalog.port", 5000);
    ;
    int debugPort = PropertyUtil.propertyAsInt(config, "ice.rest.catalog.debug.port", 5001);

    JvmMetrics.builder().register();

    Catalog catalog = CatalogUtil.buildIcebergCatalog("rest_backend", config, null);

    Server httpServer = createServer(port, catalog, config);
    httpServer.start();
    logger.info("Serving http://0.0.0.0:{}", port);

    // FIXME: exception here does not terminate the process
    createDebugServer(debugPort).start();
    logger.info("Serving http://0.0.0.0:{}/{metrics,healtz,livez,readyz}", debugPort);

    httpServer.join();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = new CommandLine(new Main()).execute(args);
    System.exit(exitCode);
  }
}
