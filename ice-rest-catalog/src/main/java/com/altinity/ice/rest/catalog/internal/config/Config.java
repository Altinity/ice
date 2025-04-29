package com.altinity.ice.rest.catalog.internal.config;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public final class Config {

  private static final String PREFIX = "ICE_REST_CATALOG_";
  private static final Logger logger = LoggerFactory.getLogger(Config.class);

  private Config() {}

  public static final String OPTION_S3_REGION = "ice.s3.region"; // TODO: rename to ic2.aws.region
  public static final String OPTION_REST_CATALOG_PORT = "ice.rest.catalog.port";
  public static final String OPTION_REST_CATALOG_DEBUG_PORT = "ice.rest.catalog.debug.port";
  public static final String OPTION_ADMIN_PORT = "ice.admin.port";
  public static final String OPTION_TABLE_CONFIG = "ice.table.config"; // format: k1=v1&k2=v2,...
  public static final String OPTION_TOKEN = "ice.token";
  public static final String OPTION_TOKENS =
      "ice.tokens"; // format: $alias:$token:aws_role_arn=...&...,$alias:...
  public static final String OPTION_ANONYMOUS_ACCESS = "ice.anonymous.access";
  public static final String OPTION_ANONYMOUS_ACCESS_CONFIG =
      "ice.anonymous.access.config"; // format: param=value&...
  public static final String OPTION_SNAPSHOT_EXPIRATION_DAYS =
      "ice.maintenance.snapshot.expiration.days";

  // TODO: return Config, not Map
  // https://py.iceberg.apache.org/configuration/#setting-configuration-values
  public static Map<String, String> load(String configFile) throws IOException {
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

    BiConsumer<String, String> put =
        (k, v) -> {
          logger.info("config: Activating {}={} default", k, v);
          p.put(k, v);
        };

    if (p.getOrDefault("uri", "").toLowerCase().startsWith("jdbc:sqlite:")) {
      // https://github.com/databricks/iceberg-rest-image/issues/39
      put.accept(CatalogProperties.CLIENT_POOL_SIZE, "1");
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
      put.accept(
          CatalogProperties.FILE_IO_IMPL, org.apache.iceberg.aws.s3.S3FileIO.class.getName());
    }
    if (p.getOrDefault("s3.endpoint", "").toLowerCase().startsWith("http://")) { // TODO: if set?
      put.accept(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
    }

    return p;
  }

  public static Map<String, String> parseQuery(String query) {
    return Arrays.stream(query.split("&"))
        .map(param -> param.split("=", 2))
        .filter(x -> !x[0].isEmpty())
        .collect(
            Collectors.groupingBy(
                pair -> URLDecoder.decode(pair[0], StandardCharsets.UTF_8),
                Collectors.mapping(
                    pair ->
                        pair.length > 1 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "",
                    Collectors.joining(",") // Collectors.toList()
                    )));
  }
}
