package com.altinity.ice.internal.config;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public final class Config {

  private static final String PREFIX = "ICE_";
  private static final Logger logger = LoggerFactory.getLogger(Config.class);

  private Config() {}

  // TODO: map<String, map<String, String>>
  // https://py.iceberg.apache.org/configuration/#setting-configuration-values
  public static Map<String, String> load(String catalogName, String configFile) throws IOException {
    var p = new HashMap<String, String>();
    logger.info("config: Defaults set to {}", p);

    boolean defaultConfigFile = configFile == null || configFile.isEmpty();
    var file = new File(!defaultConfigFile ? configFile : ".ice.yaml"); // TODO: move out
    try (InputStream in = new FileInputStream(file)) {
      var yaml = new Yaml();
      Map<String, Map<String, Map<String, String>>> config = yaml.load(new BufferedInputStream(in));
      Map<String, String> m =
          config.getOrDefault("catalog", Map.of()).getOrDefault(catalogName, Map.of());
      p.putAll(m);
      logger.info("config: Loaded {} from {}", m.keySet(), file);
    } catch (FileNotFoundException e) {
      if (!defaultConfigFile) {
        throw e;
      }
    }

    // Strip ${prefix} from env vars.
    var prefix = String.format("%sCATALOG_%s", PREFIX, catalogName);
    p.putAll(
        new TreeMap<>(System.getenv())
            .entrySet().stream()
                .filter(e -> e.getKey().startsWith(PREFIX))
                .collect(
                    Collectors.toMap(
                        e -> {
                          String k =
                              e.getKey()
                                  .replaceFirst(prefix, "")
                                  .replaceAll("__", "-")
                                  .replaceAll("_", ".")
                                  .toLowerCase();
                          logger.info("config: Overriding {} with ${}", k, e.getKey());
                          return k;
                        },
                        Map.Entry::getValue)));

    if (!p.containsKey("io-impl") && !p.getOrDefault("s3.endpoint", "").isEmpty()) {
      p.put(CatalogProperties.FILE_IO_IMPL, org.apache.iceberg.aws.s3.S3FileIO.class.getName());
    }
    if (p.getOrDefault("s3.endpoint", "").toLowerCase().startsWith("http://")) { // TODO: if set?
      p.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
    }

    return p;
  }
}
