package com.altinity.ice;

import com.altinity.ice.cmd.Check;
import com.altinity.ice.cmd.CreateTable;
import com.altinity.ice.cmd.Describe;
import com.altinity.ice.cmd.Insert;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import picocli.CommandLine;

// TODO: cmd/* -> CatalogActions
// TODO: move contents in this package to internal/ & Main to separate package
@CommandLine.Command(
    name = "ice",
    description = "Iceberg REST Catalog client.",
    mixinStandardHelpOptions = true,
    scope = CommandLine.ScopeType.INHERIT,
    versionProvider = Main.VersionProvider.class)
public final class Main {

  static class VersionProvider implements CommandLine.IVersionProvider {
    public String[] getVersion() {
      return new String[] {Main.class.getPackage().getImplementationVersion()};
    }
  }

  private static final String PREFIX = "ICE_";
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  @CommandLine.Option(
      names = {"-c", "--config"},
      description = "/path/to/config.yaml ($CWD/.ice.yaml by default)",
      scope = CommandLine.ScopeType.INHERIT)
  String configFile;

  private Main() {}

  // TODO: map<String, map<String, String>>
  // https://py.iceberg.apache.org/configuration/#setting-configuration-values
  private static Map<String, String> loadConfig(String catalogName, String configFile)
      throws IOException {
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

  @CommandLine.Command(name = "check", description = "Check configuration.")
  void check() throws IOException {
    try (RESTCatalog catalog = loadCatalog(this.configFile)) {
      Check.run(catalog);
      System.out.println("OK");
    }
  }

  @CommandLine.Command(name = "describe", description = "Describe catalog/namespace/table.")
  void describe() throws IOException {
    // TODO: check ti has namespace
    // TODO: pyiceberg describe
    try (RESTCatalog catalog = loadCatalog(this.configFile)) {
      Describe.run(catalog);
    }
  }

  // TODO: merge into put?
  @CommandLine.Command(name = "create-table", description = "Create table.")
  void createTable(
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<name>",
              description = "table name (e.g. ns1.table1)")
          String name,
      @CommandLine.Option(
              names = {"-p"},
              description = "create table if not exists")
          boolean createTableIfNotExists,
      @CommandLine.Option(
              arity = "1",
              required = true,
              names = "--schema-from-parquet",
              description = "/path/to/file.parquet")
          String schemaFile)
      throws IOException {
    // TODO: check ti has namespace
    try (RESTCatalog catalog = loadCatalog(this.configFile)) {
      CreateTable.run(catalog, TableIdentifier.parse(name), schemaFile, createTableIfNotExists);
    }
  }

  @CommandLine.Command(name = "insert", description = "Write data to catalog.")
  void insert(
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<name>",
              description = "table name (e.g. ns1.table1)")
          String name,
      @CommandLine.Option(
              names = {"-p", "--create-table"},
              description = "create table if not exists")
          boolean createTableIfNotExists,
      @CommandLine.Parameters(
              arity = "1..*",
              paramLabel = "<files>",
              description = "/path/to/file.parquet")
          String[] dataFiles,
      @CommandLine.Option(
              names = "--no-copy",
              description = "add files to catalog without copying them")
          boolean noCopy,
      @CommandLine.Option(names = "--dry-run", description = "skip transaction commit")
          boolean dryRun // FIXME: no-commit
      ) throws IOException {
    // TODO: "s3a://sasquatch/test/nyc/taxis/yellow_tripdata_2024-12.parquet"
    // FIXME: catalog is supposed to be closed
    try (RESTCatalog catalog = loadCatalog(this.configFile)) {
      TableIdentifier tableId = TableIdentifier.parse(name);
      if (createTableIfNotExists) {
        CreateTable.run(catalog, tableId, dataFiles[0], true);
      }
      if (noCopy) {
        throw new UnsupportedOperationException("--no-copy is not fully tested yet and hence disabled");
      }
      Insert.run(catalog, tableId, dataFiles, noCopy, dryRun);
    }
  }

  private static RESTCatalog loadCatalog(String configFile) throws IOException {
    var catalogName = "default";
    var config = loadConfig(catalogName, configFile);
    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize(catalogName, config);
    return catalog;
  }

  public static void main(String[] args) {
    int exitCode =
        new CommandLine(new Main())
            .setExecutionExceptionHandler(
                (Exception ex, CommandLine self, CommandLine.ParseResult res) -> {
                  logger.error("Failed", ex);
                  return 1;
                })
            .execute(args);
    System.exit(exitCode);
  }
}
