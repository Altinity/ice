package com.altinity.ice;

import ch.qos.logback.classic.Level;
import com.altinity.ice.internal.cmd.Check;
import com.altinity.ice.internal.cmd.CreateTable;
import com.altinity.ice.internal.cmd.DeleteTable;
import com.altinity.ice.internal.cmd.Describe;
import com.altinity.ice.internal.cmd.Insert;
import com.altinity.ice.internal.config.Config;
import com.altinity.ice.internal.iceberg.DataFileNamingStrategy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.AutoComplete;
import picocli.CommandLine;

@CommandLine.Command(
    name = "ice",
    description = "Iceberg REST Catalog client.",
    mixinStandardHelpOptions = true,
    scope = CommandLine.ScopeType.INHERIT,
    versionProvider = Main.VersionProvider.class,
    subcommands = AutoComplete.GenerateCompletion.class)
public final class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  static class VersionProvider implements CommandLine.IVersionProvider {
    public String[] getVersion() {
      return new String[] {Main.class.getPackage().getImplementationVersion()};
    }
  }

  @CommandLine.Option(
      names = {"-c", "--config"},
      description = "/path/to/config.yaml ($CWD/.ice.yaml by default)",
      scope = CommandLine.ScopeType.INHERIT)
  String configFile;

  @CommandLine.Option(
      names = "--log-level",
      defaultValue = "INFO",
      description = "Set the log level (e.g., DEBUG, INFO, WARN, ERROR)",
      scope = CommandLine.ScopeType.INHERIT)
  private String logLevel;

  private Main() {}

  @CommandLine.Command(name = "check", description = "Check configuration.")
  void check() throws IOException {
    try (RESTCatalog catalog = loadCatalog(this.configFile)) {
      Check.run(catalog);
      System.out.println("OK");
    }
  }

  @CommandLine.Command(name = "describe", description = "Describe catalog/namespace/table.")
  void describe(
      @CommandLine.Parameters(
              arity = "0..1",
              paramLabel = "<target>",
              description = "Target (e.g. ns1.table1)")
          String target,
      @CommandLine.Option(
              names = {"--json"},
              description = "Output JSON instead of YAML")
          boolean json,
      @CommandLine.Option(
              names = {"--include-metrics"},
              description = "Include table metrics in the output")
          boolean includeMetrics)
      throws IOException {
    try (RESTCatalog catalog = loadCatalog(this.configFile)) {
      Describe.run(catalog, target, json, includeMetrics);
    }
  }

  @CommandLine.Command(name = "create-table", description = "Create table.")
  void createTable(
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<name>",
              description = "Table name (e.g. ns1.table1)")
          String name,
      @CommandLine.Option(
              names = {"--location"},
              description = "Table location (defaults to $warehouse/$namespace/$table)")
          String location,
      @CommandLine.Option(
              names = {"-p"},
              description = "Create table if not exists")
          boolean createTableIfNotExists,
      @CommandLine.Option(
              names = {"--s3-no-sign-request"},
              description = "Access input file(s) ")
          boolean s3NoSignRequest,
      @CommandLine.Option(
              arity = "1",
              required = true,
              names = "--schema-from-parquet",
              description = "/path/to/file.parquet")
          String schemaFile,
      @CommandLine.Option(
              names = {"--partition-by"},
              description = "Comma-separated list of columns to partition by",
              split = ",")
          List<String> partitionColumns)
      throws IOException {
    try (RESTCatalog catalog = loadCatalog(this.configFile)) {
      CreateTable.run(
          catalog,
          TableIdentifier.parse(name),
          schemaFile,
          location,
          createTableIfNotExists,
          s3NoSignRequest,
          partitionColumns);
    }
  }

  @CommandLine.Command(name = "insert", description = "Write data to catalog.")
  void insert(
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<name>",
              description = "Table name (e.g. ns1.table1)")
          String name,
      @CommandLine.Option(
              names = {"-p", "--create-table"},
              description = "Create table if not exists")
          boolean createTableIfNotExists,
      @CommandLine.Parameters(
              arity = "1..*",
              paramLabel = "<files>",
              description = "/path/to/file.parquet")
          String[] dataFiles,
      @CommandLine.Option(
              names = "--no-copy",
              description = "Add files to catalog without copying them")
          boolean noCopy,
      @CommandLine.Option(
              names = "--force-no-copy",
              description =
                  "Add files to catalog without copying them even if files are in different location(s) from table (implies --no-copy)")
          boolean forceNoCopy,
      @CommandLine.Option(
              names = "--force-table-auth",
              description =
                  "Use table credentials to access input files (instead of credentials from execution environment)")
          boolean forceTableAuth,
      @CommandLine.Option(
              names = {"--s3-no-sign-request"},
              description = "Access input file(s) ")
          boolean s3NoSignRequest,
      @CommandLine.Option(
              names = "--s3-copy-object",
              description =
                  "Avoid download/upload by using https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html for copying S3 objects."
                      + " Note that AWS does not support copying objects anonymously (i.e. you can't use this flag to copy objects from public buckets like https://registry.opendata.aws/aws-public-blockchain/).")
          boolean s3CopyObject,
      @CommandLine.Option(names = "--no-commit", description = "Skip transaction commit")
          boolean noCommit,
      @CommandLine.Option(
              names = "--data-file-naming-strategy",
              description = "Supported: DEFAULT, INPUT_FILENAME",
              defaultValue = "DEFAULT")
          DataFileNamingStrategy.Name dataFileNamingStrategy,
      @CommandLine.Option(names = "--skip-duplicates", description = "Skip duplicates")
          boolean skipDuplicates,
      @CommandLine.Option(
              names = {"--retry-list"},
              description =
                  "/path/to/file where to save list of files to retry"
                      + " (useful for retrying partially failed insert using `cat ice.retry | ice insert - --retry-list=ice.retry`)")
          String retryList)
      throws IOException {
    if (s3NoSignRequest && s3CopyObject) {
      throw new UnsupportedOperationException(
          "--s3-no-sign-request + --s3-copy-object is not supported by AWS (see --help for details)");
    }
    try (RESTCatalog catalog = loadCatalog(this.configFile)) {
      if (dataFiles.length == 1 && "-".equals(dataFiles[0])) {
        dataFiles = readInput().toArray(new String[0]);
        if (dataFiles.length == 0) {
          logger.info("Nothing to insert (stdin empty)");
          return;
        }
      }
      TableIdentifier tableId = TableIdentifier.parse(name);
      if (createTableIfNotExists) {
        // TODO: newCreateTableTransaction
        CreateTable.run(
            catalog, tableId, dataFiles[0], null, createTableIfNotExists, s3NoSignRequest, null);
      }
      Insert.run(
          catalog,
          tableId,
          dataFiles,
          dataFileNamingStrategy,
          skipDuplicates,
          noCommit,
          noCopy,
          forceNoCopy,
          forceTableAuth,
          s3NoSignRequest,
          s3CopyObject,
          retryList);
    }
  }

  private static List<String> readInput() {
    List<String> r = new ArrayList<>();
    try (Scanner scanner = new Scanner(System.in)) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        if (!line.isBlank()) {
          r.add(line);
        }
      }
    }
    return r;
  }

  @CommandLine.Command(name = "delete-table", description = "Delete table.")
  void deleteTable(
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<name>",
              description = "Table name (e.g. ns1.table1)")
          String name,
      @CommandLine.Option(
              names = {"-p"},
              description = "Ignore not found")
          boolean ignoreNotFound)
      throws IOException {
    try (RESTCatalog catalog = loadCatalog(this.configFile)) {
      DeleteTable.run(catalog, TableIdentifier.parse(name), ignoreNotFound);
    }
  }

  private static RESTCatalog loadCatalog(String configFile) throws IOException {
    var catalogName = "default";
    var config = Config.load(catalogName, configFile);
    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize(catalogName, config);
    return catalog;
  }

  public static void main(String[] args) {
    CommandLine cmd = new CommandLine(new Main());
    CommandLine.IExecutionStrategy defaultExecutionStrategy = cmd.getExecutionStrategy();
    cmd.setExecutionStrategy(
        parseResult -> {
          var obj = (Main) parseResult.commandSpec().userObject();
          var logLevel = obj.logLevel;
          ch.qos.logback.classic.Logger rootLogger =
              (ch.qos.logback.classic.Logger)
                  LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
          Level level = Level.toLevel(logLevel.toUpperCase(), Level.INFO);
          rootLogger.setLevel(level);
          return defaultExecutionStrategy.execute(parseResult);
        });
    cmd.setExecutionExceptionHandler(
        (Exception ex, CommandLine self, CommandLine.ParseResult res) -> {
          logger.error("Failed", ex);
          return 1;
        });
    int exitCode = cmd.execute(args);
    System.exit(exitCode);
  }
}
