/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli;

import ch.qos.logback.classic.Level;
import com.altinity.ice.cli.internal.cmd.AlterTable;
import com.altinity.ice.cli.internal.cmd.Check;
import com.altinity.ice.cli.internal.cmd.CreateNamespace;
import com.altinity.ice.cli.internal.cmd.CreateTable;
import com.altinity.ice.cli.internal.cmd.Delete;
import com.altinity.ice.cli.internal.cmd.DeleteNamespace;
import com.altinity.ice.cli.internal.cmd.DeleteTable;
import com.altinity.ice.cli.internal.cmd.Describe;
import com.altinity.ice.cli.internal.cmd.DescribeParquet;
import com.altinity.ice.cli.internal.cmd.Insert;
import com.altinity.ice.cli.internal.cmd.InsertWatch;
import com.altinity.ice.cli.internal.cmd.Scan;
import com.altinity.ice.cli.internal.config.Config;
import com.altinity.ice.cli.internal.iceberg.rest.RESTCatalogFactory;
import com.altinity.ice.internal.jetty.DebugServer;
import com.altinity.ice.internal.picocli.VersionProvider;
import com.altinity.ice.internal.strings.Strings;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.AutoComplete;
import picocli.CommandLine;

@CommandLine.Command(
    name = "ice",
    description = "Iceberg REST Catalog client.",
    mixinStandardHelpOptions = true,
    scope = CommandLine.ScopeType.INHERIT,
    versionProvider = VersionProvider.class,
    subcommands = AutoComplete.GenerateCompletion.class)
public final class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  @CommandLine.Option(
      names = {"-c", "--config"},
      description = "/path/to/config.yaml ($CWD/.ice.yaml by default)",
      scope = CommandLine.ScopeType.INHERIT)
  private String configFile;

  public String configFile() {
    if (Strings.isNullOrEmpty(configFile)) {
      return System.getenv("ICE_CONFIG");
    }
    return configFile;
  }

  @CommandLine.Option(
      names = "--log-level",
      defaultValue = "INFO",
      description = "Set the log level (e.g., DEBUG, INFO, WARN, ERROR)",
      scope = CommandLine.ScopeType.INHERIT)
  private String logLevel;

  @CommandLine.Option(
      names = {"--insecure", "--ssl-no-verify"},
      description =
          "Skip SSL certificate verification (WARNING: insecure, use only for development)",
      scope = CommandLine.ScopeType.INHERIT)
  private boolean insecure;

  private Main() {}

  @CommandLine.Command(name = "check", description = "Check configuration.")
  void check() throws IOException {
    try (RESTCatalog catalog = loadCatalog()) {
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
              names = {"-s"},
              description = "Include schema")
          boolean includeSchema,
      @CommandLine.Option(
              names = {"-p"},
              description = "Include properties")
          boolean includeProperties,
      @CommandLine.Option(
              names = {"-m"},
              description = "Include metrics")
          boolean includeMetrics,
      @CommandLine.Option(
              names = {"-a"},
              description = "Include all")
          boolean includeAll,
      @CommandLine.Option(
              names = {"--json"},
              description = "Output JSON instead of YAML")
          boolean json)
      throws IOException {
    try (RESTCatalog catalog = loadCatalog()) {
      var options = new HashSet<Describe.Option>();
      if (includeSchema || includeAll) {
        options.add(Describe.Option.INCLUDE_SCHEMA);
      }
      if (includeProperties || includeAll) {
        options.add(Describe.Option.INCLUDE_PROPERTIES);
      }
      if (includeMetrics || includeAll) {
        options.add(Describe.Option.INCLUDE_METRICS);
      }
      Describe.run(catalog, target, json, options.toArray(new Describe.Option[0]));
    }
  }

  @CommandLine.Command(name = "describe-parquet", description = "Describe parquet file metadata.")
  void describeParquet(
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<target>",
              description = "Path to parquet file")
          String target,
      @CommandLine.Option(
              names = {"-a", "--all"},
              description = "Show everything")
          boolean showAll,
      @CommandLine.Option(
              names = {"-s", "--summary"},
              description = "Show size, rows, number of row groups, size, compress_size, etc.")
          boolean showSummary,
      @CommandLine.Option(
              names = {"--columns"},
              description = "Show columns")
          boolean showColumns,
      @CommandLine.Option(
              names = {"-r", "--row-groups"},
              description = "Show row groups")
          boolean showRowGroups,
      @CommandLine.Option(
              names = {"-d", "--row-group-details"},
              description = "Show column stats within row group")
          boolean showRowGroupDetails,
      @CommandLine.Option(
              names = {"--json"},
              description = "Output JSON instead of YAML")
          boolean json,
      @CommandLine.Option(names = {"--s3-region"}) String s3Region,
      @CommandLine.Option(
              names = {"--s3-no-sign-request"},
              description = "Access S3 files without authentication")
          boolean s3NoSignRequest)
      throws IOException {
    setAWSRegion(s3Region);
    try (RESTCatalog catalog = loadCatalog()) {
      var options = new ArrayList<DescribeParquet.Option>();
      if (showAll || showSummary) {
        options.add(DescribeParquet.Option.SUMMARY);
      }
      if (showAll || showColumns) {
        options.add(DescribeParquet.Option.COLUMNS);
      }
      if (showAll || showRowGroups) {
        options.add(DescribeParquet.Option.ROW_GROUPS);
      }
      if (showAll || showRowGroupDetails) {
        options.add(DescribeParquet.Option.ROW_GROUP_DETAILS);
      }

      if (options.isEmpty()) {
        options.add(DescribeParquet.Option.SUMMARY);
      }

      DescribeParquet.run(
          catalog, target, json, s3NoSignRequest, options.toArray(new DescribeParquet.Option[0]));
    }
  }

  public record IceSortOrder(
      @JsonProperty("column") String column,
      @JsonProperty("desc") boolean desc,
      @JsonProperty("nullFirst") boolean nullFirst) {}

  public record IcePartition(
      @JsonProperty("column") String column, @JsonProperty("transform") String transform) {}

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
              names = "--use-vended-credentials",
              description =
                  "Use vended credentials to access input files (instead of credentials from execution environment)")
          boolean useVendedCredentials,
      @CommandLine.Option(names = {"--s3-region"}) String s3Region,
      @CommandLine.Option(
              names = {"--s3-no-sign-request"},
              description = "Access input file(s)")
          boolean s3NoSignRequest,
      @CommandLine.Option(
              arity = "1",
              required = true,
              names = "--schema-from-parquet",
              description = "/path/to/file.parquet")
          String schemaFile,
      @CommandLine.Option(
              names = {"--partition"},
              description =
                  "Partition spec, e.g. [{\"column\":\"name\", \"transform\":\"identity\"}],"
                      + "Supported transformations: \"hour\", \"day\", \"month\", \"year\", \"identity\" (default)")
          String partitionJson,
      @CommandLine.Option(
              names = {"--sort"},
              description =
                  "Sort order, e.g. [{\"column\":\"name\", \"desc\":false, \"nullFirst\":false}]")
          String sortOrderJson)
      throws IOException {
    setAWSRegion(s3Region);
    try (RESTCatalog catalog = loadCatalog()) {
      List<IceSortOrder> sortOrders = new ArrayList<>();
      List<IcePartition> partitions = new ArrayList<>();

      if (sortOrderJson != null && !sortOrderJson.isEmpty()) {
        ObjectMapper mapper = newObjectMapper();
        IceSortOrder[] orders = mapper.readValue(sortOrderJson, IceSortOrder[].class);
        sortOrders = Arrays.asList(orders);
      }

      if (partitionJson != null && !partitionJson.isEmpty()) {
        ObjectMapper mapper = newObjectMapper();
        IcePartition[] parts = mapper.readValue(partitionJson, IcePartition[].class);
        partitions = Arrays.asList(parts);
      }

      CreateTable.run(
          catalog,
          TableIdentifier.parse(name),
          schemaFile,
          location,
          createTableIfNotExists,
          useVendedCredentials,
          s3NoSignRequest,
          partitions,
          sortOrders);
    }
  }

  @CommandLine.Command(name = "alter-table", description = "Alter table.")
  void alterTable(
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<name>",
              description = "Table name (e.g. ns1.table1)")
          String name,
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<updates>",
              description =
                  """
                  List of table modifications,
                  e.g. [{"op":"drop_column","name":"foo"}]

                  Supported operations:
                    - add_column           (params: "name", "type" (https://iceberg.apache.org/spec/#primitive-types), "doc" (optional))
                    - alter_column         (params: "name", "type" (https://iceberg.apache.org/spec/#primitive-types))
                    - rename_column        (params: "name", "new_name")
                    - drop_column          (params: "name")
                    - set_tblproperty      (params: "key", "value" (set to null to remove table property))
                    - rename_to            (params: "new_name")
                    - drop_partition_field (params: "name")
                  """)
          String updatesJson)
      throws IOException {
    try (RESTCatalog catalog = loadCatalog(this.configFile())) {
      TableIdentifier tableId = TableIdentifier.parse(name);

      ObjectMapper mapper = newObjectMapper();
      AlterTable.Update[] updates = mapper.readValue(updatesJson, AlterTable.Update[].class);
      AlterTable.run(catalog, tableId, Arrays.stream(updates).toList());
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
              arity = "0..*",
              paramLabel = "<files>",
              description = "/path/to/file.parquet")
          String[] files,
      @CommandLine.Option(
              names = "--files-from",
              description =
                  "Read list of parquet files from the specified file (one file per line)")
          String filesFrom,
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
              names = {"--use-vended-credentials", "--force-table-auth" /* deprecated */},
              description =
                  "Use table credentials to access input files (instead of credentials from execution environment)")
          boolean useVendedCredentials,
      @CommandLine.Option(names = {"--s3-region"}) String s3Region,
      @CommandLine.Option(
              names = {"--s3-no-sign-request"},
              description = "Access input file(s)")
          boolean s3NoSignRequest,
      @CommandLine.Option(
              names = "--s3-copy-object",
              description =
                  "Avoid download/upload by using https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html or https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html for copying S3 objects."
                      + " Note that AWS does not support copying objects anonymously (i.e. you can't use this flag to copy objects from public buckets like https://registry.opendata.aws/aws-public-blockchain/).")
          boolean s3CopyObject,
      @CommandLine.Option(
              names = "--s3-multipart-upload-thread-count",
              description = "Number of threads to use for uploading multiparts.",
              defaultValue = "4")
          int s3MultipartUploadThreadCount,
      @CommandLine.Option(names = "--no-commit", description = "Skip transaction commit")
          boolean noCommit,
      @CommandLine.Option(
              names = "--data-file-naming-strategy",
              description = "Supported: DEFAULT, PRESERVE_ORIGINAL",
              defaultValue = "DEFAULT")
          Insert.DataFileNamingStrategy.Name dataFileNamingStrategy,
      @CommandLine.Option(names = "--skip-duplicates", description = "Skip duplicates")
          boolean skipDuplicates,
      @CommandLine.Option(names = "--force-duplicates", description = "Force insert duplicates")
          boolean forceDuplicates,
      @CommandLine.Option(
              names = {"--retry-list"},
              description =
                  "/path/to/file where to save list of files to retry"
                      + " (useful for retrying partially failed insert using `cat ice.retry | ice insert - --retry-list=ice.retry`)")
          String retryList,
      @CommandLine.Option(
              names = {"--retry-list-exit-code"},
              description =
                  "Exit code to return when insert produces non-empty --retry-list file (default: 0)")
          int retryListExitCode,
      @CommandLine.Option(
              names = {"--partition"},
              description =
                  "Partition spec, e.g. [{\"column\":\"name\", \"transform\":\"identity\"}],"
                      + "Supported transformations: \"hour\", \"day\", \"month\", \"year\", \"identity\" (default)")
          String partitionJson,
      @CommandLine.Option(
              names = {"--sort"},
              description =
                  "Sort order, e.g. [{\"column\":\"name\", \"desc\":false, \"nullFirst\":false}]")
          String sortOrderJson,
      @CommandLine.Option(
              names = {"--assume-sorted"},
              description = "Skip data sorting. Assume it's already sorted.")
          boolean assumeSorted,
      @CommandLine.Option(
              names = {"--thread-count"},
              description = "Number of threads to use for inserting data",
              defaultValue = "-1")
          int threadCount,
      @CommandLine.Option(
              names = {"--watch"},
              description = "Event queue. Supported: AWS SQS")
          String watch,
      @CommandLine.Option(
              names = {"--watch-fire-once"},
              description = "")
          boolean watchFireOnce,
      @CommandLine.Option(
              names = {"--watch-debug-addr"},
              description = "")
          String watchDebugAddr)
      throws IOException, InterruptedException {
    if (s3NoSignRequest && s3CopyObject) {
      throw new UnsupportedOperationException(
          "--s3-no-sign-request + --s3-copy-object is not supported by AWS (see --help for details)");
    }
    boolean filesSet = files != null && files.length > 0;
    boolean filesFromSet = !Strings.isNullOrEmpty(filesFrom);
    if (!filesSet && !filesFromSet) {
      throw new IllegalArgumentException(
          "At least one <files> argument or --files-from is required");
    }
    if (filesSet && filesFromSet) {
      throw new IllegalArgumentException(
          "<files> arguments and --files-from are mutually exclusive");
    }
    setAWSRegion(s3Region);
    try (RESTCatalog catalog = loadCatalog()) {
      if (filesFromSet) {
        files = readInputFromFile(filesFrom).toArray(new String[0]);
        if (files.length == 0) {
          logger.info("Nothing to insert (file empty)");
          return;
        }
      } else if (files.length == 1 && "-".equals(files[0])) {
        files = readInput().toArray(new String[0]);
        if (files.length == 0) {
          logger.info("Nothing to insert (stdin empty)");
          return;
        }
      }

      List<IcePartition> partitions = null;
      if (partitionJson != null && !partitionJson.isEmpty()) {
        ObjectMapper mapper = newObjectMapper();
        IcePartition[] parts = mapper.readValue(partitionJson, IcePartition[].class);
        partitions = Arrays.asList(parts);
      }

      List<IceSortOrder> sortOrders = null;
      if (sortOrderJson != null && !sortOrderJson.isEmpty()) {
        ObjectMapper mapper = newObjectMapper();
        IceSortOrder[] orders = mapper.readValue(sortOrderJson, IceSortOrder[].class);
        sortOrders = Arrays.asList(orders);
      }

      TableIdentifier tableId = TableIdentifier.parse(name);
      boolean watchMode = !Strings.isNullOrEmpty(watch);

      if (createTableIfNotExists && !watchMode) {
        CreateTable.run(
            catalog,
            tableId,
            files[0],
            null,
            createTableIfNotExists,
            useVendedCredentials,
            s3NoSignRequest,
            partitions,
            sortOrders);
      } // delayed in watch mode

      Insert.Options options =
          Insert.Options.builder()
              .dataFileNamingStrategy(dataFileNamingStrategy)
              .skipDuplicates(skipDuplicates)
              .forceDuplicates(forceDuplicates)
              .noCommit(noCommit)
              .noCopy(noCopy)
              .forceNoCopy(forceNoCopy)
              .useVendedCredentials(useVendedCredentials)
              .s3NoSignRequest(s3NoSignRequest)
              .s3CopyObject(s3CopyObject)
              .s3MultipartUploadThreadCount(s3MultipartUploadThreadCount)
              .assumeSorted(assumeSorted)
              .ignoreNotFound(watchMode)
              .retryListFile(retryList)
              .partitionList(partitions)
              .sortOrderList(sortOrders)
              .threadCount(
                  threadCount < 1 ? Runtime.getRuntime().availableProcessors() : threadCount)
              .build();

      if (!watchMode) {
        Insert.Result result = Insert.run(catalog, tableId, files, options);
        if (!result.ok()) {
          logger.error(
              "{}/{} file(s) failed to insert (see {})",
              result.totalNumberOfFiles(),
              result.numberOfFilesFailedToInsert(),
              retryList);
          System.exit(retryListExitCode);
        }
      } else {
        if (!Strings.isNullOrEmpty(watchDebugAddr)) {
          JvmMetrics.builder().register();

          HostAndPort debugHostAndPort = HostAndPort.fromString(watchDebugAddr);
          Server debugServer =
              DebugServer.create(debugHostAndPort.getHost(), debugHostAndPort.getPort());
          try {
            debugServer.start();
          } catch (Exception e) {
            throw new RuntimeException(e); // TODO: find a better one
          }
          logger.info("Serving http://{}/{metrics,healtz,livez,readyz}", debugHostAndPort);
        }

        InsertWatch.run(
            catalog, tableId, files, watch, watchFireOnce, createTableIfNotExists, options);
      }
    }
  }

  // FIXME: do not modify system properties, configure aws sdk instead
  private static void setAWSRegion(String v) {
    if (!Strings.isNullOrEmpty(v)) {
      System.setProperty("aws.region", v);
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

  private static List<String> readInputFromFile(String filePath) throws IOException {
    List<String> r = new ArrayList<>();
    try (Scanner scanner = new Scanner(new java.io.File(filePath))) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        if (!line.isBlank()) {
          r.add(line);
        }
      }
    }
    return r;
  }

  @CommandLine.Command(name = "scan", description = "Scan table.")
  void scanTable(
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<name>",
              description = "Table name (e.g. ns1.table1)")
          String name,
      @CommandLine.Option(
              names = {"--limit"},
              description = "Number of rows to print",
              defaultValue = "10")
          int limit,
      @CommandLine.Option(
              names = {"--json"},
              description = "Output JSON instead of YAML")
          boolean json)
      throws IOException {
    try (RESTCatalog catalog = loadCatalog()) {
      Scan.run(catalog, TableIdentifier.parse(name), limit, json);
    }
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
          boolean ignoreNotFound,
      @CommandLine.Option(
              names = {"--purge"},
              description = "Delete data")
          boolean purge)
      throws IOException {
    try (RESTCatalog catalog = loadCatalog()) {
      DeleteTable.run(catalog, TableIdentifier.parse(name), ignoreNotFound, purge);
    }
  }

  @CommandLine.Command(name = "create-namespace", description = "Create namespace.")
  void createNamespace(
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<name>",
              description = "Namespace name (e.g. parent_ns.child_ns)")
          String name,
      @CommandLine.Option(
              names = {"-p"},
              description = "Create namespace if not exists")
          boolean createNamespaceIfNotExists)
      throws IOException {
    try (RESTCatalog catalog = loadCatalog()) {
      String[] split = name.split("[.]");
      for (String level : split) {
        if (level.isEmpty()) {
          throw new IllegalArgumentException(
              "Invalid namespace name: '.' cannot separate empty names");
        }
      }
      if (split.length == 0) {
        throw new IllegalArgumentException("Invalid namespace name: name cannot be empty");
      }

      CreateNamespace.run(catalog, Namespace.of(split), createNamespaceIfNotExists);
    }
  }

  @CommandLine.Command(name = "delete-namespace", description = "Delete namespace.")
  void deleteNamespace(
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<name>",
              description = "Namespace name (e.g. parent_ns.child_ns)")
          String name,
      @CommandLine.Option(
              names = {"-p"},
              description = "Ignore not found")
          boolean ignoreNotFound)
      throws IOException {
    try (RESTCatalog catalog = loadCatalog()) {
      DeleteNamespace.run(catalog, Namespace.of(name.split("[.]")), ignoreNotFound);
    }
  }

  @CommandLine.Command(name = "delete", description = "Delete data from catalog.")
  void delete(
      @CommandLine.Parameters(
              arity = "1",
              paramLabel = "<name>",
              description = "Table name (e.g. ns1.table1)")
          String name,
      @CommandLine.Option(
              names = {"--partition"},
              description =
                  "JSON array of partition filters: [{\"name\": \"vendorId\", \"values\": [5, 6]}]. "
                      + "For timestamp columns, use ISO Datetime format YYYY-MM-ddTHH:mm:ss")
          String partitionJson,
      @CommandLine.Option(
              names = "--dry-run",
              description =
                  "Log files that would be deleted without actually deleting them (true by default)")
          Boolean dryRun)
      throws IOException {
    if (dryRun == null) {
      dryRun = true;
    }

    try (RESTCatalog catalog = loadCatalog(this.configFile())) {
      List<PartitionFilter> partitions = new ArrayList<>();
      if (partitionJson != null && !partitionJson.isEmpty()) {
        ObjectMapper mapper = newObjectMapper();
        PartitionFilter[] parts = mapper.readValue(partitionJson, PartitionFilter[].class);
        partitions = Arrays.asList(parts);
      }
      TableIdentifier tableId = TableIdentifier.parse(name);

      Delete.run(catalog, tableId, partitions, dryRun);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public record PartitionFilter(
      @JsonProperty("name") String name, @JsonProperty("values") List<Object> values) {}

  private RESTCatalog loadCatalog() throws IOException {
    return loadCatalog(this.configFile());
  }

  private static ObjectMapper newObjectMapper() {
    ObjectMapper om = new ObjectMapper(new YAMLFactory());
    om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    om.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true);
    return om;
  }

  private RESTCatalog loadCatalog(String configFile) throws IOException {
    Config config = Config.load(configFile);

    byte[] caCrt = null;
    if (!Strings.isNullOrEmpty(config.caCrt())) {
      String caCrtSrc = config.caCrt().trim();
      if (caCrtSrc.startsWith("base64:")) {
        caCrt = Base64.getDecoder().decode(Strings.removePrefix(caCrtSrc, "base64:"));
      } else {
        caCrt = caCrtSrc.getBytes();
      }
    }

    // Command-line flag takes precedence over config file
    // Default to true (verify SSL) unless explicitly disabled
    boolean sslVerify = !insecure;
    if (config.sslVerify() != null) {
      sslVerify = config.sslVerify() && !insecure;
    }

    if (!sslVerify) {
      logger.warn(
          "SSL certificate verification is DISABLED. This is insecure and should only be used for development.");
    }

    RESTCatalog catalog = RESTCatalogFactory.create(caCrt, sslVerify);
    var icebergConfig = config.toIcebergConfig();
    logger.debug(
        "Iceberg configuration: {}",
        icebergConfig.entrySet().stream()
            .map(
                e ->
                    !e.getKey().contains("key") && !e.getKey().contains("authorization")
                        ? e.getKey() + "=" + e.getValue()
                        : e.getKey())
            .sorted()
            .collect(Collectors.joining(", ")));
    String catalogUri = icebergConfig.get(CatalogProperties.URI);
    try {
      catalog.initialize("default", icebergConfig);
    } catch (org.apache.iceberg.exceptions.RESTException e) {
      throw new RuntimeException(
          String.format(
              "Failed to connect to REST catalog at '%s'. "
                  + "Please check that the catalog server is running and the URL is correct. "
                  + "Configure via: .ice.yaml 'uri' field, ICE_URI env var, or --config flag.",
              catalogUri),
          e);
    }
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
          logger.error("Fatal", ex);
          return 1;
        });
    int exitCode = cmd.execute(args);
    System.exit(exitCode);
  }
}
