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
import com.altinity.ice.cli.internal.cmd.Check;
import com.altinity.ice.cli.internal.cmd.CreateTable;
import com.altinity.ice.cli.internal.cmd.DeleteTable;
import com.altinity.ice.cli.internal.cmd.Describe;
import com.altinity.ice.cli.internal.cmd.Insert;
import com.altinity.ice.cli.internal.config.Config;
import com.altinity.ice.internal.picocli.VersionProvider;
import com.altinity.ice.internal.strings.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
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
    versionProvider = VersionProvider.class,
    subcommands = AutoComplete.GenerateCompletion.class)
public final class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  @CommandLine.Option(
      names = {"-c", "--config"},
      description = "/path/to/config.yaml ($CWD/.ice.yaml by default)",
      scope = CommandLine.ScopeType.INHERIT)
  String configFile;

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

  private Main() {}

  @CommandLine.Command(name = "check", description = "Check configuration.")
  void check() throws IOException {
    try (RESTCatalog catalog = loadCatalog(this.configFile())) {
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
    try (RESTCatalog catalog = loadCatalog(this.configFile())) {
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
          String schemaFile)
      throws IOException {
    try (RESTCatalog catalog = loadCatalog(this.configFile())) {
      CreateTable.run(
          catalog,
          TableIdentifier.parse(name),
          schemaFile,
          location,
          createTableIfNotExists,
          s3NoSignRequest);
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
              description = "Supported: DEFAULT, PRESERVE_ORIGINAL",
              defaultValue = "DEFAULT")
          Insert.DataFileNamingStrategy.Name dataFileNamingStrategy,
      @CommandLine.Option(names = "--skip-duplicates", description = "Skip duplicates")
          boolean skipDuplicates,
      @CommandLine.Option(
              names = {"--retry-list"},
              description =
                  "/path/to/file where to save list of files to retry"
                      + " (useful for retrying partially failed insert using `cat ice.retry | ice insert - --retry-list=ice.retry`)")
          String retryList,
      @CommandLine.Option(
              names = {"--thread-count"},
              description = "Number of threads to use for inserting data",
              defaultValue = "-1")
          int threadCount)
      throws IOException, InterruptedException {
    if (s3NoSignRequest && s3CopyObject) {
      throw new UnsupportedOperationException(
          "--s3-no-sign-request + --s3-copy-object is not supported by AWS (see --help for details)");
    }
    try (RESTCatalog catalog = loadCatalog(this.configFile())) {
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
            catalog, tableId, dataFiles[0], null, createTableIfNotExists, s3NoSignRequest);
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
          retryList,
          threadCount < 1 ? Runtime.getRuntime().availableProcessors() : threadCount);
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
    try (RESTCatalog catalog = loadCatalog(this.configFile())) {
      DeleteTable.run(catalog, TableIdentifier.parse(name), ignoreNotFound);
    }
  }

  private RESTCatalog loadCatalog(String configFile) throws IOException {
    Config config = Config.load(configFile);
    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize("default", config.toIcebergConfig());
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
