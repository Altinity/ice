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

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Docker integration test: same topology as {@link DockerLocalFileIOClickHouseIT} (REST catalog +
 * {@code file:///warehouse} shared with ClickHouse). Writes a one-row Parquet with int, string,
 * date, and timestamp columns, inserts via {@code ice}, then asserts ClickHouse reads the same
 * values and {@code count() = 1}. A follow-up test alters the table (required vs optional columns)
 * and checks ClickHouse {@code system.columns} types.
 *
 * <p>Requires Docker. Excluded from default Failsafe runs (see {@code pom.xml}); run explicitly,
 * e.g. {@code mvn -pl ice-rest-catalog verify -Dit.test=DockerLocalFileIOClickHouseAllTypesIT}.
 */
public class DockerLocalFileIOClickHouseAllTypesIT {

  private static final Logger logger =
      LoggerFactory.getLogger(DockerLocalFileIOClickHouseAllTypesIT.class);

  private static final String DEFAULT_CATALOG_IMAGE =
      "altinity/ice-rest-catalog:debug-with-ice-0.12.0";

  private static final String DEFAULT_CLICKHOUSE_IMAGE =
      "altinity/clickhouse-server:25.8.16.20002.altinityantalya";

  private static final String CH_DB = "ice_localfileio";
  private static final String NAMESPACE = "ch_test";
  private static final String TABLE = NAMESPACE + ".basictypes";

  private Network network;

  private Path hostWarehouseDir;

  private GenericContainer<?> catalog;

  private GenericContainer<?> clickhouse;

  @BeforeClass
  @SuppressWarnings("resource")
  public void setUp() throws Exception {
    String dockerImage = System.getProperty("docker.image", DEFAULT_CATALOG_IMAGE);
    logger.info("Using catalog Docker image: {}", dockerImage);

    String clickhouseImage = System.getProperty("clickhouse.image", DEFAULT_CLICKHOUSE_IMAGE);
    logger.info("Using ClickHouse Docker image: {}", clickhouseImage);

    hostWarehouseDir = Files.createTempDirectory("ice-warehouse-basictypes-");
    Files.setPosixFilePermissions(hostWarehouseDir, PosixFilePermissions.fromString("rwxr-xr-x"));

    URL configResource =
        getClass().getClassLoader().getResource("docker-catalog-localfileio-config.yaml");
    if (configResource == null) {
      throw new IllegalStateException("docker-catalog-localfileio-config.yaml not on classpath");
    }
    String catalogConfig = Files.readString(Paths.get(configResource.toURI()));

    network = Network.newNetwork();

    catalog =
        new GenericContainer<>(dockerImage)
            .withNetwork(network)
            .withNetworkAliases("catalog")
            .withExposedPorts(5000)
            .withFileSystemBind(hostWarehouseDir.toString(), "/warehouse", BindMode.READ_WRITE)
            .withEnv("ICE_REST_CATALOG_CONFIG", "")
            .withEnv("ICE_REST_CATALOG_CONFIG_YAML", catalogConfig)
            .waitingFor(Wait.forHttp("/v1/config").forPort(5000).forStatusCode(200));

    try {
      catalog.start();
    } catch (Exception e) {
      if (catalog != null) {
        logger.error("Catalog container logs: {}", catalog.getLogs());
      }
      throw e;
    }

    File cliConfigHost = File.createTempFile("ice-docker-cli-", ".yaml");
    try {
      Files.write(
          cliConfigHost.toPath(),
          ("uri: http://localhost:5000\n" + "warehouse: file:///warehouse\n").getBytes());
      catalog.copyFileToContainer(
          MountableFile.forHostPath(cliConfigHost.toPath()), "/tmp/ice-cli.yaml");
    } finally {
      cliConfigHost.delete();
    }

    clickhouse =
        new GenericContainer<>(clickhouseImage)
            .withNetwork(network)
            .withNetworkAliases("clickhouse")
            .withExposedPorts(8123, 9000)
            .withFileSystemBind(hostWarehouseDir.toString(), "/warehouse", BindMode.READ_ONLY)
            .waitingFor(Wait.forHttp("/ping").forPort(8123).forStatusCode(200));

    try {
      clickhouse.start();
    } catch (Exception e) {
      if (clickhouse != null) {
        logger.error("ClickHouse container logs: {}", clickhouse.getLogs());
      }
      throw e;
    }

    logger.info(
        "Catalog at {}:{}, ClickHouse at {}:{}",
        catalog.getHost(),
        catalog.getMappedPort(5000),
        clickhouse.getHost(),
        clickhouse.getMappedPort(8123));
  }

  @AfterClass
  public void tearDown() {
    try {
      if (clickhouse != null && clickhouse.isRunning()) {
        clickhouse.execInContainer(
            "clickhouse-client", "--query", "DROP DATABASE IF EXISTS `" + CH_DB + "` SYNC");
      }
    } catch (Exception e) {
      logger.warn("ClickHouse cleanup failed: {}", e.getMessage());
    }
    try {
      if (catalog != null && catalog.isRunning()) {
        ExecResult r1 =
            catalog.execInContainer(
                "ice", "--config", "/tmp/ice-cli.yaml", "delete-table", TABLE, "-p");
        if (r1.getExitCode() != 0) {
          logger.warn("delete-table stderr: {}", r1.getStderr());
        }
        ExecResult r2 =
            catalog.execInContainer(
                "ice", "--config", "/tmp/ice-cli.yaml", "delete-namespace", NAMESPACE, "-p");
        if (r2.getExitCode() != 0) {
          logger.warn("delete-namespace stderr: {}", r2.getStderr());
        }
      }
    } catch (Exception e) {
      logger.warn("Ice CLI cleanup failed: {}", e.getMessage());
    }
    if (clickhouse != null) {
      clickhouse.close();
    }
    if (catalog != null) {
      catalog.close();
    }
    if (network != null) {
      network.close();
    }
    if (hostWarehouseDir != null) {
      try {
        try (var walk = Files.walk(hostWarehouseDir)) {
          walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
      } catch (Exception e) {
        logger.warn("Failed to delete warehouse dir {}: {}", hostWarehouseDir, e.getMessage());
      }
    }
  }

  @Test
  public void testClickHouseReadsBasicTypes() throws Exception {
    Schema schema = basicTypesSchema();
    Record row = GenericRecord.create(schema);
    row.setField("id", 1);
    row.setField("b_int", 40);
    row.setField("b_string", "hello");
    row.setField("b_date", LocalDate.of(2024, 6, 15));
    row.setField("b_ts", LocalDateTime.of(2024, 6, 15, 12, 30, 45));

    Path parquetFile = Files.createTempFile("basic-", ".parquet");
    parquetFile.toFile().deleteOnExit();
    writeParquet(schema, List.of(row), parquetFile);

    catalog.copyFileToContainer(MountableFile.forHostPath(parquetFile), "/tmp/basic.parquet");

    iceExecOrThrow("create-namespace", NAMESPACE);
    iceExecOrThrow("insert", "--create-table", TABLE, "file:///tmp/basic.parquet");

    recreateClickHouseDatabase();

    String countSql = "SELECT count() FROM `" + CH_DB + "`.`" + TABLE + "` FORMAT TabSeparated";
    String count = chQueryOne(countSql);
    if (!"1".equals(count)) {
      throw new AssertionError("Expected count()=1, got: " + count);
    }

    String valuesSql =
        "SELECT b_int, b_string, toString(b_date), formatDateTime(b_ts, '%Y-%m-%d %H:%M:%S') FROM `"
            + CH_DB
            + "`.`"
            + TABLE
            + "` FORMAT TabSeparated";
    String line = chQueryOne(valuesSql);
    String[] cells = line.split("\t", -1);
    if (cells.length != 4) {
      throw new AssertionError("Expected 4 columns, got " + cells.length + ": " + line);
    }
    if (!"40".equals(cells[0])) {
      throw new AssertionError("b_int: expected 40, got " + cells[0]);
    }
    if (!"hello".equals(cells[1])) {
      throw new AssertionError("b_string: expected hello, got " + cells[1]);
    }
    if (!"2024-06-15".equals(cells[2])) {
      throw new AssertionError("b_date: expected 2024-06-15, got " + cells[2]);
    }
    if (!"2024-06-15 12:30:45".equals(cells[3])) {
      throw new AssertionError("b_ts: expected 2024-06-15 12:30:45, got " + cells[3]);
    }
  }

  @Test(dependsOnMethods = "testClickHouseReadsBasicTypes")
  public void testAlterTableAddRequiredAndOptionalColumns() throws Exception {
    iceExecOrThrow(
        "alter-table",
        TABLE,
        "[{\"op\":\"add_column\",\"name\":\"req_col\",\"type\":\"int\",\"required\":true,\"initial_default\":\"0\"},"
            + "{\"op\":\"add_column\",\"name\":\"opt_col\",\"type\":\"int\",\"required\":false}]");

    recreateClickHouseDatabase();

    String typesSql =
        "SELECT name, type FROM system.columns WHERE database = '"
            + CH_DB
            + "' AND table = '"
            + TABLE
            + "' AND name IN ('req_col','opt_col') ORDER BY name FORMAT TabSeparated";
    String out = chQueryOne(typesSql);
    String[] lines = out.split("\n");
    if (lines.length != 2) {
      throw new AssertionError(
          "Expected 2 rows from system.columns (req_col, opt_col), got "
              + lines.length
              + ": "
              + out);
    }
    String[] optParts = lines[0].split("\t", -1);
    String[] reqParts = lines[1].split("\t", -1);
    if (optParts.length != 2 || reqParts.length != 2) {
      throw new AssertionError("Unexpected TSV shape: " + out);
    }
    if (!"opt_col".equals(optParts[0])) {
      throw new AssertionError("Expected first row opt_col, got " + optParts[0]);
    }
    if (!"req_col".equals(reqParts[0])) {
      throw new AssertionError("Expected second row req_col, got " + reqParts[0]);
    }
    String optType = optParts[1];
    String reqType = reqParts[1];
    if (!optType.startsWith("Nullable(")) {
      throw new AssertionError(
          "opt_col (required:false) expected Nullable(...) type, got: " + optType);
    }
    if (reqType.startsWith("Nullable(")) {
      throw new AssertionError(
          "req_col (required:true) expected non-Nullable type, got: " + reqType);
    }
  }

  private void recreateClickHouseDatabase() throws Exception {
    chExecOrThrow(
        "SET allow_experimental_database_iceberg = 1; "
            + "DROP DATABASE IF EXISTS `"
            + CH_DB
            + "`; "
            + "CREATE DATABASE `"
            + CH_DB
            + "` ENGINE = DataLakeCatalog('http://catalog:5000') "
            + "SETTINGS catalog_type='rest', vended_credentials=false, warehouse='warehouse'");
  }

  private static Schema basicTypesSchema() {
    return new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "b_int", Types.IntegerType.get()),
        Types.NestedField.optional(3, "b_string", Types.StringType.get()),
        Types.NestedField.optional(4, "b_date", Types.DateType.get()),
        Types.NestedField.optional(5, "b_ts", Types.TimestampType.withoutZone()));
  }

  private static void writeParquet(Schema schema, List<Record> records, Path parquetPath)
      throws Exception {
    org.apache.iceberg.io.OutputFile outputFile =
        HadoopOutputFile.fromPath(
            new org.apache.hadoop.fs.Path(parquetPath.toUri()), new Configuration());
    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(schema)
            .setAll(java.util.Map.of())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .metricsConfig(MetricsConfig.getDefault())
            .overwrite()
            .build()) {
      for (Record rec : records) {
        writer.add(rec);
      }
    }
  }

  private void iceExecOrThrow(String... args) throws Exception {
    List<String> cmd = new ArrayList<>();
    cmd.add("ice");
    cmd.add("--config");
    cmd.add("/tmp/ice-cli.yaml");
    for (String a : args) {
      cmd.add(a);
    }
    ExecResult result = catalog.execInContainer(cmd.toArray(new String[0]));
    if (result.getExitCode() != 0) {
      throw new IllegalStateException(
          "ice "
              + String.join(" ", args)
              + " failed: exit="
              + result.getExitCode()
              + "\nstdout:\n"
              + result.getStdout()
              + "\nstderr:\n"
              + result.getStderr()
              + "\ncatalog logs:\n"
              + catalog.getLogs());
    }
  }

  private void chExecOrThrow(String multiQuery) throws Exception {
    ExecResult result =
        clickhouse.execInContainer("clickhouse-client", "--multiquery", "--query", multiQuery);
    if (result.getExitCode() != 0) {
      throw new IllegalStateException(
          "clickhouse-client failed: exit="
              + result.getExitCode()
              + "\nstdout:\n"
              + result.getStdout()
              + "\nstderr:\n"
              + result.getStderr()
              + "\nclickhouse logs:\n"
              + clickhouse.getLogs());
    }
  }

  private String chQueryOne(String sql) throws Exception {
    String prelude = "SET session_timezone='UTC'; SET allow_experimental_database_iceberg=1; ";
    ExecResult r =
        clickhouse.execInContainer("clickhouse-client", "--multiquery", "--query", prelude + sql);
    if (r.getExitCode() != 0) {
      throw new IllegalStateException(
          "clickhouse-client SELECT failed: exit="
              + r.getExitCode()
              + "\nquery:\n"
              + sql
              + "\nstdout:\n"
              + r.getStdout()
              + "\nstderr:\n"
              + r.getStderr()
              + "\nclickhouse logs:\n"
              + clickhouse.getLogs());
    }
    return r.getStdout().trim();
  }
}
