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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Docker integration test: Iceberg REST catalog with {@code file:///warehouse} (shared host volume)
 * and ClickHouse reading the same files via {@code DataLakeCatalog}.
 *
 * <p>Validates that metadata uses absolute {@code file:///warehouse/...} paths so ClickHouse can
 * resolve them against its {@code /warehouse} bind mount.
 *
 * <p>Requires Docker. Excluded from default Failsafe runs (see {@code pom.xml}); run explicitly,
 * e.g. {@code mvn -pl ice-rest-catalog verify -Dit.test=DockerLocalFileIOClickHouseIT}. Image tags
 * can be overridden via {@code -Ddocker.image=...} (catalog) and {@code -Dclickhouse.image=...}
 * (ClickHouse); both fall back to baked-in defaults for local development.
 */
public class DockerLocalFileIOClickHouseIT {

  /** Directory name under {@code test/resources/scenarios/}; must match {@code scenario.yaml}. */
  private static final String SCENARIO_NAME = "clickhouse-localfileio-read";

  private static final Logger logger = LoggerFactory.getLogger(DockerLocalFileIOClickHouseIT.class);

  private static final String DEFAULT_CATALOG_IMAGE =
      "altinity/ice-rest-catalog:debug-with-ice-0.12.0";

  private static final String DEFAULT_CLICKHOUSE_IMAGE =
      "altinity/clickhouse-server:25.8.16.20002.altinityantalya";

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

    hostWarehouseDir = Files.createTempDirectory("ice-warehouse-");

    URL configResource =
        getClass().getClassLoader().getResource("docker-catalog-localfileio-config.yaml");
    if (configResource == null) {
      throw new IllegalStateException("docker-catalog-localfileio-config.yaml not on classpath");
    }
    String catalogConfig = Files.readString(Paths.get(configResource.toURI()));

    Path scenariosDir = getScenariosDirectory().toAbsolutePath();
    if (!Files.isDirectory(scenariosDir)) {
      throw new IllegalStateException("Scenarios directory missing: " + scenariosDir);
    }
    Path insertScanInput = scenariosDir.resolve("insert-scan").resolve("input.parquet");
    if (!Files.exists(insertScanInput)) {
      throw new IllegalStateException("Missing scenario input: " + insertScanInput);
    }

    network = Network.newNetwork();

    catalog =
        new GenericContainer<>(dockerImage)
            .withNetwork(network)
            .withNetworkAliases("catalog")
            .withExposedPorts(5000)
            .withFileSystemBind(hostWarehouseDir.toString(), "/warehouse", BindMode.READ_WRITE)
            .withEnv("ICE_REST_CATALOG_CONFIG", "")
            .withEnv("ICE_REST_CATALOG_CONFIG_YAML", catalogConfig)
            .withCopyFileToContainer(MountableFile.forHostPath(scenariosDir), "/scenarios")
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
  public void testClickHouseReadsLocalFileIOTable() throws Exception {
    Path scenariosDir = getScenariosDirectory();

    File iceWrapper = File.createTempFile("ice-docker-exec-", ".sh");
    iceWrapper.deleteOnExit();
    Files.writeString(
        iceWrapper.toPath(),
        "#!/bin/sh\nexec docker exec " + catalog.getContainerId() + " ice \"$@\"\n");
    if (!iceWrapper.setExecutable(true)) {
      throw new IllegalStateException("Could not chmod +x " + iceWrapper);
    }

    File chWrapper = File.createTempFile("ch-docker-exec-", ".sh");
    chWrapper.deleteOnExit();
    Files.writeString(
        chWrapper.toPath(),
        "#!/bin/sh\nexec docker exec "
            + clickhouse.getContainerId()
            + " clickhouse-client \"$@\"\n");
    if (!chWrapper.setExecutable(true)) {
      throw new IllegalStateException("Could not chmod +x " + chWrapper);
    }

    Map<String, String> templateVars = new HashMap<>();
    templateVars.put("ICE_CLI", iceWrapper.getAbsolutePath());
    templateVars.put("CH_EXEC", chWrapper.getAbsolutePath());
    templateVars.put("CLI_CONFIG", "/tmp/ice-cli.yaml");
    templateVars.put("SCENARIO_DIR", "/scenarios/" + SCENARIO_NAME);
    templateVars.put("CATALOG_URI_INTERNAL", "http://catalog:5000");
    templateVars.put("MINIO_ENDPOINT", "");

    ScenarioTestRunner runner = new ScenarioTestRunner(scenariosDir, templateVars);
    ScenarioTestRunner.ScenarioResult result = runner.executeScenario(SCENARIO_NAME);

    if (result.runScriptResult() != null) {
      logger.info("Run script exit code: {}", result.runScriptResult().exitCode());
    }
    assertScenarioSuccess(result);
  }

  private static void assertScenarioSuccess(ScenarioTestRunner.ScenarioResult result) {
    if (result.isSuccess()) {
      return;
    }
    StringBuilder errorMessage = new StringBuilder();
    errorMessage.append("Scenario failed:\n");
    if (result.runScriptResult() != null && result.runScriptResult().exitCode() != 0) {
      errorMessage.append("\nRun script exit code: ").append(result.runScriptResult().exitCode());
      errorMessage.append("\nStdout:\n").append(result.runScriptResult().stdout());
      errorMessage.append("\nStderr:\n").append(result.runScriptResult().stderr());
    }
    if (result.verifyScriptResult() != null && result.verifyScriptResult().exitCode() != 0) {
      errorMessage
          .append("\nVerify script exit code: ")
          .append(result.verifyScriptResult().exitCode());
      errorMessage.append("\nStdout:\n").append(result.verifyScriptResult().stdout());
      errorMessage.append("\nStderr:\n").append(result.verifyScriptResult().stderr());
    }
    throw new AssertionError(errorMessage.toString());
  }

  private Path getScenariosDirectory() throws Exception {
    URL scenariosUrl = getClass().getClassLoader().getResource("scenarios");
    if (scenariosUrl == null) {
      return Paths.get("src/test/resources/scenarios").toAbsolutePath();
    }
    return Paths.get(scenariosUrl.toURI());
  }
}
