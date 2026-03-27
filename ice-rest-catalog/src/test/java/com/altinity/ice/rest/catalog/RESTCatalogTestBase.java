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

import static com.altinity.ice.rest.catalog.Main.createServer;

import com.altinity.ice.internal.jetty.DebugServer;
import com.altinity.ice.rest.catalog.internal.config.Config;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * Base class for REST catalog integration tests. Provides common setup and teardown for minio, REST
 * catalog server, and REST client.
 */
public abstract class RESTCatalogTestBase {

  protected static final Logger logger = LoggerFactory.getLogger(RESTCatalogTestBase.class);
  protected Server server;
  protected Server debugServer;

  @SuppressWarnings("rawtypes")
  protected final GenericContainer minio =
      new GenericContainer("minio/minio:latest")
          .withExposedPorts(9000)
          .withEnv("MINIO_ACCESS_KEY", "minioadmin")
          .withEnv("MINIO_SECRET_KEY", "minioadmin")
          .withCommand("server", "/data");

  @BeforeClass
  public void setUp() throws Exception {
    // Start minio container
    minio.start();

    // Configure S3 properties for minio
    String minioEndpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);

    // Create S3 client to create bucket
    S3Client s3Client =
        S3Client.builder()
            .endpointOverride(URI.create(minioEndpoint))
            .region(Region.US_EAST_1)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("minioadmin", "minioadmin")))
            .forcePathStyle(true)
            .build();

    // Create the test bucket
    try {
      s3Client.createBucket(CreateBucketRequest.builder().bucket("test-bucket").build());
      logger.info("Created test-bucket in minio");
    } catch (Exception e) {
      logger.warn("Bucket may already exist: {}", e.getMessage());
    } finally {
      s3Client.close();
    }

    // Create ICE REST catalog server configuration
    Config config =
        new Config(
            "localhost:8080", // addr
            "localhost:8081", // debugAddr
            null, // adminAddr
            "test-catalog", // name
            "jdbc:sqlite::memory:", // uri
            "s3://test-bucket/warehouse", // warehouse
            null, // localFileIOBaseDir
            new Config.S3(minioEndpoint, true, "minioadmin", "minioadmin", "us-east-1"), // s3
            null, // bearerTokens
            new Config.AnonymousAccess(
                true,
                new Config.AccessConfig(
                    false, null)), // anonymousAccess - enable with read-write for testing
            null, // maintenanceSchedule
            null, // maintenance
            null, // loadTableProperties
            null // icebergProperties
            );

    // Create backend catalog from config
    Map<String, String> icebergConfig = config.toIcebergConfig();
    Catalog backendCatalog =
        org.apache.iceberg.CatalogUtil.buildIcebergCatalog("backend", icebergConfig, null);

    // Start ICE REST catalog server
    server = createServer("localhost", 8080, backendCatalog, config, icebergConfig, null);
    server.start();

    // Start DebugServer for /metrics endpoint (used by MetricsScenarioBasedIT)
    debugServer = DebugServer.create("localhost", 8081);
    debugServer.start();

    // Wait for server to be ready
    while (!server.isStarted()) {
      Thread.sleep(100);
    }

    // Server is ready for CLI commands
  }

  @AfterClass
  public void tearDown() {

    // Stop the Debug server
    if (debugServer != null) {
      try {
        debugServer.stop();
      } catch (Exception e) {
        logger.error("Error stopping debug server: {}", e.getMessage(), e);
      }
    }

    // Stop the REST catalog server
    if (server != null) {
      try {
        server.stop();
      } catch (Exception e) {
        logger.error("Error stopping server: {}", e.getMessage(), e);
      }
    }

    // Stop minio container
    if (minio != null && minio.isRunning()) {
      minio.stop();
    }
  }

  /** Helper method to create a temporary CLI config file */
  protected File createTempCliConfig() throws Exception {
    File tempConfigFile = File.createTempFile("ice-rest-cli-", ".yaml");
    tempConfigFile.deleteOnExit();

    String configContent = "uri: http://localhost:8080\n";
    Files.write(tempConfigFile.toPath(), configContent.getBytes());

    return tempConfigFile;
  }

  /** Get the MinIO endpoint URL */
  protected String getMinioEndpoint() {
    return "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
  }

  /** Get the REST catalog URI */
  protected String getCatalogUri() {
    return "http://localhost:8080";
  }

  /**
   * Get the path to the scenarios directory.
   *
   * @return Path to scenarios directory
   * @throws URISyntaxException If the resource URL cannot be converted to a path
   */
  protected Path getScenariosDirectory() throws URISyntaxException {
    URL scenariosUrl = getClass().getClassLoader().getResource("scenarios");
    if (scenariosUrl == null) {
      return Paths.get("src/test/resources/scenarios");
    }
    return Paths.get(scenariosUrl.toURI());
  }

  /**
   * Create a ScenarioTestRunner for the given scenario. Subclasses provide host or container-based
   * CLI and config.
   *
   * @param scenarioName Name of the scenario (e.g. for container path resolution)
   * @return Configured ScenarioTestRunner
   * @throws Exception If there's an error creating the runner
   */
  protected abstract ScenarioTestRunner createScenarioRunner(String scenarioName) throws Exception;

  /** Data provider that discovers all test scenarios. */
  @DataProvider(name = "scenarios")
  public Object[][] scenarioProvider() throws Exception {
    Path scenariosDir = getScenariosDirectory();
    ScenarioTestRunner runner = new ScenarioTestRunner(scenariosDir, Map.of());
    List<String> scenarios = runner.discoverScenarios();

    if (scenarios.isEmpty()) {
      logger.warn("No test scenarios found in: {}", scenariosDir);
      return new Object[0][0];
    }

    logger.info("Discovered {} test scenario(s): {}", scenarios.size(), scenarios);

    Object[][] data = new Object[scenarios.size()][1];
    for (int i = 0; i < scenarios.size(); i++) {
      data[i][0] = scenarios.get(i);
    }
    return data;
  }

  /** Parameterized test that executes a single scenario. */
  @Test(dataProvider = "scenarios")
  public void testScenario(String scenarioName) throws Exception {
    logger.info("====== Starting scenario test: {} ======", scenarioName);

    ScenarioTestRunner runner = createScenarioRunner(scenarioName);
    ScenarioTestRunner.ScenarioResult result = runner.executeScenario(scenarioName);

    if (result.runScriptResult() != null) {
      logger.info("Run script exit code: {}", result.runScriptResult().exitCode());
    }
    if (result.verifyScriptResult() != null) {
      logger.info("Verify script exit code: {}", result.verifyScriptResult().exitCode());
    }

    assertScenarioSuccess(scenarioName, result);
    logger.info("====== Scenario test passed: {} ======", scenarioName);
  }

  /** Assert that the scenario result indicates success; otherwise throw AssertionError. */
  protected void assertScenarioSuccess(
      String scenarioName, ScenarioTestRunner.ScenarioResult result) {
    if (result.isSuccess()) {
      return;
    }
    StringBuilder errorMessage = new StringBuilder();
    errorMessage.append("Scenario '").append(scenarioName).append("' failed:\n");

    if (result.runScriptResult() != null && result.runScriptResult().exitCode() != 0) {
      errorMessage.append("\nRun script failed with exit code: ");
      errorMessage.append(result.runScriptResult().exitCode());
      errorMessage.append("\nStdout:\n").append(result.runScriptResult().stdout());
      errorMessage.append("\nStderr:\n").append(result.runScriptResult().stderr());
    }

    if (result.verifyScriptResult() != null && result.verifyScriptResult().exitCode() != 0) {
      errorMessage.append("\nVerify script failed with exit code: ");
      errorMessage.append(result.verifyScriptResult().exitCode());
      errorMessage.append("\nStdout:\n").append(result.verifyScriptResult().stdout());
      errorMessage.append("\nStderr:\n").append(result.verifyScriptResult().stderr());
    }

    throw new AssertionError(errorMessage.toString());
  }
}
