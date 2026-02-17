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
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * Docker-based integration tests for ICE REST Catalog.
 *
 * <p>Runs the ice-rest-catalog Docker image (specified via system property {@code docker.image})
 * alongside a MinIO container, then executes scenario-based tests against it.
 */
public class DockerScenarioBasedIT extends RESTCatalogTestBase {

  private Network network;

  private GenericContainer<?> minio;

  private GenericContainer<?> catalog;

  @Override
  @BeforeClass
  @SuppressWarnings("resource")
  public void setUp() throws Exception {
    String dockerImage =
        System.getProperty("docker.image", "altinity/ice-rest-catalog:debug-with-ice-0.12.0");
    logger.info("Using Docker image: {}", dockerImage);

    network = Network.newNetwork();

    // Start MinIO
    minio =
        new GenericContainer<>("minio/minio:latest")
            .withNetwork(network)
            .withNetworkAliases("minio")
            .withExposedPorts(9000)
            .withEnv("MINIO_ACCESS_KEY", "minioadmin")
            .withEnv("MINIO_SECRET_KEY", "minioadmin")
            .withCommand("server", "/data")
            .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));
    minio.start();

    // Create test bucket via MinIO's host-mapped port
    String minioHostEndpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
    try (var s3Client =
        S3Client.builder()
            .endpointOverride(URI.create(minioHostEndpoint))
            .region(Region.US_EAST_1)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("minioadmin", "minioadmin")))
            .forcePathStyle(true)
            .build()) {
      s3Client.createBucket(CreateBucketRequest.builder().bucket("test-bucket").build());
      logger.info("Created test-bucket in MinIO");
    }

    // Load YAML config for the catalog container (MinIO via Docker network alias "minio")
    URL configResource = getClass().getClassLoader().getResource("docker-catalog-config.yaml");
    if (configResource == null) {
      throw new IllegalStateException("docker-catalog-config.yaml not found on classpath");
    }
    String catalogConfig = Files.readString(Paths.get(configResource.toURI()));

    Path scenariosDir = getScenariosDirectory().toAbsolutePath();
    if (!Files.exists(scenariosDir) || !Files.isDirectory(scenariosDir)) {
      throw new IllegalStateException(
          "Scenarios directory must exist at "
              + scenariosDir
              + ". Run 'mvn test-compile' or run the test from Maven (e.g. mvn failsafe:integration-test).");
    }
    Path insertScanInput = scenariosDir.resolve("insert-scan").resolve("input.parquet");
    if (!Files.exists(insertScanInput)) {
      throw new IllegalStateException(
          "Scenario input not found at "
              + insertScanInput
              + ". Ensure test resources are on the classpath and scenarios/insert-scan/input.parquet exists.");
    }

    // Start the ice-rest-catalog container (debug-with-ice has ice CLI at /usr/local/bin/ice)
    catalog =
        new GenericContainer<>(dockerImage)
            .withNetwork(network)
            .withExposedPorts(5000)
            .withEnv("ICE_REST_CATALOG_CONFIG", "")
            .withEnv("ICE_REST_CATALOG_CONFIG_YAML", catalogConfig)
            .withCopyFileToContainer(MountableFile.forHostPath(scenariosDir), "/scenarios")
            .waitingFor(Wait.forHttp("/v1/config").forPort(5000).forStatusCode(200));

    try {
      catalog.start();
    } catch (Exception e) {
      if (catalog != null) {
        logger.error("Catalog container logs (stdout): {}", catalog.getLogs());
      }
      throw e;
    }

    // Copy CLI config into container so ice CLI can talk to co-located REST server
    File cliConfigHost = File.createTempFile("ice-docker-cli-", ".yaml");
    try {
      Files.write(cliConfigHost.toPath(), "uri: http://localhost:5000\n".getBytes());
      catalog.copyFileToContainer(
          MountableFile.forHostPath(cliConfigHost.toPath()), "/tmp/ice-cli.yaml");
    } finally {
      cliConfigHost.delete();
    }

    logger.info(
        "Catalog container started at {}:{}", catalog.getHost(), catalog.getMappedPort(5000));
  }

  @Override
  @AfterClass
  public void tearDown() {
    if (catalog != null) {
      catalog.close();
    }
    if (minio != null) {
      minio.close();
    }
    if (network != null) {
      network.close();
    }
  }

  @Override
  protected ScenarioTestRunner createScenarioRunner(String scenarioName) throws Exception {
    Path scenariosDir = getScenariosDirectory();

    String containerId = catalog.getContainerId();

    // Wrapper script on host: docker exec <container> ice "$@" (CLI runs inside container)
    File wrapperScript = File.createTempFile("ice-docker-exec-", ".sh");
    wrapperScript.deleteOnExit();
    String wrapperContent = "#!/bin/sh\n" + "exec docker exec " + containerId + " ice \"$@\"\n";
    Files.write(wrapperScript.toPath(), wrapperContent.getBytes());
    if (!wrapperScript.setExecutable(true)) {
      throw new IllegalStateException("Could not set wrapper script executable: " + wrapperScript);
    }

    Map<String, String> templateVars = new HashMap<>();
    templateVars.put("ICE_CLI", wrapperScript.getAbsolutePath());
    templateVars.put("CLI_CONFIG", "/tmp/ice-cli.yaml");
    templateVars.put("SCENARIO_DIR", "/scenarios/" + scenarioName);
    templateVars.put("MINIO_ENDPOINT", "");
    templateVars.put("CATALOG_URI", "http://localhost:5000");

    return new ScenarioTestRunner(scenariosDir, templateVars);
  }
}
