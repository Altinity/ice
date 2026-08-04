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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * Docker-based integration test that verifies request/client-id grouping.
 *
 * <p>Runs Apache Spark (spark-iceberg image) as an Iceberg REST client against the ice-rest-catalog
 * container and asserts that every catalog call made by a single Spark session shares one client
 * id. The id is the deterministic per-identity fingerprint ({@code fp-...}) advertised via {@code
 * /v1/config} (and echoed back by the Iceberg Java client), so even the {@code /v1/config} request
 * shares the same id as the operations. Grouping is checked from the catalog container logs, where
 * the logback pattern renders {@code %X{clientId}} on each request line.
 *
 * <p>Excluded from the default Failsafe run (see {@code ice-rest-catalog/pom.xml}); run explicitly
 * with {@code ./mvnw -pl ice-rest-catalog verify -Dit.test=DockerSparkClientIdIT}. Requires Docker
 * and a locally-built, tracing-enabled catalog image. This test depends on the request/client-id
 * tracing feature, which is not present in published images, so the image must be built from
 * current source first via {@code .bin/local-docker-build-ice-rest-catalog} (or {@code docker build
 * --build-arg BASE_IMAGE_TAG=debug -t altinity/ice-rest-catalog:debug-with-ice-local -f
 * ice-rest-catalog/Dockerfile.debug-with-ice .}). Override the image with {@code
 * -Ddocker.image=...}.
 */
public class DockerSparkClientIdIT {

  private static final Logger logger = LoggerFactory.getLogger(DockerSparkClientIdIT.class);

  private static final String SPARK_IMAGE = "tabulario/spark-iceberg:3.5.5_1.8.1";

  // Strips ANSI escape codes (present only if the catalog runs with a color-capable terminal).
  private static final Pattern ANSI = Pattern.compile("\u001B\\[[;\\d]*m");

  // Matches request log lines emitted by RESTCatalogServlet:
  // "... RESTCatalogServlet > <clientId> <requestId> @token:anonymous <METHOD> v1/<path>".
  // Anchoring on the logger name avoids false matches when MDC ids are absent.
  private static final Pattern REQUEST_LINE =
      Pattern.compile(
          "RESTCatalogServlet > (\\S+) (\\S+) @token:anonymous (GET|POST|HEAD|DELETE) (v1/\\S+)");

  // The deterministic per-identity fingerprint advertised by the catalog (fp-<crc32 hex>).
  private static final Pattern FINGERPRINT_PATTERN = Pattern.compile("fp-[0-9a-fA-F]+");

  private Network network;
  private GenericContainer<?> minio;
  private GenericContainer<?> catalog;
  private GenericContainer<?> spark;

  @BeforeClass
  @SuppressWarnings("resource")
  public void setUp() throws Exception {
    String dockerImage =
        System.getProperty("docker.image", "altinity/ice-rest-catalog:debug-with-ice-local");
    logger.info("Using catalog Docker image: {}", dockerImage);

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

    // Catalog config (SQLite metastore, MinIO warehouse, anonymous read-write, tracing defaults on)
    URL configResource = getClass().getClassLoader().getResource("docker-catalog-config.yaml");
    if (configResource == null) {
      throw new IllegalStateException("docker-catalog-config.yaml not found on classpath");
    }
    String catalogConfig = Files.readString(Paths.get(configResource.toURI()));

    // Start the ice-rest-catalog container
    catalog =
        new GenericContainer<>(dockerImage)
            .withNetwork(network)
            .withNetworkAliases("catalog")
            .withExposedPorts(5000)
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

    // Spark client, configured to talk to the co-located catalog + MinIO over the Docker network.
    Path sparkDefaults = Files.createTempFile("spark-defaults-", ".conf");
    Files.writeString(sparkDefaults, sparkDefaultsConf(), StandardCharsets.UTF_8);

    spark =
        new GenericContainer<>(SPARK_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("spark")
            .withExposedPorts(8888)
            .withCopyFileToContainer(
                MountableFile.forHostPath(sparkDefaults), "/opt/spark/conf/spark-defaults.conf")
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));

    try {
      spark.start();
    } catch (Exception e) {
      if (spark != null) {
        logger.error("Spark container logs: {}", spark.getLogs());
      }
      throw e;
    }

    logger.info(
        "Catalog started at {}:{}, Spark started ({})",
        catalog.getHost(),
        catalog.getMappedPort(5000),
        spark.getContainerId());
  }

  @AfterClass
  public void tearDown() {
    if (spark != null) {
      spark.close();
    }
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

  @Test
  public void sparkRequestsShareOneClientId() throws Exception {
    // A single spark-sql invocation == one SparkSession == one Iceberg RESTCatalog client, which
    // performs one /v1/config handshake followed by several catalog operations.
    String sql =
        "CREATE NAMESPACE IF NOT EXISTS spark_ns; "
            + "CREATE TABLE spark_ns.t (id BIGINT) USING iceberg; "
            + "INSERT INTO spark_ns.t VALUES (1),(2),(3); "
            + "SELECT COUNT(*) FROM spark_ns.t;";

    Container.ExecResult result =
        spark.execInContainer("bash", "-c", "/opt/spark/bin/spark-sql -e \"" + sql + "\"");

    if (result.getExitCode() != 0) {
      logger.error("spark-sql stdout:\n{}", result.getStdout());
      logger.error("spark-sql stderr:\n{}", result.getStderr());
      logger.error("catalog logs:\n{}", catalog.getLogs());
    }
    assertThat(result.getExitCode())
        .as("spark-sql should succeed; stderr: %s", result.getStderr())
        .isEqualTo(0);

    String logs = ANSI.matcher(catalog.getLogs()).replaceAll("");

    Set<String> operationClientIds = new LinkedHashSet<>();
    int operationRequests = 0;
    String configClientId = null;
    Matcher m = REQUEST_LINE.matcher(logs);
    while (m.find()) {
      String clientId = m.group(1);
      String path = m.group(4);
      if (path.startsWith("v1/config")) {
        configClientId = clientId;
      } else {
        operationClientIds.add(clientId);
        operationRequests++;
      }
    }

    logger.info(
        "Parsed {} operation requests; config clientId={}, operation clientIds={}",
        operationRequests,
        configClientId,
        operationClientIds);

    assertThat(operationRequests)
        .as("expected multiple catalog operations from the Spark session")
        .isGreaterThanOrEqualTo(2);

    assertThat(operationClientIds)
        .as("all operations from one Spark client must share a single client id")
        .hasSize(1);

    String sharedClientId = operationClientIds.iterator().next();
    assertThat(sharedClientId)
        .as("shared client id must be the deterministic fingerprint advertised by the catalog")
        .matches(FINGERPRINT_PATTERN);

    assertThat(configClientId)
        .as("the /v1/config request must share the same client id as the operations")
        .isEqualTo(sharedClientId);
  }

  private static String sparkDefaultsConf() {
    return String.join(
            "\n",
            "spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.demo org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.demo.type rest",
            "spark.sql.catalog.demo.uri http://catalog:5000",
            "spark.sql.catalog.demo.io-impl org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.demo.warehouse s3://test-bucket/warehouse",
            "spark.sql.catalog.demo.s3.endpoint http://minio:9000",
            "spark.sql.catalog.demo.s3.path-style-access true",
            "spark.sql.catalog.demo.s3.access-key minioadmin",
            "spark.sql.catalog.demo.s3.secret-key minioadmin",
            "spark.sql.catalog.demo.client.region us-east-1",
            "spark.sql.catalog.demo.s3.ssl-enabled false",
            "spark.sql.defaultCatalog demo",
            "spark.sql.catalogImplementation in-memory")
        + "\n";
  }
}
