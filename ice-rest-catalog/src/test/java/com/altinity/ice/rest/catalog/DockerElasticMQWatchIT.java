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

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Docker integration test: {@code ice insert --watch} consuming an S3 object-created event from an
 * SQS-compatible queue (ElasticMQ) and committing the referenced Parquet file as an Iceberg
 * snapshot.
 *
 * <p>Topology: MinIO holds the warehouse ({@code s3://test-bucket/warehouse}) and the landing
 * object ({@code s3://test-bucket/landing/data.parquet}); ElasticMQ provides the {@code s3-events}
 * queue; the catalog container runs the co-located {@code ice} CLI in watch mode against ElasticMQ.
 * The watch is driven with {@code --watch-commit-schedule} + {@code --watch-max-files=1} so the
 * first matched file flushes immediately, and {@code --watch-fire-once} so the command exits after
 * one poll cycle instead of looping forever.
 *
 * <p>The watch batching flags are client-side in the {@code ice} CLI, so the catalog image must be
 * built from current source for the bundled {@code ice} to have them. Build it locally once (per
 * {@code ice} change) with:
 *
 * <pre>docker build --build-arg BASE_IMAGE_TAG=debug \
 *   -t altinity/ice-rest-catalog:debug-with-ice-local \
 *   -f ice-rest-catalog/Dockerfile.debug-with-ice .</pre>
 *
 * <p>Requires Docker. Excluded from default Failsafe runs (see {@code pom.xml}); run explicitly,
 * e.g. {@code mvn -pl ice-rest-catalog failsafe:integration-test failsafe:verify
 * -Dit.test=DockerElasticMQWatchIT}. Image tags can be overridden via {@code -Ddocker.image=...},
 * {@code -Delasticmq.image=...} and {@code -Dminio.image=...}.
 */
public class DockerElasticMQWatchIT {

  private static final Logger logger = LoggerFactory.getLogger(DockerElasticMQWatchIT.class);

  private static final String DEFAULT_CATALOG_IMAGE =
      "altinity/ice-rest-catalog:debug-with-ice-local";
  private static final String DEFAULT_ELASTICMQ_IMAGE = "softwaremill/elasticmq-native:1.6.15";
  private static final String DEFAULT_MINIO_IMAGE = "minio/minio:latest";

  private static final String BUCKET = "test-bucket";
  private static final String QUEUE_NAME = "s3-events";
  private static final String NAMESPACE = "watch_test";
  private static final String TABLE = NAMESPACE + ".events";
  private static final String LANDING_KEY = "warehouse/watch_test/events/external/data.parquet";

  // ElasticMQ queue URL as seen from inside the Docker network (see elasticmq.conf accountId).
  private static final String QUEUE_URL_INTERNAL =
      "http://elasticmq:9324/000000000000/" + QUEUE_NAME;

  private Network network;
  private GenericContainer<?> minio;
  private GenericContainer<?> elasticmq;
  private GenericContainer<?> catalog;

  @BeforeClass
  @SuppressWarnings("resource")
  public void setUp() throws Exception {
    String dockerImage = System.getProperty("docker.image", DEFAULT_CATALOG_IMAGE);
    String elasticmqImage = System.getProperty("elasticmq.image", DEFAULT_ELASTICMQ_IMAGE);
    String minioImage = System.getProperty("minio.image", DEFAULT_MINIO_IMAGE);
    logger.info(
        "Using images: catalog={}, elasticmq={}, minio={}",
        dockerImage,
        elasticmqImage,
        minioImage);

    network = Network.newNetwork();

    minio =
        new GenericContainer<>(minioImage)
            .withNetwork(network)
            .withNetworkAliases("minio")
            .withExposedPorts(9000)
            .withEnv("MINIO_ACCESS_KEY", "minioadmin")
            .withEnv("MINIO_SECRET_KEY", "minioadmin")
            .withCommand("server", "/data")
            .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));
    minio.start();

    String minioHostEndpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
    try (S3Client s3 = minioS3(minioHostEndpoint)) {
      s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
      logger.info("Created bucket {} in MinIO", BUCKET);
    }

    elasticmq =
        new GenericContainer<>(elasticmqImage)
            .withNetwork(network)
            .withNetworkAliases("elasticmq")
            .withExposedPorts(9324)
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("elasticmq.conf"), "/opt/elasticmq.conf")
            .withCommand("-Dconfig.file=/opt/elasticmq.conf")
            .waitingFor(Wait.forListeningPort());
    elasticmq.start();

    URL configResource = getClass().getClassLoader().getResource("docker-catalog-config.yaml");
    if (configResource == null) {
      throw new IllegalStateException("docker-catalog-config.yaml not on classpath");
    }
    String catalogConfig = Files.readString(Paths.get(configResource.toURI()));

    catalog =
        new GenericContainer<>(dockerImage)
            .withNetwork(network)
            .withNetworkAliases("catalog")
            .withExposedPorts(5000)
            .withEnv("ICE_REST_CATALOG_CONFIG", "")
            .withEnv("ICE_REST_CATALOG_CONFIG_YAML", catalogConfig)
            // Default AWS credential chain used by the watcher's SQS client. Values match MinIO so
            // the same env also satisfies any incidental S3 use; ElasticMQ accepts any well-formed
            // credentials.
            .withEnv("AWS_ACCESS_KEY_ID", "minioadmin")
            .withEnv("AWS_SECRET_ACCESS_KEY", "minioadmin")
            .withEnv("AWS_REGION", "us-east-1")
            .waitingFor(Wait.forHttp("/v1/config").forPort(5000).forStatusCode(200));

    try {
      catalog.start();
    } catch (Exception e) {
      logger.error("Catalog container logs: {}", catalog.getLogs());
      throw e;
    }

    // CLI config: ice runs inside the catalog container, so it reaches MinIO and ElasticMQ via
    // their network aliases.
    String cliConfig =
        "uri: http://localhost:5000\n"
            + "warehouse: s3://"
            + BUCKET
            + "/warehouse\n"
            + "s3:\n"
            + "  endpoint: http://minio:9000\n"
            + "  pathStyleAccess: true\n"
            + "  accessKeyID: minioadmin\n"
            + "  secretAccessKey: minioadmin\n"
            + "  region: us-east-1\n";
    catalog.copyFileToContainer(
        MountableFile.forHostPath(writeTemp(cliConfig)), "/tmp/ice-cli.yaml");

    logger.info(
        "Catalog at {}:{}, ElasticMQ at {}:{}",
        catalog.getHost(),
        catalog.getMappedPort(5000),
        elasticmq.getHost(),
        elasticmq.getMappedPort(9324));
  }

  @AfterClass
  public void tearDown() {
    if (catalog != null) {
      catalog.close();
    }
    if (elasticmq != null) {
      elasticmq.close();
    }
    if (minio != null) {
      minio.close();
    }
    if (network != null) {
      network.close();
    }
  }

  @Test
  public void testWatchCommitsS3EventAsSnapshot() throws Exception {
    Path parquet = Files.createTempFile("watch-it-", ".parquet");
    try {
      writeParquet(parquet);
      long size = Files.size(parquet);

      // Upload the file to MinIO under the warehouse prefix so the noCopy insert path uses
      // table.io() (REST-catalog-configured FileIO with the MinIO endpoint) rather than a raw
      // S3FileIO that lacks the endpoint override.
      String minioHostEndpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
      try (S3Client s3 = minioS3(minioHostEndpoint)) {
        s3.putObject(
            PutObjectRequest.builder().bucket(BUCKET).key(LANDING_KEY).build(),
            RequestBody.fromFile(parquet));
      }
      logger.info("Uploaded {} to s3://{}/{} ({} bytes)", parquet, BUCKET, LANDING_KEY, size);

      // Pre-create the table from a local copy so CreateTable.run (which builds its own S3Client
      // without the MinIO endpoint) is never invoked during the watch flush.
      catalog.copyFileToContainer(MountableFile.forHostPath(parquet), "/tmp/seed.parquet");
      iceExecOrThrow("create-namespace", NAMESPACE);
      iceExecOrThrow("insert", "--create-table", TABLE, "file:///tmp/seed.parquet");

      // Notify the watcher about the new object.
      String elasticmqHostEndpoint =
          "http://" + elasticmq.getHost() + ":" + elasticmq.getMappedPort(9324);
      String queueUrlHost = elasticmqHostEndpoint + "/000000000000/" + QUEUE_NAME;
      try (SqsClient sqs =
          SqsClient.builder()
              .endpointOverride(URI.create(elasticmqHostEndpoint))
              .region(Region.US_EAST_1)
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsBasicCredentials.create("minioadmin", "minioadmin")))
              .build()) {
        sqs.sendMessage(
            SendMessageRequest.builder()
                .queueUrl(queueUrlHost)
                .messageBody(s3Event(BUCKET, LANDING_KEY, size))
                .build());
      }
      logger.info("Sent S3 event for s3://{}/{} to {}", BUCKET, LANDING_KEY, queueUrlHost);

      // Run the watcher once. --watch-max-files=1 flushes the first matched file immediately;
      // --watch-fire-once exits after the first poll cycle.
      ExecResult watch =
          ice(
              "insert",
              TABLE,
              "-p",
              "--force-no-copy",
              "--skip-duplicates",
              "--watch=" + QUEUE_URL_INTERNAL,
              "--watch-endpoint=http://elasticmq:9324",
              "--watch-commit-schedule=every 5 minutes",
              "--watch-max-files=1",
              "--watch-fire-once",
              "s3://" + BUCKET + "/warehouse/watch_test/events/external/*.parquet");
      logger.info("watch stdout:\n{}", watch.getStdout());
      logger.info("watch stderr:\n{}", watch.getStderr());
      if (watch.getExitCode() != 0) {
        throw new AssertionError(
            "ice insert --watch exited " + watch.getExitCode() + ":\n" + watch.getStderr());
      }

      ExecResult scan = iceExecOrThrow("scan", TABLE);
      logger.info("scan stdout:\n{}", scan.getStdout());
      if (!scan.getStdout().contains("watch-it")) {
        throw new AssertionError(
            "Expected committed row in scan output, got:\n" + scan.getStdout());
      }
    } finally {
      Files.deleteIfExists(parquet);
    }
  }

  @Test
  public void testWatchMaxBytesTrigger() throws Exception {
    String table = NAMESPACE + ".events_bytes";
    String landingKey = "warehouse/watch_test/events_bytes/external/data.parquet";
    long size = seedTableAndEnqueueEvent(table, landingKey);

    ExecResult watch =
        ice(
            "insert",
            table,
            "-p",
            "--force-no-copy",
            "--skip-duplicates",
            "--watch=" + QUEUE_URL_INTERNAL,
            "--watch-endpoint=http://elasticmq:9324",
            "--watch-max-bytes=" + size,
            "--watch-fire-once",
            "s3://" + BUCKET + "/warehouse/watch_test/events_bytes/external/*.parquet");
    logger.info("watch stdout:\n{}", watch.getStdout());
    logger.info("watch stderr:\n{}", watch.getStderr());
    if (watch.getExitCode() != 0) {
      throw new AssertionError(
          "ice insert --watch exited " + watch.getExitCode() + ":\n" + watch.getStderr());
    }
    if (!watch.getStderr().contains("trigger: max_bytes")) {
      throw new AssertionError(
          "Expected flush trigger 'max_bytes' in watch stderr, got:\n" + watch.getStderr());
    }

    ExecResult scan = iceExecOrThrow("scan", table);
    if (!scan.getStdout().contains("watch-it")) {
      throw new AssertionError("Expected committed row in scan output, got:\n" + scan.getStdout());
    }
  }

  @Test
  public void testWatchScheduleOnlyTrigger() throws Exception {
    String table = NAMESPACE + ".events_sched";
    String landingKey = "warehouse/watch_test/events_sched/external/data.parquet";
    seedTableAndEnqueueEvent(table, landingKey);

    ExecResult watch =
        ice(
            "insert",
            table,
            "-p",
            "--force-no-copy",
            "--skip-duplicates",
            "--watch=" + QUEUE_URL_INTERNAL,
            "--watch-endpoint=http://elasticmq:9324",
            "--watch-commit-schedule=every 1 minutes",
            "--watch-fire-once",
            "s3://" + BUCKET + "/warehouse/watch_test/events_sched/external/*.parquet");
    logger.info("watch stdout:\n{}", watch.getStdout());
    logger.info("watch stderr:\n{}", watch.getStderr());
    if (watch.getExitCode() != 0) {
      throw new AssertionError(
          "ice insert --watch exited " + watch.getExitCode() + ":\n" + watch.getStderr());
    }
    if (!watch.getStderr().contains("trigger: fire_once")) {
      throw new AssertionError(
          "Expected flush trigger 'fire_once' in watch stderr, got:\n" + watch.getStderr());
    }

    ExecResult scan = iceExecOrThrow("scan", table);
    if (!scan.getStdout().contains("watch-it")) {
      throw new AssertionError("Expected committed row in scan output, got:\n" + scan.getStdout());
    }
  }

  /**
   * Writes a Parquet file, uploads it to MinIO, pre-creates the table from a local copy, and sends
   * an S3 event to ElasticMQ. Returns the file size in bytes (useful for --watch-max-bytes).
   */
  private long seedTableAndEnqueueEvent(String table, String landingKey) throws Exception {
    Path parquet = Files.createTempFile("watch-it-", ".parquet");
    try {
      writeParquet(parquet);
      long size = Files.size(parquet);

      String minioHostEndpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
      try (S3Client s3 = minioS3(minioHostEndpoint)) {
        s3.putObject(
            PutObjectRequest.builder().bucket(BUCKET).key(landingKey).build(),
            RequestBody.fromFile(parquet));
      }
      logger.info("Uploaded to s3://{}/{} ({} bytes)", BUCKET, landingKey, size);

      catalog.copyFileToContainer(MountableFile.forHostPath(parquet), "/tmp/seed.parquet");
      iceExecOrThrow("insert", "--create-table", table, "file:///tmp/seed.parquet");

      String elasticmqHostEndpoint =
          "http://" + elasticmq.getHost() + ":" + elasticmq.getMappedPort(9324);
      String queueUrlHost = elasticmqHostEndpoint + "/000000000000/" + QUEUE_NAME;
      try (SqsClient sqs =
          SqsClient.builder()
              .endpointOverride(URI.create(elasticmqHostEndpoint))
              .region(Region.US_EAST_1)
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsBasicCredentials.create("minioadmin", "minioadmin")))
              .build()) {
        sqs.sendMessage(
            SendMessageRequest.builder()
                .queueUrl(queueUrlHost)
                .messageBody(s3Event(BUCKET, landingKey, size))
                .build());
      }
      logger.info("Sent S3 event for s3://{}/{}", BUCKET, landingKey);

      return size;
    } finally {
      Files.deleteIfExists(parquet);
    }
  }

  private static String s3Event(String bucket, String key, long size) {
    return "{\"Records\":[{\"eventName\":\"ObjectCreated:Put\","
        + "\"eventTime\":\"2026-08-16T00:00:00.000Z\","
        + "\"s3\":{\"bucket\":{\"name\":\""
        + bucket
        + "\"},\"object\":{\"key\":\""
        + key
        + "\",\"size\":"
        + size
        + "}}}]}";
  }

  private static S3Client minioS3(String endpoint) {
    return S3Client.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create("minioadmin", "minioadmin")))
        .forcePathStyle(true)
        .build();
  }

  /** Runs the catalog container's bundled {@code ice} CLI against the test config. */
  private ExecResult ice(String... args) throws IOException, InterruptedException {
    List<String> cmd = new ArrayList<>();
    cmd.add("ice");
    cmd.add("--config");
    cmd.add("/tmp/ice-cli.yaml");
    for (String a : args) {
      cmd.add(a);
    }
    return catalog.execInContainer(cmd.toArray(new String[0]));
  }

  private ExecResult iceExecOrThrow(String... args) throws IOException, InterruptedException {
    ExecResult r = ice(args);
    logger.info("ice {} stdout:\n{}", String.join(" ", args), r.getStdout());
    if (r.getExitCode() != 0) {
      throw new IllegalStateException(
          "ice "
              + String.join(" ", args)
              + " failed: exit="
              + r.getExitCode()
              + "\nstdout:\n"
              + r.getStdout()
              + "\nstderr:\n"
              + r.getStderr()
              + "\ncatalog logs:\n"
              + catalog.getLogs());
    }
    return r;
  }

  private static Path writeTemp(String contents) throws IOException {
    Path f = Files.createTempFile("ice-watch-cli-", ".yaml");
    Files.writeString(f, contents);
    f.toFile().deleteOnExit();
    return f;
  }

  private static void writeParquet(Path file) throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    Record row = GenericRecord.create(schema);
    row.setField("id", 1);
    row.setField("name", "watch-it");
    OutputFile outputFile =
        HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(file.toUri()), new Configuration());
    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(schema)
            .setAll(java.util.Map.of())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .metricsConfig(MetricsConfig.getDefault())
            // Files.createTempFile already created the file, so allow overwriting it.
            .overwrite()
            .build()) {
      writer.add(row);
    }
  }
}
