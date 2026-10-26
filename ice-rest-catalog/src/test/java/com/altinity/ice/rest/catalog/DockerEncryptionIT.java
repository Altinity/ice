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

import com.altinity.ice.encryption.EncryptingRESTCatalog;
import com.altinity.ice.testsupport.StaticKeyManagementClient;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;
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
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Docker integration test for Iceberg table encryption: REST catalog + Spark + ClickHouse sharing
 * {@code file:///warehouse}.
 *
 * <ol>
 *   <li>Spark creates an encrypted table ({@code encryption.key-id}) through the REST catalog.
 *   <li>{@code ice insert} writes encrypted data/manifests (KMS: {@link
 *       StaticKeyManagementClient}).
 *   <li>Files on disk are checked to be encrypted ({@code PARE} Parquet footer magic, {@code AGS1}
 *       AES GCM Stream for manifests and manifest lists).
 *   <li>ClickHouse is expected to fail reading the table (it doesn't support Iceberg encryption).
 *   <li>Spark appends a row and reads everything back (decryption of both ice- and Spark-written
 *       files).
 * </ol>
 *
 * <p>Spark uses {@link EncryptingRESTCatalog} ({@code catalog-impl}) as the upstream Iceberg REST
 * client doesn't implement encryption.
 *
 * <p>Requires Docker and a prior {@code mvn install} (for the ice-encryption/ice-test-support
 * jars). Excluded from default Failsafe runs (see {@code pom.xml}); run explicitly, e.g. {@code mvn
 * -pl ice-rest-catalog failsafe:integration-test failsafe:verify -Dit.test=DockerEncryptionIT
 * -Ddocker.image=altinity/ice-rest-catalog:debug-with-ice-local}. The Spark image is built from
 * {@code src/test/docker/spark-iceberg} on first run and kept for reuse.
 */
public class DockerEncryptionIT {

  private static final Logger logger = LoggerFactory.getLogger(DockerEncryptionIT.class);

  private static final String DEFAULT_CATALOG_IMAGE =
      "altinity/ice-rest-catalog:debug-with-ice-0.12.0";

  private static final String DEFAULT_CLICKHOUSE_IMAGE =
      "altinity/clickhouse-server:25.8.16.20002.altinityantalya";

  private static final String SPARK_IMAGE = "ice-it/spark-iceberg:4.1.1-1.11.0";

  private static final String KMS_IMPL = StaticKeyManagementClient.class.getName();
  private static final String KEY_ID = "ice-it-master-key";

  private static final String NAMESPACE = "enc_ns";
  private static final String TABLE = NAMESPACE + ".t";
  private static final String CH_DB = "ice_enc";

  private static final String ICE_JAR = "/usr/local/bin/ice-rest-catalog";
  private static final String TEST_SUPPORT_JAR = "/opt/ice-test/ice-test-support.jar";

  private Network network;
  private Path hostWarehouseDir;
  private GenericContainer<?> catalog;
  private GenericContainer<?> spark;
  private GenericContainer<?> clickhouse;

  @BeforeClass
  @SuppressWarnings("resource")
  public void setUp() throws Exception {
    String catalogImage = System.getProperty("docker.image", DEFAULT_CATALOG_IMAGE);
    String clickhouseImage = System.getProperty("clickhouse.image", DEFAULT_CLICKHOUSE_IMAGE);
    logger.info("Using catalog image {}, ClickHouse image {}", catalogImage, clickhouseImage);

    Path encryptionJar = jarOf(EncryptingRESTCatalog.class);
    Path testSupportJar = jarOf(StaticKeyManagementClient.class);

    hostWarehouseDir = Files.createTempDirectory("ice-warehouse-");
    // spark (uid 185) and clickhouse (uid 101) need to traverse the bind mount
    Files.setPosixFilePermissions(hostWarehouseDir, PosixFilePermissions.fromString("rwxr-xr-x"));

    String catalogConfig =
        Files.readString(
            Paths.get(
                getClass()
                    .getClassLoader()
                    .getResource("docker-catalog-localfileio-config.yaml")
                    .toURI()));

    network = Network.newNetwork();

    catalog =
        new GenericContainer<>(catalogImage)
            .withNetwork(network)
            .withNetworkAliases("catalog")
            .withExposedPorts(5000)
            .withFileSystemBind(hostWarehouseDir.toString(), "/warehouse", BindMode.READ_WRITE)
            .withEnv("ICE_REST_CATALOG_CONFIG", "")
            .withEnv("ICE_REST_CATALOG_CONFIG_YAML", catalogConfig)
            .withCopyFileToContainer(
                MountableFile.forHostPath(testSupportJar, 0644), TEST_SUPPORT_JAR)
            .waitingFor(Wait.forHttp("/v1/config").forPort(5000).forStatusCode(200));
    startOrDumpLogs(catalog);

    copyStringToContainer(
        catalog,
        "uri: http://localhost:5000\n"
            + "warehouse: file:///warehouse\n"
            + "icebergProperties:\n"
            + "  encryption.kms-impl: "
            + KMS_IMPL
            + "\n",
        "/tmp/ice-cli-kms.yaml");
    copyStringToContainer(
        catalog, "uri: http://localhost:5000\nwarehouse: file:///warehouse\n", "/tmp/ice-cli.yaml");

    Path sparkDockerDir = Paths.get("src/test/docker/spark-iceberg").toAbsolutePath();
    spark =
        new GenericContainer<>(
                new ImageFromDockerfile(SPARK_IMAGE, false).withFileFromPath(".", sparkDockerDir))
            .withNetwork(network)
            .withNetworkAliases("spark")
            .withFileSystemBind(hostWarehouseDir.toString(), "/warehouse", BindMode.READ_WRITE)
            .withCopyFileToContainer(
                MountableFile.forHostPath(encryptionJar, 0644),
                "/opt/spark/jars/ice-encryption.jar")
            .withCopyFileToContainer(
                MountableFile.forHostPath(testSupportJar, 0644),
                "/opt/spark/jars/ice-test-support.jar")
            // root: Spark writes into table dirs created by the (root) catalog container
            .withCreateContainerCmdModifier(
                c -> c.withEntrypoint("tail", "-f", "/dev/null").withUser("root"))
            .waitingFor(Wait.forSuccessfulCommand("true"));
    startOrDumpLogs(spark);

    clickhouse =
        new GenericContainer<>(clickhouseImage)
            .withNetwork(network)
            .withNetworkAliases("clickhouse")
            .withExposedPorts(8123, 9000)
            .withFileSystemBind(hostWarehouseDir.toString(), "/warehouse", BindMode.READ_ONLY)
            .waitingFor(Wait.forHttp("/ping").forPort(8123).forStatusCode(200));
    startOrDumpLogs(clickhouse);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    for (GenericContainer<?> c : new GenericContainer<?>[] {clickhouse, spark, catalog}) {
      if (c != null) {
        c.close();
      }
    }
    if (network != null) {
      network.close();
    }
    if (hostWarehouseDir != null) {
      try (Stream<Path> walk = Files.walk(hostWarehouseDir)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      } catch (Exception e) {
        logger.warn("Failed to delete warehouse dir {}: {}", hostWarehouseDir, e.getMessage());
      }
    }
  }

  @Test
  public void testSparkReadsTableEncryptedByIce() throws Exception {
    sparkSql(
        "CREATE NAMESPACE IF NOT EXISTS ice."
            + NAMESPACE
            + "; CREATE TABLE ice."
            + TABLE
            + " (id bigint, data string) USING iceberg TBLPROPERTIES ("
            + "'encryption.key-id'='"
            + KEY_ID
            + "', 'format-version'='3')");

    Path input = Files.createTempFile("ice-enc-input-", ".parquet");
    try {
      writeParquet(input);
      catalog.copyFileToContainer(MountableFile.forHostPath(input), "/tmp/enc-input.parquet");
    } finally {
      Files.deleteIfExists(input);
    }

    // without a KMS, ice must refuse to write into an encrypted table
    ExecResult noKms =
        catalog.execInContainer(
            "ice",
            "--config",
            "/tmp/ice-cli.yaml",
            "insert",
            TABLE,
            "file:///tmp/enc-input.parquet");
    assertThat(noKms.getExitCode()).as(describe("ice insert without KMS", noKms)).isNotZero();
    assertThat(noKms.getStdout() + noKms.getStderr()).contains("no KMS is configured");

    iceWithKmsOrThrow("insert", TABLE, "file:///tmp/enc-input.parquet");

    assertFilesEncrypted();

    assertClickHouseCannotRead();

    String rows =
        sparkSql(
            "INSERT INTO ice."
                + TABLE
                + " VALUES (4, 'written-by-spark'); SELECT id, data FROM ice."
                + TABLE
                + " ORDER BY id");
    assertThat(rows.lines().map(String::trim).filter(l -> !l.isEmpty()).toList())
        .containsExactly("1\talpha", "2\tbeta", "3\tgamma", "4\twritten-by-spark");

    assertFilesEncrypted();
  }

  // Must run before Spark writes: Spark (HadoopFileIO) records file:/ (single slash) paths, which
  // ClickHouse fails to resolve regardless of encryption.
  private void assertClickHouseCannotRead() throws Exception {
    ExecResult ch =
        clickhouse.execInContainer(
            "clickhouse-client",
            "--multiquery",
            "--query",
            "SET allow_experimental_database_iceberg = 1; "
                + "DROP DATABASE IF EXISTS `"
                + CH_DB
                + "`; CREATE DATABASE `"
                + CH_DB
                + "` ENGINE = DataLakeCatalog('http://catalog:5000') "
                + "SETTINGS catalog_type='rest', vended_credentials=false, warehouse='warehouse'; "
                + "SELECT id, data FROM `"
                + CH_DB
                + "`.`"
                + TABLE
                + "` ORDER BY id");
    logger.info("ClickHouse read of encrypted table: {}", describe("clickhouse-client", ch));
    assertThat(ch.getExitCode())
        .as("ClickHouse is not expected to decrypt Iceberg tables: " + describe("ch", ch))
        .isNotZero();
    assertThat(ch.getStdout()).doesNotContain("alpha");
    assertThat(ch.getStderr())
        .as("ClickHouse must fail on the encrypted content, not on file resolution")
        .doesNotContain("No such file or directory");
  }

  private void assertFilesEncrypted() throws IOException {
    List<Path> dataFiles = new ArrayList<>();
    List<Path> avroFiles = new ArrayList<>();
    try (Stream<Path> walk = Files.walk(hostWarehouseDir)) {
      walk.filter(Files::isRegularFile)
          .forEach(
              p -> {
                String name = p.getFileName().toString();
                if (name.endsWith(".parquet")) {
                  dataFiles.add(p);
                } else if (name.endsWith(".avro")) {
                  avroFiles.add(p);
                }
              });
    }
    assertThat(dataFiles).as("data files").isNotEmpty();
    assertThat(avroFiles).as("manifests/manifest lists").isNotEmpty();
    for (Path p : dataFiles) {
      assertThat(magic(p, false)).as("footer magic of %s", p).isEqualTo("PARE");
    }
    for (Path p : avroFiles) {
      assertThat(magic(p, true)).as("header magic of %s", p).isEqualTo("AGS1");
    }
    assertThat(latestMetadataJson()).contains("\"encryption-keys\"").contains(KEY_ID);
  }

  private String latestMetadataJson() throws IOException {
    try (Stream<Path> walk = Files.walk(hostWarehouseDir)) {
      Path latest =
          walk.filter(p -> p.getFileName().toString().endsWith(".metadata.json"))
              .max(Comparator.comparing(p -> p.toFile().lastModified()))
              .orElseThrow(() -> new AssertionError("no metadata.json in " + hostWarehouseDir));
      return Files.readString(latest);
    }
  }

  private static String magic(Path p, boolean head) throws IOException {
    byte[] bytes = Files.readAllBytes(p);
    int offset = head ? 0 : bytes.length - 4;
    return new String(bytes, offset, 4, StandardCharsets.US_ASCII);
  }

  private void iceWithKmsOrThrow(String... args) throws Exception {
    List<String> cmd = new ArrayList<>();
    // ice from the image + test KMS on the classpath (never bundled into the ice binary)
    cmd.addAll(
        List.of(
            "java",
            "-cp",
            ICE_JAR + ":" + TEST_SUPPORT_JAR,
            "com.altinity.ice.rest.catalog.Main",
            "ice",
            "--config",
            "/tmp/ice-cli-kms.yaml"));
    cmd.addAll(List.of(args));
    ExecResult r = catalog.execInContainer(cmd.toArray(new String[0]));
    if (r.getExitCode() != 0) {
      throw new IllegalStateException(
          describe("ice " + String.join(" ", args), r) + "\ncatalog logs:\n" + catalog.getLogs());
    }
  }

  private String sparkSql(String sql) throws Exception {
    ExecResult r =
        spark.execInContainer(
            "/opt/spark/bin/spark-sql",
            "--master",
            "local[1]",
            "--conf",
            "spark.ui.enabled=false",
            "--conf",
            "spark.sql.catalogImplementation=in-memory",
            "--conf",
            "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "--conf",
            "spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog",
            "--conf",
            "spark.sql.catalog.ice.catalog-impl=" + EncryptingRESTCatalog.class.getName(),
            "--conf",
            "spark.sql.catalog.ice.uri=http://catalog:5000",
            "--conf",
            "spark.sql.catalog.ice.io-impl=org.apache.iceberg.hadoop.HadoopFileIO",
            "--conf",
            "spark.sql.catalog.ice.encryption.kms-impl=" + KMS_IMPL,
            "-S",
            "-e",
            sql);
    if (r.getExitCode() != 0) {
      throw new IllegalStateException(
          describe("spark-sql " + sql, r) + "\ncatalog logs:\n" + catalog.getLogs());
    }
    return r.getStdout();
  }

  private static void writeParquet(Path path) throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    try (FileAppender<Record> writer =
        Parquet.write(
                HadoopOutputFile.fromPath(
                    new org.apache.hadoop.fs.Path(path.toUri()), new Configuration()))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::create)
            .metricsConfig(MetricsConfig.getDefault())
            .overwrite()
            .build()) {
      String[] values = {"alpha", "beta", "gamma"};
      for (int i = 0; i < values.length; i++) {
        GenericRecord rec = GenericRecord.create(schema);
        rec.setField("id", (long) i + 1);
        rec.setField("data", values[i]);
        writer.add(rec);
      }
    }
  }

  /** Returns the jar containing {@code clazz}, packaging it if it comes from a classes dir. */
  private static Path jarOf(Class<?> clazz) throws IOException, URISyntaxException {
    Path location = Paths.get(clazz.getProtectionDomain().getCodeSource().getLocation().toURI());
    if (Files.isRegularFile(location)) {
      return location;
    }
    Path jar = Files.createTempFile(clazz.getSimpleName() + "-", ".jar");
    jar.toFile().deleteOnExit();
    try (OutputStream os = Files.newOutputStream(jar);
        JarOutputStream jos = new JarOutputStream(os);
        Stream<Path> walk = Files.walk(location)) {
      for (Path p : walk.filter(Files::isRegularFile).toList()) {
        jos.putNextEntry(new JarEntry(location.relativize(p).toString().replace('\\', '/')));
        try (InputStream is = Files.newInputStream(p)) {
          is.transferTo(jos);
        }
        jos.closeEntry();
      }
    }
    return jar;
  }

  private static void copyStringToContainer(
      GenericContainer<?> container, String content, String containerPath) throws IOException {
    Path tmp = Files.createTempFile("ice-it-", ".tmp");
    try {
      Files.writeString(tmp, content);
      container.copyFileToContainer(MountableFile.forHostPath(tmp), containerPath);
    } finally {
      Files.deleteIfExists(tmp);
    }
  }

  private static void startOrDumpLogs(GenericContainer<?> container) {
    try {
      container.start();
    } catch (RuntimeException e) {
      logger.error("Container {} failed to start. Logs:\n{}", container, container.getLogs());
      throw e;
    }
  }

  private static String describe(String what, ExecResult r) {
    return what
        + ": exit="
        + r.getExitCode()
        + "\nstdout:\n"
        + r.getStdout()
        + "\nstderr:\n"
        + r.getStderr();
  }
}
