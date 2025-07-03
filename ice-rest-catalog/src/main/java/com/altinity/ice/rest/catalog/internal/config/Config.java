/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.config;

import com.altinity.ice.internal.iceberg.io.LocalFileIO;
import com.altinity.ice.internal.iceberg.io.SchemeFileIO;
import com.altinity.ice.internal.strings.Strings;
import com.altinity.ice.rest.catalog.internal.aws.CustomS3TablesCatalog;
import com.altinity.ice.rest.catalog.internal.etcd.EtcdCatalog;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.io.Files;

public record Config(
    @JsonPropertyDescription("host:port (0.0.0.0:5000 by default)") String addr,
    @JsonPropertyDescription("host:port (0.0.0.0:5001 by default)") String debugAddr,
    @JsonPropertyDescription("host:port, e.g. localhost:5002 (disabled by default)")
        String adminAddr,
    @JsonPropertyDescription("Catalog storage URI: jdbc:..., etcd:...") String uri,
    @JsonPropertyDescription("Path to warehouse, e.g. s3://..., file://...") String warehouse,
    @JsonPropertyDescription(
            "/path/to/dir to serve as a root for warehouse=file://... (current workdir by default)")
        String localFileIOBaseDir,
    @JsonPropertyDescription("Settings for warehouse=s3://...") S3 s3,
    Token[] bearerTokens,
    @JsonPropertyDescription("Anonymous access configuration") AnonymousAccess anonymousAccess,
    @JsonPropertyDescription(
            "Maintenance schedule in https://github.com/shyiko/skedule?tab=readme-ov-file#format format, e.g. \"every day 00:00\". Empty schedule disables automatic maintenance (default)")
        String maintenanceSchedule,
    @JsonPropertyDescription("TTL for snapshots in days.") int snapshotTTLInDays,
    @JsonPropertyDescription("TTL for orphan files in days.") int orphanFileExpirationDays,
    @JsonPropertyDescription(
            "(experimental) Extra properties to include in loadTable REST response.")
        Map<String, String> loadTableProperties,
    @JsonPropertyDescription(
            "(experimental) Iceberg properties (see https://iceberg.apache.org/javadoc/1.8.1/"
                + "; e.g. https://iceberg.apache.org/javadoc/1.8.1/org/apache/iceberg/aws/s3/S3FileIOProperties.html)")
        Map<String, String> icebergProperties) {

  private static final String DEFAULT_ADDR = "0.0.0.0:5000";
  private static final String DEFAULT_DEBUG_ADDR = "0.0.0.0:5001";

  @JsonCreator
  public Config(
      String addr,
      String debugAddr,
      String adminAddr,
      @JsonProperty(required = true) String uri,
      @JsonProperty(required = true) String warehouse,
      String localFileIOBaseDir,
      S3 s3,
      Token[] bearerTokens,
      AnonymousAccess anonymousAccess,
      String maintenanceSchedule,
      int snapshotTTLInDays,
      int orphanFileExpirationDays,
      Map<String, String> loadTableProperties,
      @JsonProperty("iceberg") Map<String, String> icebergProperties) {
    this.addr = Strings.orDefault(addr, DEFAULT_ADDR);
    this.debugAddr = Strings.orDefault(debugAddr, DEFAULT_DEBUG_ADDR);
    this.adminAddr = Strings.orDefault(adminAddr, System.getenv("ICE_REST_CATALOG_ADMIN_ADDR"));
    this.uri = uri;
    this.warehouse = warehouse;
    this.localFileIOBaseDir = localFileIOBaseDir;
    this.s3 = s3;
    this.bearerTokens = Objects.requireNonNullElse(bearerTokens, new Token[0]);
    this.anonymousAccess =
        Objects.requireNonNullElse(anonymousAccess, new AnonymousAccess(false, null));
    this.maintenanceSchedule = maintenanceSchedule;
    this.snapshotTTLInDays = snapshotTTLInDays;
    this.orphanFileExpirationDays = orphanFileExpirationDays;
    this.loadTableProperties = Objects.requireNonNullElse(loadTableProperties, Map.of());
    this.icebergProperties = Objects.requireNonNullElse(icebergProperties, Map.of());
  }

  public record S3(
      @JsonPropertyDescription(
              "AWS_ENDPOINT_URL_S3 (see https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-envvars.html#envvars-list)")
          String endpoint,
      @JsonPropertyDescription(
              "Enable path-style requests (typically needed in case of minio, etc.)")
          boolean pathStyleAccess,
      @JsonPropertyDescription(
              "AWS_ACCESS_KEY_ID (see https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-envvars.html#envvars-list)")
          String accessKeyID,
      @JsonPropertyDescription(
              "AWS_SECRET_ACCESS_KEY (see https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-envvars.html#envvars-list)")
          String secretAccessKey,
      @JsonPropertyDescription(
              "AWS_REGION (see https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-envvars.html#envvars-list)")
          String region) {}

  public record Token(
      @JsonPropertyDescription("Name") String name,
      @JsonPropertyDescription("Token itself (e.g. `openssl rand -base64 16`)") String value,
      @JsonPropertyDescription("Token access config") AccessConfig accessConfig) {

    public Token(String name, String value, AccessConfig accessConfig) {
      this.name = name;
      this.value = value; // TODO: reserve prefix for using salted+hashed values
      this.accessConfig = Objects.requireNonNullElse(accessConfig, AccessConfig.READ_WRITE);
    }

    public String resourceName() {
      return !Strings.isNullOrEmpty(name) ? "token:" + name : "token";
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record AccessConfig(
      @JsonPropertyDescription("Allow to execute read-only operations only")
          boolean readOnly, // FIXME: should be readOnly unless explicitly set in case of anonymous
      @JsonPropertyDescription(
              "ARN of IAM Role to assume when including credentials in loadTable REST response")
          String awsAssumeRoleARN) {

    private static final AccessConfig READ_WRITE = new AccessConfig(false, null);
    private static final AccessConfig READ_ONLY = new AccessConfig(true, null);

    public boolean hasAWSAssumeRoleARN() {
      return !Strings.isNullOrEmpty(awsAssumeRoleARN);
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record AnonymousAccess(
      @JsonPropertyDescription("Allow anonymous clients") boolean enabled,
      @JsonPropertyDescription("Access config for anonymous clients") AccessConfig accessConfig) {

    public AnonymousAccess(boolean enabled, AccessConfig accessConfig) {
      this.enabled = enabled;
      this.accessConfig = Objects.requireNonNullElse(accessConfig, AccessConfig.READ_ONLY);
    }
  }

  public static Config load(String configFile) throws IOException {
    boolean configFileGiven = !Strings.isNullOrEmpty(configFile);
    Config c =
        com.altinity.ice.internal.config.Config.load(
            configFileGiven ? configFile : ".ice-rest-catalog.yaml",
            configFileGiven,
            System.getenv("ICE_REST_CATALOG_CONFIG_YAML"),
            new TypeReference<>() {});
    Set<String> set = Arrays.stream(c.bearerTokens).map(Token::name).collect(Collectors.toSet());
    if (set.contains("anonymous")) {
      throw new IllegalArgumentException("invalid config: token alias \"anonymous\" is reserved");
    }
    if (set.size() != c.bearerTokens.length) {
      throw new IllegalArgumentException(
          "invalid config: multiple tokens with the same alias (name) found");
    }
    return c;
  }

  public Map<String, String> toIcebergLoadTableConfig() {
    var m = new HashMap<String, String>();
    String iceIODefault = "ice.io.default.";
    if (s3 != null) {
      if (!Strings.isNullOrEmpty(s3.endpoint)) {
        m.put(iceIODefault + S3FileIOProperties.ENDPOINT, s3.endpoint);
      }
      if (s3.pathStyleAccess) {
        m.put(iceIODefault + S3FileIOProperties.PATH_STYLE_ACCESS, "true");
      }
      if (!Strings.isNullOrEmpty(s3.region)) {
        m.put(iceIODefault + AwsClientProperties.CLIENT_REGION, s3.region);
      }
    }
    if (warehouse.startsWith("arn:aws:s3tables:")) {
      String region = warehouse.split(":")[3];
      m.putIfAbsent(iceIODefault + AwsClientProperties.CLIENT_REGION, region);
    }
    for (Map.Entry<String, String> e : loadTableProperties.entrySet()) {
      if (e.getValue() != null) {
        m.put(e.getKey(), e.getValue());
      } else {
        m.remove(e.getKey());
      }
    }
    return m;
  }

  // TODO: get rid of IOException
  public Map<String, String> toIcebergConfig() throws IOException {
    var m =
        new HashMap<String, String>() {

          public void putNotNullOrEmpty(String key, String value) {
            if (Strings.isNullOrEmpty(value)) {
              return;
            }
            put(key, value);
          }
        };

    m.put(CatalogProperties.URI, uri);
    m.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
    m.put(CatalogProperties.FILE_IO_IMPL, SchemeFileIO.class.getName());

    if (s3 != null) {
      m.putNotNullOrEmpty(S3FileIOProperties.ENDPOINT, s3.endpoint);
      m.putNotNullOrEmpty(S3FileIOProperties.ACCESS_KEY_ID, s3.accessKeyID);
      m.putNotNullOrEmpty(S3FileIOProperties.SECRET_ACCESS_KEY, s3.secretAccessKey);
      m.putNotNullOrEmpty(AwsClientProperties.CLIENT_REGION, s3.region);
      if (s3.pathStyleAccess) {
        m.putNotNullOrEmpty(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
      }
    }

    if (localFileIOBaseDir != null) {
      m.putNotNullOrEmpty(LocalFileIO.LOCALFILEIO_PROP_BASEDIR, localFileIOBaseDir);
    }

    if (icebergProperties != null) {
      m.putAll(icebergProperties);
    }

    var uri = m.getOrDefault(CatalogProperties.URI, "").toLowerCase();
    if (uri.startsWith("jdbc:")) {
      if (uri.startsWith("jdbc:sqlite:")) {
        // https://github.com/databricks/iceberg-rest-image/issues/39
        m.putIfAbsent(CatalogProperties.CLIENT_POOL_SIZE, "1");
        var u = URI.create(m.get(CatalogProperties.URI));
        String afterScheme = u.getSchemeSpecificPart();
        String sqliteFilePrefix = "sqlite:file:";
        if (afterScheme.startsWith(sqliteFilePrefix)) {
          var f = URI.create(afterScheme.substring(sqliteFilePrefix.length()));
          if (f.getPath().contains(File.separator)) {
            // TODO: move it out of here; toIcebergConfig() should not have side-effects
            Files.createParentDirs(new File(f.getPath())); // defaults to V0
          }
        }
      }
      m.putIfAbsent(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
      // org.apache.iceberg.jdbc.JdbcUtil.SCHEMA_VERSION_PROPERTY
      var schemaVersionProperty = "jdbc.schema-version";
      if (!m.containsKey(schemaVersionProperty)) {
        // defaults to V0
        m.putIfAbsent(schemaVersionProperty, "V1");
      }
    } else if (uri.startsWith("etcd:")) {
      m.putIfAbsent(CatalogProperties.CATALOG_IMPL, EtcdCatalog.class.getName());
    }

    String warehouse = m.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");

    if (warehouse.startsWith("arn:aws:s3tables:")) {
      m.putIfAbsent(CatalogProperties.CATALOG_IMPL, CustomS3TablesCatalog.class.getName());
      m.putIfAbsent("rest.sigv4-enabled", "true");
      m.putIfAbsent(AwsProperties.REST_SIGNING_NAME, "s3tables");
      String region = warehouse.split(":")[3];
      m.putIfAbsent(AwsProperties.REST_SIGNER_REGION, region);
      m.putIfAbsent(AwsClientProperties.CLIENT_REGION, region);
    }

    if (warehouse.startsWith("file://")) {
      if (!m.containsKey(LocalFileIO.LOCALFILEIO_PROP_BASEDIR)) {
        // FIXME: wrong thing to do if warehouse is absolute
        m.put(LocalFileIO.LOCALFILEIO_PROP_BASEDIR, new File(".").getAbsolutePath());
      }
      var warehouseLocation =
          Strings.removePrefix(m.get(CatalogProperties.WAREHOUSE_LOCATION), "file://");
      File d = Paths.get(m.get(LocalFileIO.LOCALFILEIO_PROP_BASEDIR), warehouseLocation).toFile();
      ;
      // TODO: move it out of here; toIcebergConfig() should not have side-effects
      if (!d.isDirectory() && !d.mkdirs()) {
        throw new IOException("Unable to create " + d);
      }
    }

    return m;
  }
}
