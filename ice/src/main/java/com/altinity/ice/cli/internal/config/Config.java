/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.config;

import com.altinity.ice.internal.iceberg.io.LocalFileIO;
import com.altinity.ice.internal.iceberg.io.SchemeFileIO;
import com.altinity.ice.internal.strings.Strings;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

public record Config(
    @JsonPropertyDescription("URL of Iceberg REST Catalog (http://localhost:5000 by default)")
        String uri,
    @JsonPropertyDescription("Bearer token to authorizer requests with") String bearerToken,
    @JsonPropertyDescription(
            "/path/to/dir where to store downloaded files when `ice insert`ing from http:// & https://")
        String httpCacheDir,
    @JsonPropertyDescription("/path/to/dir to serve as a root for warehouse=file://...")
        String localFileIOBaseDir,
    @JsonPropertyDescription("Settings for warehouse=s3://...") S3 s3,
    @JsonPropertyDescription(
            "(experimental) Iceberg properties (see https://iceberg.apache.org/javadoc/1.8.1/"
                + "; e.g. https://iceberg.apache.org/javadoc/1.8.1/org/apache/iceberg/aws/s3/S3FileIOProperties.html)")
        Map<String, String> icebergProperties) {

  // TODO: remove
  public static final String OPTION_HTTP_CACHE = "ice.http.cache";

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

  public static Config load(String configFile) throws IOException {
    boolean configFileGiven = !Strings.isNullOrEmpty(configFile);
    return com.altinity.ice.internal.config.Config.load(
        configFileGiven ? configFile : ".ice.yaml",
        configFileGiven,
        System.getenv("ICE_CONFIG_YAML"),
        new TypeReference<>() {});
  }

  public Map<String, String> toIcebergConfig() {
    var m =
        new HashMap<String, String>() {

          public void putNotNullOrEmpty(String key, String value) {
            if (Strings.isNullOrEmpty(value)) {
              return;
            }
            put(key, value);
          }
        };

    String iceURIEnvVar = System.getenv("ICE_URI");
    m.put(
        CatalogProperties.URI,
        !Strings.isNullOrEmpty(uri)
            ? uri
            : !Strings.isNullOrEmpty(iceURIEnvVar) ? iceURIEnvVar : "http://localhost:5000");
    m.put(CatalogProperties.FILE_IO_IMPL, SchemeFileIO.class.getName());
    // read by org.apache.iceberg.rest.RESTSessionCatalog.configHeaders
    m.putNotNullOrEmpty(
        "header.authorization",
        !Strings.isNullOrEmpty(bearerToken) ? String.format("bearer %s", bearerToken) : null);

    if (s3 != null) {
      m.putNotNullOrEmpty(S3FileIOProperties.ENDPOINT, s3.endpoint);
      m.putNotNullOrEmpty(S3FileIOProperties.ACCESS_KEY_ID, s3.accessKeyID);
      m.putNotNullOrEmpty(S3FileIOProperties.SECRET_ACCESS_KEY, s3.secretAccessKey);
      m.putNotNullOrEmpty(AwsClientProperties.CLIENT_REGION, s3.region);
      if (s3.pathStyleAccess) {
        m.putNotNullOrEmpty(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
      }
    }

    m.putNotNullOrEmpty(LocalFileIO.LOCALFILEIO_PROP_BASEDIR, localFileIOBaseDir);
    m.putNotNullOrEmpty(OPTION_HTTP_CACHE, httpCacheDir);

    if (icebergProperties != null) {
      m.putAll(icebergProperties);
    }

    return m;
  }
}
