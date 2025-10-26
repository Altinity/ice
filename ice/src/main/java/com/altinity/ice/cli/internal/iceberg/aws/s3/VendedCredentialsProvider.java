/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.iceberg.aws.s3;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.DefaultAuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.utils.IoUtils;
import software.amazon.awssdk.utils.SdkAutoCloseable;
import software.amazon.awssdk.utils.cache.CachedSupplier;
import software.amazon.awssdk.utils.cache.RefreshResult;

// VendedCredentialsProvider is a modified version of
// org.apache.iceberg.aws.s3.VendedCredentialsProvider
// that allows credentials without sessionToken (e.g. in case of minio).
public class VendedCredentialsProvider implements AwsCredentialsProvider, SdkAutoCloseable {

  // Copy of S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS but not package private.
  private static final String SESSION_TOKEN_EXPIRES_AT_MS = "s3.session-token-expires-at-ms";

  public static final String URI = "credentials.uri";
  private volatile HTTPClient client;
  private final Map<String, String> properties;
  private final CachedSupplier<AwsCredentials> credentialCache;

  private VendedCredentialsProvider(Map<String, String> properties) {
    Preconditions.checkArgument(null != properties, "Invalid properties: null");
    Preconditions.checkArgument(null != properties.get(URI), "Invalid URI: null");
    this.properties = properties;
    this.credentialCache =
        CachedSupplier.builder(this::refreshCredential)
            .cachedValueName(org.apache.iceberg.aws.s3.VendedCredentialsProvider.class.getName())
            .build();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return credentialCache.get();
  }

  @Override
  public void close() {
    IoUtils.closeQuietly(client, null);
    credentialCache.close();
  }

  public static VendedCredentialsProvider create(Map<String, String> properties) {
    return new VendedCredentialsProvider(properties);
  }

  private RESTClient httpClient() {
    if (null == client) {
      synchronized (this) {
        if (null == client) {
          DefaultAuthSession authSession =
              DefaultAuthSession.of(
                  HTTPHeaders.of(OAuth2Util.authHeaders(properties.get(OAuth2Properties.TOKEN))));
          client =
              HTTPClient.builder(properties)
                  .uri(properties.get(URI))
                  .withAuthSession(authSession)
                  .build();
        }
      }
    }

    return client;
  }

  private LoadCredentialsResponse fetchCredentials() {
    return httpClient()
        .get(
            properties.get(URI),
            null,
            LoadCredentialsResponse.class,
            Map.of(),
            ErrorHandlers.defaultErrorHandler());
  }

  private RefreshResult<AwsCredentials> refreshCredential() {
    LoadCredentialsResponse response = fetchCredentials();

    List<Credential> s3Credentials =
        response.credentials().stream()
            .filter(c -> c.prefix().startsWith("s3"))
            .collect(Collectors.toList());

    Preconditions.checkState(!s3Credentials.isEmpty(), "Invalid S3 Credentials: empty");
    Preconditions.checkState(
        s3Credentials.size() == 1, "Invalid S3 Credentials: only one S3 credential should exist");

    Credential s3Credential = s3Credentials.get(0);
    checkCredential(s3Credential, S3FileIOProperties.ACCESS_KEY_ID);
    checkCredential(s3Credential, S3FileIOProperties.SECRET_ACCESS_KEY);

    String accessKeyId = s3Credential.config().get(S3FileIOProperties.ACCESS_KEY_ID);
    String secretAccessKey = s3Credential.config().get(S3FileIOProperties.SECRET_ACCESS_KEY);

    if (s3Credential.config().containsKey(S3FileIOProperties.SESSION_TOKEN)) {
      String sessionToken = s3Credential.config().get(S3FileIOProperties.SESSION_TOKEN);

      Instant expiresAt = null, prefetchAt = null;
      if (s3Credential.config().containsKey(SESSION_TOKEN_EXPIRES_AT_MS)) {
        expiresAt =
            Instant.ofEpochMilli(
                Long.parseLong(s3Credential.config().get(SESSION_TOKEN_EXPIRES_AT_MS)));
        prefetchAt = expiresAt.minus(5, ChronoUnit.MINUTES);
      }

      return RefreshResult.builder(
              (AwsCredentials)
                  AwsSessionCredentials.builder()
                      .accessKeyId(accessKeyId)
                      .secretAccessKey(secretAccessKey)
                      .sessionToken(sessionToken)
                      .expirationTime(expiresAt)
                      .build())
          .staleTime(expiresAt)
          .prefetchTime(prefetchAt)
          .build();
    } else {
      return RefreshResult.builder(
              (AwsCredentials)
                  AwsBasicCredentials.builder()
                      .accessKeyId(accessKeyId)
                      .secretAccessKey(secretAccessKey)
                      .build())
          .build();
    }
  }

  private void checkCredential(Credential credential, String property) {
    Preconditions.checkState(
        credential.config().containsKey(property), "Invalid S3 Credentials: %s not set", property);
  }
}
