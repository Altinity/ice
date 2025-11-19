/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.rest;

import com.altinity.ice.rest.catalog.internal.auth.Session;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsSessionCredentialsIdentity;

public class RESTCatalogMiddlewareCredentials extends RESTCatalogMiddleware {

  private static final ImmutableLoadCredentialsResponse EMPTY_RESPONSE =
      ImmutableLoadCredentialsResponse.builder().build();
  private final Function<String, AwsCredentialsProvider> awsCredentialsProvider;

  public RESTCatalogMiddlewareCredentials(
      RESTCatalogHandler next,
      @Nullable Function<String, AwsCredentialsProvider> awsCredentialsProvider) {
    super(next);
    this.awsCredentialsProvider = awsCredentialsProvider;
  }

  @Override
  public <T extends RESTResponse> T handle(
      Session session,
      Route route,
      Map<String, String> vars,
      Object requestBody,
      Class<T> responseType) {
    if (route == Route.CREDENTIALS) {
      LoadCredentialsResponse res;
      if (awsCredentialsProvider != null) {
        Map<String, String> config = new HashMap<>();
        applyCredentials(session, config);
        res =
            ImmutableLoadCredentialsResponse.builder()
                .addCredentials(
                    ImmutableCredential.builder().prefix("s3").putAllConfig(config).build())
                .build();
      } else {
        res = EMPTY_RESPONSE;
      }
      return responseType.cast(res);
    }
    return next.handle(session, route, vars, requestBody, responseType);
  }

  private void applyCredentials(Session session, Map<String, String> config) {
    String providerLookupKey = null;
    if (session != null) {
      providerLookupKey = session.uid();
    }
    AwsCredentialsProvider credentialsProvider = awsCredentialsProvider.apply(providerLookupKey);
    Preconditions.checkState(credentialsProvider != null); // null here means misconfiguration
    AwsCredentials awsCredentials = credentialsProvider.resolveCredentials();
    config.put(S3FileIOProperties.ACCESS_KEY_ID, awsCredentials.accessKeyId());
    config.put(S3FileIOProperties.SECRET_ACCESS_KEY, awsCredentials.secretAccessKey());
    if (awsCredentials instanceof AwsSessionCredentialsIdentity awsSessionCredentials) {
      config.put(S3FileIOProperties.SESSION_TOKEN, awsSessionCredentials.sessionToken());
      // S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS
      awsSessionCredentials
          .expirationTime()
          .ifPresent(
              exp ->
                  config.put("s3.session-token-expires-at-ms", String.valueOf(exp.toEpochMilli())));
    }
  }
}
