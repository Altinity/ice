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
import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsSessionCredentialsIdentity;

public class RESTCatalogMiddlewareTableAWCredentials extends RESTCatalogMiddleware {

  private final Function<String, AwsCredentialsProvider> awsCredentialsProvider;

  public RESTCatalogMiddlewareTableAWCredentials(
      RESTCatalogHandler next, Function<String, AwsCredentialsProvider> awsCredentialsProvider) {
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
    T restResponse = next.handle(session, route, vars, requestBody, responseType);
    if (restResponse instanceof LoadTableResponse loadTableResponse) {
      Map<String, String> tableConfig = loadTableResponse.config();
      applyCredentials(session, tableConfig);
    }
    return restResponse;
  }

  private void applyCredentials(Session session, Map<String, String> tableConfig) {
    String providerLookupKey = null;
    if (session != null) {
      providerLookupKey = session.uid();
    }
    AwsCredentialsProvider credentialsProvider = awsCredentialsProvider.apply(providerLookupKey);
    Preconditions.checkState(credentialsProvider != null); // null here means misconfiguration
    AwsCredentials awsCredentials = credentialsProvider.resolveCredentials();
    tableConfig.put(S3FileIOProperties.ACCESS_KEY_ID, awsCredentials.accessKeyId());
    tableConfig.put(S3FileIOProperties.SECRET_ACCESS_KEY, awsCredentials.secretAccessKey());
    if (awsCredentials instanceof AwsSessionCredentialsIdentity) {
      tableConfig.put(
          S3FileIOProperties.SESSION_TOKEN,
          ((AwsSessionCredentialsIdentity) awsCredentials).sessionToken());
    }
  }
}
