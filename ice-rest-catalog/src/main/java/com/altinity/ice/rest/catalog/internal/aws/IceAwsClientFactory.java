/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.aws;

import java.util.Map;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * S3FileIOAwsClientFactory to be set as {@link S3FileIOProperties#CLIENT_FACTORY}
 * (s3.client-factory-impl) so that ice controls how S3 clients are built.
 */
public class IceAwsClientFactory implements S3FileIOAwsClientFactory {

  private AwsClientProperties awsClientProperties;
  private S3FileIOProperties s3FileIOProperties;
  private HttpClientProperties httpClientProperties;
  private Map<String, String> objectMetadata;

  public IceAwsClientFactory() {
    this.awsClientProperties = new AwsClientProperties();
    this.s3FileIOProperties = new S3FileIOProperties();
    this.httpClientProperties = new HttpClientProperties();
    this.objectMetadata = Map.of();
  }

  @Override
  public S3Client s3() {
    return S3Client.builder()
        .applyMutation(awsClientProperties::applyClientRegionConfiguration)
        .applyMutation(httpClientProperties::applyHttpClientConfigurations)
        .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
        .applyMutation(s3FileIOProperties::applyServiceConfigurations)
        .applyMutation(
            b -> s3FileIOProperties.applyCredentialConfigurations(awsClientProperties, b))
        .applyMutation(s3FileIOProperties::applySignerConfiguration)
        .applyMutation(s3FileIOProperties::applyS3AccessGrantsConfigurations)
        .applyMutation(s3FileIOProperties::applyUserAgentConfigurations)
        .applyMutation(s3FileIOProperties::applyRetryConfigurations)
        .applyMutation(this::applyObjectMetadataConfiguration)
        .build();
  }

  @Override
  public S3AsyncClient s3Async() {
    if (s3FileIOProperties.isS3CRTEnabled()) {
      return S3AsyncClient.crtBuilder()
          .applyMutation(awsClientProperties::applyClientRegionConfiguration)
          .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
          .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
          .applyMutation(s3FileIOProperties::applyS3CrtConfigurations)
          .build();
    }
    return S3AsyncClient.builder()
        .applyMutation(awsClientProperties::applyClientRegionConfiguration)
        .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
        .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
        .applyMutation(this::applyObjectMetadataConfiguration)
        .build();
  }

  private void applyObjectMetadataConfiguration(SdkClientBuilder<?, ?> builder) {
    if (objectMetadata.isEmpty()) {
      return;
    }
    builder.overrideConfiguration(
        c -> c.addExecutionInterceptor(new S3ObjectMetadataInterceptor(objectMetadata)));
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.awsClientProperties = new AwsClientProperties(properties);
    this.s3FileIOProperties = new S3FileIOProperties(properties);
    this.httpClientProperties = new HttpClientProperties(properties);
    this.objectMetadata = S3ObjectMetadataInterceptor.metadataFromProperties(properties);
  }
}
