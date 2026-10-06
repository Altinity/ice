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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.Test;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3ObjectMetadataInterceptorTest {

  @Test
  public void metadataFromPropertiesIgnoresUnrelatedPropertiesAndHeaderPrefix() {
    var m =
        S3ObjectMetadataInterceptor.metadataFromProperties(
            Map.of(
                "s3.endpoint", "http://localhost:9000",
                "s3.metadata.x-amz-meta-expiration-seconds", "1000",
                "s3.metadata.owner", "ice",
                "s3.metadata.", "ignored"));
    assertThat(m).containsOnly(Map.entry("expiration-seconds", "1000"), Map.entry("owner", "ice"));
  }

  @Test
  public void putObjectAndCreateMultipartUploadGetMetadata() {
    var i = new S3ObjectMetadataInterceptor(Map.of("owner", "ice"));

    var put = (PutObjectRequest) modify(i, PutObjectRequest.builder().bucket("b").key("k").build());
    assertThat(put.metadata()).containsExactly(Map.entry("owner", "ice"));

    var mpu =
        (CreateMultipartUploadRequest)
            modify(i, CreateMultipartUploadRequest.builder().bucket("b").key("k").build());
    assertThat(mpu.metadata()).containsExactly(Map.entry("owner", "ice"));
  }

  @Test
  public void requestMetadataWins() {
    var i = new S3ObjectMetadataInterceptor(Map.of("owner", "ice", "env", "prod"));
    var put =
        (PutObjectRequest)
            modify(
                i,
                PutObjectRequest.builder()
                    .bucket("b")
                    .key("k")
                    .metadata(Map.of("owner", "explicit"))
                    .build());
    assertThat(put.metadata())
        .containsOnly(Map.entry("owner", "explicit"), Map.entry("env", "prod"));
  }

  @Test
  public void otherRequestsAreLeftAlone() {
    var i = new S3ObjectMetadataInterceptor(Map.of("owner", "ice"));
    var get = GetObjectRequest.builder().bucket("b").key("k").build();
    assertThat(modify(i, get)).isSameAs(get);
  }

  private static SdkRequest modify(S3ObjectMetadataInterceptor i, SdkRequest request) {
    return i.modifyRequest(() -> request, new ExecutionAttributes());
  }
}
