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

import com.altinity.ice.internal.strings.Strings;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Adds user-defined object metadata (sent as x-amz-meta-* headers) to every S3 request that creates
 * an object.
 */
public final class S3ObjectMetadataInterceptor implements ExecutionInterceptor {

  public static final String METADATA_PREFIX = "s3.metadata.";

  private static final String HEADER_PREFIX = "x-amz-meta-";

  private final Map<String, String> metadata;

  public S3ObjectMetadataInterceptor(Map<String, String> metadata) {
    this.metadata = Map.copyOf(metadata);
  }

  /**
   * Extracts object metadata from catalog properties. Keys are stripped of METADATA_PREFIX
   * (both x-amz-meta-foo and foo result in the x-amz-meta-foo header).
   */
  public static Map<String, String> metadataFromProperties(Map<String, String> properties) {
    Map<String, String> m = new LinkedHashMap<>();
    for (Map.Entry<String, String> e : properties.entrySet()) {
      String k = e.getKey();
      if (!k.startsWith(METADATA_PREFIX)) {
        continue;
      }
      k = Strings.removePrefix(k, METADATA_PREFIX);
      if (k.toLowerCase().startsWith(HEADER_PREFIX)) {
        k = k.substring(HEADER_PREFIX.length());
      }
      if (k.isEmpty() || e.getValue() == null) {
        continue;
      }
      m.put(k, e.getValue());
    }
    return m;
  }

  @Override
  public SdkRequest modifyRequest(Context.ModifyRequest context, ExecutionAttributes attrs) {
    SdkRequest request = context.request();
    if (request instanceof PutObjectRequest r) {
      return r.toBuilder().metadata(merge(r.hasMetadata() ? r.metadata() : Map.of())).build();
    }
    if (request instanceof CreateMultipartUploadRequest r) {
      return r.toBuilder().metadata(merge(r.hasMetadata() ? r.metadata() : Map.of())).build();
    }
    return request;
  }

  private Map<String, String> merge(Map<String, String> requestMetadata) {
    Map<String, String> m = new HashMap<>(metadata);
    m.putAll(requestMetadata);
    return m;
  }
}
