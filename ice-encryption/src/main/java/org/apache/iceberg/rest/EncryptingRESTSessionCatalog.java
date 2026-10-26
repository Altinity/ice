/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.iceberg.rest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

// Lives in org.apache.iceberg.rest because RESTTableOperations is package-private.
public class EncryptingRESTSessionCatalog extends RESTSessionCatalog {

  private KeyManagementClient kmsClient;

  public EncryptingRESTSessionCatalog(
      Function<Map<String, String>, RESTClient> clientBuilder,
      BiFunction<SessionCatalog.SessionContext, Map<String, String>, FileIO> ioBuilder) {
    super(clientBuilder, ioBuilder);
  }

  @Override
  public void initialize(String name, Map<String, String> unresolved) {
    super.initialize(name, unresolved);
    Map<String, String> props = properties();
    if (props.containsKey(CatalogProperties.ENCRYPTION_KMS_TYPE)
        || props.containsKey(CatalogProperties.ENCRYPTION_KMS_IMPL)) {
      this.kmsClient = EncryptionUtil.createKmsClient(props);
    }
  }

  @Override
  protected RESTTableOperations newTableOps(
      RESTClient restClient,
      String path,
      Supplier<Map<String, String>> readHeaders,
      Supplier<Map<String, String>> mutationHeaderSupplier,
      FileIO fileIO,
      TableMetadata current,
      Set<Endpoint> supportedEndpoints) {
    return newTableOps(
        restClient,
        path,
        readHeaders,
        mutationHeaderSupplier,
        fileIO,
        RESTTableOperations.UpdateType.SIMPLE,
        Lists.newArrayList(),
        current,
        supportedEndpoints);
  }

  @Override
  protected RESTTableOperations newTableOps(
      RESTClient restClient,
      String path,
      Supplier<Map<String, String>> readHeaders,
      Supplier<Map<String, String>> mutationHeaderSupplier,
      FileIO fileIO,
      RESTTableOperations.UpdateType updateType,
      List<MetadataUpdate> createChanges,
      TableMetadata current,
      Set<Endpoint> supportedEndpoints) {
    return new EncryptingRESTTableOperations(
        restClient,
        path,
        readHeaders,
        mutationHeaderSupplier,
        fileIO,
        updateType,
        createChanges,
        current,
        supportedEndpoints,
        kmsClient);
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      if (kmsClient != null) {
        kmsClient.close();
      }
    }
  }
}
