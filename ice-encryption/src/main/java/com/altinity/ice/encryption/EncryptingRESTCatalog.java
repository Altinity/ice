/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.encryption;

import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.EncryptingRESTSessionCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTSessionCatalog;

/**
 * {@link RESTCatalog} that supports Iceberg table encryption (tables with the {@code
 * encryption.key-id} property), which the upstream REST client does not implement.
 *
 * <p>The KMS is configured with the standard catalog properties {@code encryption.kms-type} or
 * {@code encryption.kms-impl}. Tables without {@code encryption.key-id} behave exactly as with
 * {@link RESTCatalog}.
 *
 * <p>Spark: {@code spark.sql.catalog.<name>.catalog-impl=com.altinity.ice.encryption
 * .EncryptingRESTCatalog}.
 */
public class EncryptingRESTCatalog extends RESTCatalog {

  public EncryptingRESTCatalog() {
    super();
  }

  public EncryptingRESTCatalog(
      SessionCatalog.SessionContext context,
      Function<Map<String, String>, RESTClient> clientBuilder) {
    super(context, clientBuilder);
  }

  @Override
  protected RESTSessionCatalog newSessionCatalog(
      Function<Map<String, String>, RESTClient> clientBuilder) {
    return new EncryptingRESTSessionCatalog(clientBuilder, null);
  }
}
