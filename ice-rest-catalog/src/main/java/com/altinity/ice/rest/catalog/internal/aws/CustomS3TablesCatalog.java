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

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

public class CustomS3TablesCatalog extends S3TablesCatalog {

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (!namespace.isEmpty()) {
      // Do not fail if client sends GET ?parent=$ns request (even if S3 Table buckets don't support
      // nested namespaces).
      return new ArrayList<>();
    }
    return super.listNamespaces(namespace);
  }
}
