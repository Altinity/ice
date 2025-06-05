/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.cmd;

import java.io.IOException;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DeleteNamespace {

  private static final Logger logger = LoggerFactory.getLogger(DeleteNamespace.class);

  private DeleteNamespace() {}

  public static void run(RESTCatalog catalog, Namespace ns, boolean ignoreNotFound)
      throws IOException {
    if (!catalog.dropNamespace(ns)) {
      if (!ignoreNotFound) {
        throw new NotFoundException(String.format("Namespace %s not found", ns));
      }
      logger.warn("Namespace {} not found", ns);
    }
  }
}
