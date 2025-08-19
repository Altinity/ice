/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.maintenance;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public record MaintenanceRunner(Catalog catalog, Collection<MaintenanceJob> jobs) {

  private static final Logger logger = LoggerFactory.getLogger(MaintenanceRunner.class);

  public void run() throws IOException {
    List<Namespace> namespaces;
    if (catalog instanceof SupportsNamespaces nsCatalog) {
      namespaces = nsCatalog.listNamespaces();
    } else {
      throw new UnsupportedOperationException("Catalog does not support namespace operations");
    }

    logger.info("Performing catalog maintenance");
    for (Namespace namespace : namespaces) {
      List<TableIdentifier> tables = catalog.listTables(namespace);
      for (TableIdentifier tableIdent : tables) {
        Table table = catalog.loadTable(tableIdent);
        logger.info("Performing maintenance on table: {}", table.name());
        MDC.put("msgContext", table.name() + ": ");
        try {
          for (MaintenanceJob job : jobs) {
            job.perform(table);
          }
        } finally {
          MDC.remove("msgContext");
        }
      }
    }
    logger.info("Catalog maintenance completed");
  }
}
