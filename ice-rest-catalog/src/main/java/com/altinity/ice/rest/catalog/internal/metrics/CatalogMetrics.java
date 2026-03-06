/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.metrics;

import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.CATALOG_NAMESPACES_HELP;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.CATALOG_NAMESPACES_NAME;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.CATALOG_OPERATIONS_TOTAL_HELP;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.CATALOG_OPERATIONS_TOTAL_NAME;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.CATALOG_OPERATION_LABELS;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.CATALOG_TABLES_HELP;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.CATALOG_TABLES_NAME;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.LABEL_CATALOG;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prometheus metrics for catalog-level statistics.
 *
 * <p>Tracks:
 *
 * <ul>
 *   <li>Total number of tables (gauge)
 *   <li>Total number of namespaces (gauge)
 *   <li>Catalog operations counter (create/drop table/namespace)
 * </ul>
 */
public class CatalogMetrics {

  private static final Logger logger = LoggerFactory.getLogger(CatalogMetrics.class);

  // Initialization-on-Demand Holder for thread-safe lazy singleton
  private static class Holder {
    private static final CatalogMetrics INSTANCE = new CatalogMetrics();
  }

  // Operation types for the operations counter
  public static final String OP_CREATE_TABLE = "create_table";
  public static final String OP_DROP_TABLE = "drop_table";
  public static final String OP_CREATE_NAMESPACE = "create_namespace";
  public static final String OP_DROP_NAMESPACE = "drop_namespace";

  private final Gauge tablesTotal;
  private final Gauge namespacesTotal;
  private final Counter operationsTotal;

  /** Returns the singleton instance of the catalog metrics. */
  public static CatalogMetrics getInstance() {
    return Holder.INSTANCE;
  }

  private CatalogMetrics() {
    this.tablesTotal =
        Gauge.builder()
            .name(CATALOG_TABLES_NAME)
            .help(CATALOG_TABLES_HELP)
            .labelNames(LABEL_CATALOG)
            .register();

    this.namespacesTotal =
        Gauge.builder()
            .name(CATALOG_NAMESPACES_NAME)
            .help(CATALOG_NAMESPACES_HELP)
            .labelNames(LABEL_CATALOG)
            .register();

    this.operationsTotal =
        Counter.builder()
            .name(CATALOG_OPERATIONS_TOTAL_NAME)
            .help(CATALOG_OPERATIONS_TOTAL_HELP)
            .labelNames(CATALOG_OPERATION_LABELS)
            .register();

    logger.info("Catalog Prometheus metrics initialized");
  }

  /** Set the total number of tables in the catalog. */
  public void setTablesTotal(String catalog, long count) {
    tablesTotal.labelValues(catalog).set(count);
  }

  /** Increment the total number of tables in the catalog. */
  public void incrementTablesTotal(String catalog) {
    tablesTotal.labelValues(catalog).inc();
  }

  /** Decrement the total number of tables in the catalog. */
  public void decrementTablesTotal(String catalog) {
    tablesTotal.labelValues(catalog).dec();
  }

  /** Set the total number of namespaces in the catalog. */
  public void setNamespacesTotal(String catalog, long count) {
    namespacesTotal.labelValues(catalog).set(count);
  }

  /** Increment the total number of namespaces in the catalog. */
  public void incrementNamespacesTotal(String catalog) {
    namespacesTotal.labelValues(catalog).inc();
  }

  /** Decrement the total number of namespaces in the catalog. */
  public void decrementNamespacesTotal(String catalog) {
    namespacesTotal.labelValues(catalog).dec();
  }

  /** Record a catalog operation. */
  public void recordOperation(String catalog, String operation) {
    operationsTotal.labelValues(catalog, operation).inc();
  }

  /** Record a table creation. */
  public void recordTableCreated(String catalog) {
    incrementTablesTotal(catalog);
    recordOperation(catalog, OP_CREATE_TABLE);
  }

  /** Record a table drop. */
  public void recordTableDropped(String catalog) {
    decrementTablesTotal(catalog);
    recordOperation(catalog, OP_DROP_TABLE);
  }

  /** Record a namespace creation. */
  public void recordNamespaceCreated(String catalog) {
    incrementNamespacesTotal(catalog);
    recordOperation(catalog, OP_CREATE_NAMESPACE);
  }

  /** Record a namespace drop. */
  public void recordNamespaceDropped(String catalog) {
    decrementNamespacesTotal(catalog);
    recordOperation(catalog, OP_DROP_NAMESPACE);
  }
}
