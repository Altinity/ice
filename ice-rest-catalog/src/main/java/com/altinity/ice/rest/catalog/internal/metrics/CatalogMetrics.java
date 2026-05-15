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
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_LOCK_ACQUIRE_SECONDS_HELP;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_LOCK_ACQUIRE_SECONDS_NAME;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_LOCK_ACQUIRE_TIMEOUTS_TOTAL_HELP;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_LOCK_ACQUIRE_TIMEOUTS_TOTAL_NAME;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_LOCK_HELD_SECONDS_HELP;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_LOCK_HELD_SECONDS_NAME;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_LOCK_LABELS;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_RETRIES_TOTAL_HELP;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_RETRIES_TOTAL_NAME;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_RETRY_LABELS;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.DURATION_BUCKETS;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.LABEL_CATALOG;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
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
  private final Counter commitRetriesTotal;
  private final Histogram commitLockAcquireSeconds;
  private final Histogram commitLockHeldSeconds;
  private final Counter commitLockAcquireTimeoutsTotal;

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

    this.commitRetriesTotal =
        Counter.builder()
            .name(COMMIT_RETRIES_TOTAL_NAME)
            .help(COMMIT_RETRIES_TOTAL_HELP)
            .labelNames(COMMIT_RETRY_LABELS)
            .register();

    this.commitLockAcquireSeconds =
        Histogram.builder()
            .name(COMMIT_LOCK_ACQUIRE_SECONDS_NAME)
            .help(COMMIT_LOCK_ACQUIRE_SECONDS_HELP)
            .labelNames(COMMIT_LOCK_LABELS)
            .classicUpperBounds(DURATION_BUCKETS)
            .register();

    this.commitLockHeldSeconds =
        Histogram.builder()
            .name(COMMIT_LOCK_HELD_SECONDS_NAME)
            .help(COMMIT_LOCK_HELD_SECONDS_HELP)
            .labelNames(COMMIT_LOCK_LABELS)
            .classicUpperBounds(DURATION_BUCKETS)
            .register();

    this.commitLockAcquireTimeoutsTotal =
        Counter.builder()
            .name(COMMIT_LOCK_ACQUIRE_TIMEOUTS_TOTAL_NAME)
            .help(COMMIT_LOCK_ACQUIRE_TIMEOUTS_TOTAL_HELP)
            .labelNames(COMMIT_LOCK_LABELS)
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

  /** Record one server-side commit retry after a commit CAS conflict (CommitFailedException). */
  public void recordCommitRetry(String catalog, String namespace, String table) {
    commitRetriesTotal.labelValues(catalog, namespace, table).inc();
  }

  /** Record duration of etcd commit lock acquisition (wait time). */
  public void recordCommitLockAcquireSeconds(String catalog, double seconds) {
    commitLockAcquireSeconds.labelValues(catalog).observe(seconds);
  }

  /** Record duration the etcd commit lock was held during a commit. */
  public void recordCommitLockHeldSeconds(String catalog, double seconds) {
    commitLockHeldSeconds.labelValues(catalog).observe(seconds);
  }

  /** Record a commit lock acquire that exceeded {@code acquireTimeoutMs}. */
  public void recordCommitLockAcquireTimeout(String catalog) {
    commitLockAcquireTimeoutsTotal.labelValues(catalog).inc();
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
