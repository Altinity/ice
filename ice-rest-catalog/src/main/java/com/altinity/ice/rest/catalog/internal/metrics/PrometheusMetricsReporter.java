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

import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.*;

import io.prometheus.metrics.core.datapoints.DistributionDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Prometheus-based metrics reporter for Iceberg operations. This reporter exposes Iceberg scan
 * and commit metrics via Prometheus, making them available at the /metrics endpoint.
 *
 * <p>This class implements {@link MetricsReporter} to follow Iceberg's standard metrics reporting
 * pattern, allowing it to be used both as a server-side reporter (receiving metrics via REST) and
 * potentially as a client-side reporter via catalog configuration.
 *
 * <p>This class uses a singleton pattern because Prometheus metrics can only be registered once per
 * JVM.
 *
 * <p>All duration metrics use Histograms instead of Summaries to allow aggregation across multiple
 * instances in distributed deployments.
 *
 * @see IcebergMetricNames for metric names and help strings
 * @see <a href="https://iceberg.apache.org/docs/latest/metrics-reporting/">Iceberg Metrics
 *     Reporting</a>
 */
public class PrometheusMetricsReporter implements MetricsReporter {

  private static final Logger logger = LoggerFactory.getLogger(PrometheusMetricsReporter.class);

  private static class Holder {
    private static final PrometheusMetricsReporter INSTANCE = new PrometheusMetricsReporter();
  }

  // Scan metrics counters
  private final Counter scansTotal;
  private final Counter scanResultDataFiles;
  private final Counter scanResultDeleteFiles;
  private final Counter scanIndexedDeleteFiles;
  private final Counter scanPositionalDeleteFiles;
  private final Counter scanEqualityDeleteFiles;
  private final Counter scanTotalDataManifests;
  private final Counter scanTotalDeleteManifests;
  private final Counter scanScannedDataManifests;
  private final Counter scanSkippedDataManifests;
  private final Counter scanTotalFileSizeBytes;
  private final Counter scanTotalDeleteFileSizeBytes;

  // Scan timing metrics (Histogram for aggregation across instances)
  private final Histogram scanPlanningDuration;
  private final Histogram scanDataFilesPerScan;

  // Commit metrics counters
  private final Counter commitsTotal;
  private final Counter commitAddedDataFiles;
  private final Counter commitRemovedDataFiles;
  private final Counter commitAddedDeleteFiles;
  private final Counter commitRemovedDeleteFiles;
  private final Counter commitAddedRecords;
  private final Counter commitRemovedRecords;
  private final Counter commitAddedEqualityDeletes;
  private final Counter commitTotalFilesSizeBytes;

  // Commit timing metrics (Histogram for aggregation across instances)
  private final Histogram commitDuration;

  // Error counter
  private final Counter metricsReportErrors;

  // Status metric to confirm reporter is active
  private final Counter metricsReporterActive;

  // Table-level metrics
  private final Counter tableSnapshotsTotal;
  private final Counter tableSchemaUpdatesTotal;

  /** Returns the singleton instance of the metrics reporter. */
  public static PrometheusMetricsReporter getInstance() {
    return Holder.INSTANCE;
  }

  private PrometheusMetricsReporter() {
    // Status metric - incremented once to confirm reporter is active and visible in /metrics
    this.metricsReporterActive =
        Counter.builder().name(REPORTER_ACTIVE_NAME).help(REPORTER_ACTIVE_HELP).register();
    this.metricsReporterActive.inc(); // Make it visible immediately

    // Error counter
    this.metricsReportErrors =
        Counter.builder()
            .name(REPORT_ERRORS_NAME)
            .help(REPORT_ERRORS_HELP)
            .labelNames(LABEL_TYPE)
            .register();

    // Scan metrics
    this.scansTotal =
        Counter.builder()
            .name(SCANS_TOTAL_NAME)
            .help(SCANS_TOTAL_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.scanResultDataFiles =
        Counter.builder()
            .name(SCAN_RESULT_DATA_FILES_NAME)
            .help(SCAN_RESULT_DATA_FILES_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.scanResultDeleteFiles =
        Counter.builder()
            .name(SCAN_RESULT_DELETE_FILES_NAME)
            .help(SCAN_RESULT_DELETE_FILES_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.scanIndexedDeleteFiles =
        Counter.builder()
            .name(SCAN_INDEXED_DELETE_FILES_NAME)
            .help(SCAN_INDEXED_DELETE_FILES_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.scanPositionalDeleteFiles =
        Counter.builder()
            .name(SCAN_POSITIONAL_DELETE_FILES_NAME)
            .help(SCAN_POSITIONAL_DELETE_FILES_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.scanEqualityDeleteFiles =
        Counter.builder()
            .name(SCAN_EQUALITY_DELETE_FILES_NAME)
            .help(SCAN_EQUALITY_DELETE_FILES_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.scanTotalDataManifests =
        Counter.builder()
            .name(SCAN_TOTAL_DATA_MANIFESTS_NAME)
            .help(SCAN_TOTAL_DATA_MANIFESTS_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.scanTotalDeleteManifests =
        Counter.builder()
            .name(SCAN_TOTAL_DELETE_MANIFESTS_NAME)
            .help(SCAN_TOTAL_DELETE_MANIFESTS_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.scanScannedDataManifests =
        Counter.builder()
            .name(SCAN_SCANNED_DATA_MANIFESTS_NAME)
            .help(SCAN_SCANNED_DATA_MANIFESTS_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.scanSkippedDataManifests =
        Counter.builder()
            .name(SCAN_SKIPPED_DATA_MANIFESTS_NAME)
            .help(SCAN_SKIPPED_DATA_MANIFESTS_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.scanTotalFileSizeBytes =
        Counter.builder()
            .name(SCAN_TOTAL_FILE_SIZE_BYTES_NAME)
            .help(SCAN_TOTAL_FILE_SIZE_BYTES_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.scanTotalDeleteFileSizeBytes =
        Counter.builder()
            .name(SCAN_TOTAL_DELETE_FILE_SIZE_BYTES_NAME)
            .help(SCAN_TOTAL_DELETE_FILE_SIZE_BYTES_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    // Scan timing - using Histogram for aggregation across instances
    this.scanPlanningDuration =
        Histogram.builder()
            .name(SCAN_PLANNING_DURATION_NAME)
            .help(SCAN_PLANNING_DURATION_HELP)
            .labelNames(SCAN_LABELS)
            .classicUpperBounds(DURATION_BUCKETS)
            .register();

    this.scanDataFilesPerScan =
        Histogram.builder()
            .name(SCAN_DATA_FILES_PER_SCAN_NAME)
            .help(SCAN_DATA_FILES_PER_SCAN_HELP)
            .labelNames(SCAN_LABELS)
            .classicExponentialUpperBounds(1, 2, 10)
            .register();

    // Commit metrics
    this.commitsTotal =
        Counter.builder()
            .name(COMMITS_TOTAL_NAME)
            .help(COMMITS_TOTAL_HELP)
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitAddedDataFiles =
        Counter.builder()
            .name(COMMIT_ADDED_DATA_FILES_NAME)
            .help(COMMIT_ADDED_DATA_FILES_HELP)
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitRemovedDataFiles =
        Counter.builder()
            .name(COMMIT_REMOVED_DATA_FILES_NAME)
            .help(COMMIT_REMOVED_DATA_FILES_HELP)
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitAddedDeleteFiles =
        Counter.builder()
            .name(COMMIT_ADDED_DELETE_FILES_NAME)
            .help(COMMIT_ADDED_DELETE_FILES_HELP)
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitRemovedDeleteFiles =
        Counter.builder()
            .name(COMMIT_REMOVED_DELETE_FILES_NAME)
            .help(COMMIT_REMOVED_DELETE_FILES_HELP)
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitAddedRecords =
        Counter.builder()
            .name(COMMIT_ADDED_RECORDS_NAME)
            .help(COMMIT_ADDED_RECORDS_HELP)
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitRemovedRecords =
        Counter.builder()
            .name(COMMIT_REMOVED_RECORDS_NAME)
            .help(COMMIT_REMOVED_RECORDS_HELP)
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitAddedEqualityDeletes =
        Counter.builder()
            .name(COMMIT_ADDED_EQUALITY_DELETES_NAME)
            .help(COMMIT_ADDED_EQUALITY_DELETES_HELP)
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitTotalFilesSizeBytes =
        Counter.builder()
            .name(COMMIT_TOTAL_FILES_SIZE_BYTES_NAME)
            .help(COMMIT_TOTAL_FILES_SIZE_BYTES_HELP)
            .labelNames(COMMIT_LABELS)
            .register();

    // Commit timing - using Histogram for aggregation across instances
    this.commitDuration =
        Histogram.builder()
            .name(COMMIT_DURATION_NAME)
            .help(COMMIT_DURATION_HELP)
            .labelNames(COMMIT_LABELS)
            .classicUpperBounds(DURATION_BUCKETS)
            .register();

    // Table-level metrics for snapshots and schema evolution
    this.tableSnapshotsTotal =
        Counter.builder()
            .name(TABLE_SNAPSHOTS_TOTAL_NAME)
            .help(TABLE_SNAPSHOTS_TOTAL_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    this.tableSchemaUpdatesTotal =
        Counter.builder()
            .name(TABLE_SCHEMA_UPDATES_TOTAL_NAME)
            .help(TABLE_SCHEMA_UPDATES_TOTAL_HELP)
            .labelNames(SCAN_LABELS)
            .register();

    logger.info("Prometheus Iceberg metrics reporter initialized");
  }

  /**
   * Report metrics from an Iceberg MetricsReport.
   *
   * @param catalogName the catalog name for multi-catalog deployments
   * @param report the metrics report to process
   */
  public void report(String catalogName, MetricsReport report) {
    logger.debug("Reporting metrics report: catalogName: {}, report: {}", catalogName, report);
    if (report == null) {
      return;
    }

    String catalog = catalogName != null ? catalogName : "default";

    try {
      if (report instanceof ScanReport) {
        reportScanMetrics(catalog, (ScanReport) report);
      } else if (report instanceof CommitReport) {
        reportCommitMetrics(catalog, (CommitReport) report);
      } else {
        logger.debug("Unknown metrics report type: {}", report.getClass().getName());
      }
    } catch (Exception e) {
      String reportType = report.getClass().getSimpleName();
      logger.warn("Error processing {} metrics report: {}", reportType, e.getMessage());
      metricsReportErrors.labelValues(reportType).inc();
    }
  }

  /**
   * Report metrics from an Iceberg MetricsReport using default catalog name.
   *
   * <p>This method implements {@link MetricsReporter#report(MetricsReport)}.
   *
   * @param report the metrics report to process
   */
  @Override
  public void report(MetricsReport report) {
    logger.debug("Reporting metrics report: {}", report);
    report(null, report);
  }

  private void reportScanMetrics(String catalog, ScanReport report) {
    String fullTableName = report.tableName();
    String namespace = extractNamespace(fullTableName);
    String table = extractTableName(fullTableName);

    logger.debug(
        "Recording scan metrics for catalog: {}, namespace: {}, table: {}",
        catalog,
        namespace,
        table);

    scansTotal.labelValues(catalog, namespace, table).inc();

    var metrics = report.scanMetrics();
    if (metrics == null) {
      return;
    }

    // File counts
    if (metrics.resultDataFiles() != null) {
      long count = metrics.resultDataFiles().value();
      scanResultDataFiles.labelValues(catalog, namespace, table).inc(count);
      scanDataFilesPerScan.labelValues(catalog, namespace, table).observe(count);
    }

    if (metrics.resultDeleteFiles() != null) {
      scanResultDeleteFiles
          .labelValues(catalog, namespace, table)
          .inc(metrics.resultDeleteFiles().value());
    }

    // Additional delete file metrics (if available in the Iceberg version)
    if (metrics.indexedDeleteFiles() != null) {
      scanIndexedDeleteFiles
          .labelValues(catalog, namespace, table)
          .inc(metrics.indexedDeleteFiles().value());
    }

    if (metrics.positionalDeleteFiles() != null) {
      scanPositionalDeleteFiles
          .labelValues(catalog, namespace, table)
          .inc(metrics.positionalDeleteFiles().value());
    }

    if (metrics.equalityDeleteFiles() != null) {
      scanEqualityDeleteFiles
          .labelValues(catalog, namespace, table)
          .inc(metrics.equalityDeleteFiles().value());
    }

    // Manifest counts
    if (metrics.totalDataManifests() != null) {
      scanTotalDataManifests
          .labelValues(catalog, namespace, table)
          .inc(metrics.totalDataManifests().value());
    }

    if (metrics.totalDeleteManifests() != null) {
      scanTotalDeleteManifests
          .labelValues(catalog, namespace, table)
          .inc(metrics.totalDeleteManifests().value());
    }

    if (metrics.scannedDataManifests() != null) {
      scanScannedDataManifests
          .labelValues(catalog, namespace, table)
          .inc(metrics.scannedDataManifests().value());
    }

    if (metrics.skippedDataManifests() != null) {
      scanSkippedDataManifests
          .labelValues(catalog, namespace, table)
          .inc(metrics.skippedDataManifests().value());
    }

    // File sizes
    if (metrics.totalFileSizeInBytes() != null) {
      scanTotalFileSizeBytes
          .labelValues(catalog, namespace, table)
          .inc(metrics.totalFileSizeInBytes().value());
    }

    if (metrics.totalDeleteFileSizeInBytes() != null) {
      scanTotalDeleteFileSizeBytes
          .labelValues(catalog, namespace, table)
          .inc(metrics.totalDeleteFileSizeInBytes().value());
    }

    // Timing metrics
    if (metrics.totalPlanningDuration() != null) {
      observeDuration(
          scanPlanningDuration.labelValues(catalog, namespace, table),
          metrics.totalPlanningDuration().totalDuration());
    }
  }

  private void reportCommitMetrics(String catalog, CommitReport report) {
    String fullTableName = report.tableName();
    String namespace = extractNamespace(fullTableName);
    String table = extractTableName(fullTableName);
    String operation = report.operation() != null ? report.operation() : "unknown";

    logger.debug(
        "Recording commit metrics for catalog: {}, namespace: {}, table: {}, operation: {}",
        catalog,
        namespace,
        table,
        operation);

    commitsTotal.labelValues(catalog, namespace, table, operation).inc();

    var metrics = report.commitMetrics();
    if (metrics == null) {
      return;
    }

    if (metrics.addedDataFiles() != null) {
      commitAddedDataFiles
          .labelValues(catalog, namespace, table, operation)
          .inc(metrics.addedDataFiles().value());
    }

    if (metrics.removedDataFiles() != null) {
      commitRemovedDataFiles
          .labelValues(catalog, namespace, table, operation)
          .inc(metrics.removedDataFiles().value());
    }

    if (metrics.addedDeleteFiles() != null) {
      commitAddedDeleteFiles
          .labelValues(catalog, namespace, table, operation)
          .inc(metrics.addedDeleteFiles().value());
    }

    if (metrics.removedDeleteFiles() != null) {
      commitRemovedDeleteFiles
          .labelValues(catalog, namespace, table, operation)
          .inc(metrics.removedDeleteFiles().value());
    }

    if (metrics.addedRecords() != null) {
      commitAddedRecords
          .labelValues(catalog, namespace, table, operation)
          .inc(metrics.addedRecords().value());
    }

    if (metrics.removedRecords() != null) {
      commitRemovedRecords
          .labelValues(catalog, namespace, table, operation)
          .inc(metrics.removedRecords().value());
    }

    if (metrics.addedEqualityDeletes() != null) {
      commitAddedEqualityDeletes
          .labelValues(catalog, namespace, table, operation)
          .inc(metrics.addedEqualityDeletes().value());
    }

    if (metrics.totalFilesSizeInBytes() != null) {
      commitTotalFilesSizeBytes
          .labelValues(catalog, namespace, table, operation)
          .inc(metrics.totalFilesSizeInBytes().value());
    }

    if (metrics.totalDuration() != null) {
      observeDuration(
          commitDuration.labelValues(catalog, namespace, table, operation),
          metrics.totalDuration().totalDuration());
    }

    tableSnapshotsTotal.labelValues(catalog, namespace, table).inc();
  }

  /**
   * Record a schema update for a table. Called from RESTCatalogAdapter when UpdateTableRequest
   * contains schema-related MetadataUpdate objects (AddSchema, SetCurrentSchema, etc.).
   */
  public void recordSchemaUpdate(String catalog, String namespace, String table) {
    tableSchemaUpdatesTotal.labelValues(catalog, namespace, table).inc();
  }

  private void observeDuration(DistributionDataPoint dataPoint, java.time.Duration duration) {
    if (duration != null) {
      double seconds = duration.toNanos() / (double) TimeUnit.SECONDS.toNanos(1);
      dataPoint.observe(seconds);
    }
  }

  /**
   * Extracts the namespace from a full table name. Handles various formats:
   *
   * <ul>
   *   <li>"namespace.table" -> "namespace"
   *   <li>"db.schema.table" -> "db.schema"
   *   <li>"table" -> "default"
   * </ul>
   */
  private String extractNamespace(String fullTableName) {
    if (fullTableName == null || fullTableName.isEmpty()) {
      return "unknown";
    }
    String[] parts = fullTableName.split("\\.");
    if (parts.length > 1) {
      return String.join(".", Arrays.copyOf(parts, parts.length - 1));
    }
    return "default";
  }

  /**
   * Extracts the table name from a full table name. Handles various formats:
   *
   * <ul>
   *   <li>"namespace.table" -> "table"
   *   <li>"db.schema.table" -> "table"
   *   <li>"table" -> "table"
   * </ul>
   */
  private String extractTableName(String fullTableName) {
    if (fullTableName == null || fullTableName.isEmpty()) {
      return "unknown";
    }
    String[] parts = fullTableName.split("\\.");
    return parts[parts.length - 1];
  }
}
