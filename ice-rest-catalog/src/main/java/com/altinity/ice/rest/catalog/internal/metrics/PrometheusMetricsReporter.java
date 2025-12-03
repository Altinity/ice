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

import io.prometheus.metrics.core.datapoints.DistributionDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Prometheus-based metrics reporter for Iceberg operations. This reporter exposes Iceberg scan
 * and commit metrics via Prometheus, making them available at the /metrics endpoint.
 *
 * <p>This class uses a singleton pattern because Prometheus metrics can only be registered once per
 * JVM.
 *
 * <p>All duration metrics use Histograms instead of Summaries to allow aggregation across multiple
 * instances in distributed deployments.
 */
public class PrometheusMetricsReporter {

  private static final Logger logger = LoggerFactory.getLogger(PrometheusMetricsReporter.class);

  // Singleton instance
  private static volatile PrometheusMetricsReporter instance;
  private static final Object lock = new Object();

  // Common label names
  private static final String[] SCAN_LABELS = {"catalog", "namespace", "table"};
  private static final String[] COMMIT_LABELS = {"catalog", "namespace", "table", "operation"};

  // Duration histogram buckets (in seconds) - suitable for typical Iceberg operations
  private static final double[] DURATION_BUCKETS = {
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60
  };

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

  // Info metric to confirm reporter is active
  private final Counter metricsReporterInfo;

  /** Returns the singleton instance of the metrics reporter. */
  public static PrometheusMetricsReporter getInstance() {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance = new PrometheusMetricsReporter();
        }
      }
    }
    return instance;
  }

  private PrometheusMetricsReporter() {
    // Info metric - incremented once to confirm reporter is active and visible in /metrics
    this.metricsReporterInfo =
        Counter.builder()
            .name("iceberg_metrics_reporter_info")
            .help("Iceberg metrics reporter info (value 1 means reporter is active)")
            .register();
    this.metricsReporterInfo.inc(); // Make it visible immediately

    // Error counter
    this.metricsReportErrors =
        Counter.builder()
            .name("iceberg_metrics_report_errors_total")
            .help("Total number of errors while processing metrics reports")
            .labelNames("type")
            .register();

    // Scan metrics
    this.scansTotal =
        Counter.builder()
            .name("iceberg_scans_total")
            .help("Total number of Iceberg table scans")
            .labelNames(SCAN_LABELS)
            .register();

    this.scanResultDataFiles =
        Counter.builder()
            .name("iceberg_scan_result_data_files_total")
            .help("Total number of data files in scan results")
            .labelNames(SCAN_LABELS)
            .register();

    this.scanResultDeleteFiles =
        Counter.builder()
            .name("iceberg_scan_result_delete_files_total")
            .help("Total number of delete files in scan results")
            .labelNames(SCAN_LABELS)
            .register();

    this.scanIndexedDeleteFiles =
        Counter.builder()
            .name("iceberg_scan_indexed_delete_files_total")
            .help("Total number of indexed delete files in scan results")
            .labelNames(SCAN_LABELS)
            .register();

    this.scanPositionalDeleteFiles =
        Counter.builder()
            .name("iceberg_scan_positional_delete_files_total")
            .help("Total number of positional delete files in scan results")
            .labelNames(SCAN_LABELS)
            .register();

    this.scanEqualityDeleteFiles =
        Counter.builder()
            .name("iceberg_scan_equality_delete_files_total")
            .help("Total number of equality delete files in scan results")
            .labelNames(SCAN_LABELS)
            .register();

    this.scanTotalDataManifests =
        Counter.builder()
            .name("iceberg_scan_total_data_manifests")
            .help("Total number of data manifests considered during scans")
            .labelNames(SCAN_LABELS)
            .register();

    this.scanTotalDeleteManifests =
        Counter.builder()
            .name("iceberg_scan_total_delete_manifests")
            .help("Total number of delete manifests considered during scans")
            .labelNames(SCAN_LABELS)
            .register();

    this.scanScannedDataManifests =
        Counter.builder()
            .name("iceberg_scan_scanned_data_manifests")
            .help("Total number of data manifests actually scanned")
            .labelNames(SCAN_LABELS)
            .register();

    this.scanSkippedDataManifests =
        Counter.builder()
            .name("iceberg_scan_skipped_data_manifests")
            .help("Total number of data manifests skipped during scans")
            .labelNames(SCAN_LABELS)
            .register();

    this.scanTotalFileSizeBytes =
        Counter.builder()
            .name("iceberg_scan_total_file_size_bytes")
            .help("Total file size in bytes for scanned data files")
            .labelNames(SCAN_LABELS)
            .register();

    this.scanTotalDeleteFileSizeBytes =
        Counter.builder()
            .name("iceberg_scan_total_delete_file_size_bytes")
            .help("Total file size in bytes for scanned delete files")
            .labelNames(SCAN_LABELS)
            .register();

    // Scan timing - using Histogram for aggregation across instances
    this.scanPlanningDuration =
        Histogram.builder()
            .name("iceberg_scan_planning_duration_seconds")
            .help("Duration of scan planning in seconds")
            .labelNames(SCAN_LABELS)
            .classicUpperBounds(DURATION_BUCKETS)
            .register();

    this.scanDataFilesPerScan =
        Histogram.builder()
            .name("iceberg_scan_data_files_per_scan")
            .help("Distribution of data files per scan")
            .labelNames(SCAN_LABELS)
            .classicExponentialUpperBounds(1, 2, 10)
            .register();

    // Commit metrics
    this.commitsTotal =
        Counter.builder()
            .name("iceberg_commits_total")
            .help("Total number of Iceberg table commits")
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitAddedDataFiles =
        Counter.builder()
            .name("iceberg_commit_added_data_files_total")
            .help("Total number of data files added in commits")
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitRemovedDataFiles =
        Counter.builder()
            .name("iceberg_commit_removed_data_files_total")
            .help("Total number of data files removed in commits")
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitAddedDeleteFiles =
        Counter.builder()
            .name("iceberg_commit_added_delete_files_total")
            .help("Total number of delete files added in commits")
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitRemovedDeleteFiles =
        Counter.builder()
            .name("iceberg_commit_removed_delete_files_total")
            .help("Total number of delete files removed in commits")
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitAddedRecords =
        Counter.builder()
            .name("iceberg_commit_added_records_total")
            .help("Total number of records added in commits")
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitRemovedRecords =
        Counter.builder()
            .name("iceberg_commit_removed_records_total")
            .help("Total number of records removed in commits")
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitAddedEqualityDeletes =
        Counter.builder()
            .name("iceberg_commit_added_equality_deletes_total")
            .help("Total number of equality deletes added in commits")
            .labelNames(COMMIT_LABELS)
            .register();

    this.commitTotalFilesSizeBytes =
        Counter.builder()
            .name("iceberg_commit_total_files_size_bytes")
            .help("Total size in bytes of files involved in commits")
            .labelNames(COMMIT_LABELS)
            .register();

    // Commit timing - using Histogram for aggregation across instances
    this.commitDuration =
        Histogram.builder()
            .name("iceberg_commit_duration_seconds")
            .help("Duration of commit operations in seconds")
            .labelNames(COMMIT_LABELS)
            .classicUpperBounds(DURATION_BUCKETS)
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
   * @param report the metrics report to process
   */
  public void report(MetricsReport report) {
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
