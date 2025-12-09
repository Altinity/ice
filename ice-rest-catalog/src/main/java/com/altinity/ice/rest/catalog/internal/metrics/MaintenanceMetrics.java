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

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceMetrics {

  private static final Logger logger = LoggerFactory.getLogger(MaintenanceMetrics.class);

  private static volatile MaintenanceMetrics instance;
  private static final Object lock = new Object();

  // ==========================================================================
  // Labels
  // ==========================================================================

  private static final String LABEL_STATUS = "status";
  private static final String LABEL_TABLE = "table";

  private static final String[] STATUS_LABELS = {LABEL_STATUS};

  // ==========================================================================
  // General Maintenance Metrics
  // ==========================================================================

  private static final String RUNS_TOTAL_NAME = "ice_maintenance_runs_total";
  private static final String RUNS_TOTAL_HELP = "Total number of maintenance runs";

  private static final String DURATION_SECONDS_NAME = "ice_maintenance_duration_seconds";
  private static final String DURATION_SECONDS_HELP = "Duration of maintenance run in seconds";

  private static final String IN_PROGRESS_NAME = "ice_maintenance_in_progress";
  private static final String IN_PROGRESS_HELP =
      "Whether maintenance is currently running (1 = running, 0 = idle)";

  private static final String LAST_RUN_TIMESTAMP_NAME = "ice_maintenance_last_run_timestamp";
  private static final String LAST_RUN_TIMESTAMP_HELP =
      "Unix timestamp of the last maintenance run";

  private static final String SKIPPED_TOTAL_NAME = "ice_maintenance_skipped_total";
  private static final String SKIPPED_TOTAL_HELP =
      "Times maintenance was skipped (already in maintenance mode)";

  private static final String ORPHAN_FILES_FOUND_TOTAL_NAME =
      "ice_maintenance_orphan_files_found_total";
  private static final String ORPHAN_FILES_FOUND_TOTAL_HELP = "Total orphaned files discovered";

  private static final String ORPHAN_FILES_DELETED_TOTAL_NAME =
      "ice_maintenance_orphan_files_deleted_total";
  private static final String ORPHAN_FILES_DELETED_TOTAL_HELP =
      "Total orphaned files successfully deleted";

  private static final String ORPHAN_FILES_EXCLUDED_TOTAL_NAME =
      "ice_maintenance_orphan_files_excluded_total";
  private static final String ORPHAN_FILES_EXCLUDED_TOTAL_HELP = "Files excluded by whitelist";

  private static final String ORPHAN_DELETE_FAILURES_TOTAL_NAME =
      "ice_maintenance_orphan_delete_failures_total";
  private static final String ORPHAN_DELETE_FAILURES_TOTAL_HELP = "Files that failed to delete";

  private static final String COMPACTION_FILES_MERGED_TOTAL_NAME =
      "ice_maintenance_compaction_files_merged_total";
  private static final String COMPACTION_FILES_MERGED_TOTAL_HELP = "Total input files merged";

  private static final String COMPACTION_FILES_CREATED_TOTAL_NAME =
      "ice_maintenance_compaction_files_created_total";
  private static final String COMPACTION_FILES_CREATED_TOTAL_HELP =
      "Total output files created after merge";

  private static final String COMPACTION_BYTES_READ_TOTAL_NAME =
      "ice_maintenance_compaction_bytes_read_total";
  private static final String COMPACTION_BYTES_READ_TOTAL_HELP =
      "Total bytes read during compaction";

  private static final String COMPACTION_BYTES_WRITTEN_TOTAL_NAME =
      "ice_maintenance_compaction_bytes_written_total";
  private static final String COMPACTION_BYTES_WRITTEN_TOTAL_HELP =
      "Total bytes written during compaction";

  private static final double[] DURATION_BUCKETS = {
    0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600
  };

  // General
  private final Counter runsTotal;
  private final Histogram durationSeconds;
  private final Gauge inProgress;
  private final Gauge lastRunTimestamp;
  private final Counter skippedTotal;

  // Orphan Cleanup
  private final Counter orphanFilesFoundTotal;
  private final Counter orphanFilesDeletedTotal;
  private final Counter orphanFilesExcludedTotal;
  private final Counter orphanDeleteFailuresTotal;

  // Data Compaction
  private final Counter compactionFilesMergedTotal;
  private final Counter compactionFilesCreatedTotal;
  private final Counter compactionBytesReadTotal;
  private final Counter compactionBytesWrittenTotal;

  /** Returns the singleton instance of the metrics. */
  public static MaintenanceMetrics getInstance() {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance = new MaintenanceMetrics();
        }
      }
    }
    return instance;
  }

  private MaintenanceMetrics() {
    // General maintenance metrics
    this.runsTotal =
        Counter.builder()
            .name(RUNS_TOTAL_NAME)
            .help(RUNS_TOTAL_HELP)
            .labelNames(STATUS_LABELS)
            .register();

    this.durationSeconds =
        Histogram.builder()
            .name(DURATION_SECONDS_NAME)
            .help(DURATION_SECONDS_HELP)
            .classicUpperBounds(DURATION_BUCKETS)
            .register();

    this.inProgress = Gauge.builder().name(IN_PROGRESS_NAME).help(IN_PROGRESS_HELP).register();

    this.lastRunTimestamp =
        Gauge.builder()
            .name(LAST_RUN_TIMESTAMP_NAME)
            .help(LAST_RUN_TIMESTAMP_HELP)
            .labelNames(STATUS_LABELS)
            .register();

    this.skippedTotal =
        Counter.builder().name(SKIPPED_TOTAL_NAME).help(SKIPPED_TOTAL_HELP).register();

    // Orphan cleanup metrics
    this.orphanFilesFoundTotal =
        Counter.builder()
            .name(ORPHAN_FILES_FOUND_TOTAL_NAME)
            .help(ORPHAN_FILES_FOUND_TOTAL_HELP)
            .labelNames(LABEL_TABLE)
            .register();

    this.orphanFilesDeletedTotal =
        Counter.builder()
            .name(ORPHAN_FILES_DELETED_TOTAL_NAME)
            .help(ORPHAN_FILES_DELETED_TOTAL_HELP)
            .labelNames(LABEL_TABLE)
            .register();

    this.orphanFilesExcludedTotal =
        Counter.builder()
            .name(ORPHAN_FILES_EXCLUDED_TOTAL_NAME)
            .help(ORPHAN_FILES_EXCLUDED_TOTAL_HELP)
            .labelNames(LABEL_TABLE)
            .register();

    this.orphanDeleteFailuresTotal =
        Counter.builder()
            .name(ORPHAN_DELETE_FAILURES_TOTAL_NAME)
            .help(ORPHAN_DELETE_FAILURES_TOTAL_HELP)
            .labelNames(LABEL_TABLE)
            .register();

    // Data compaction metrics
    this.compactionFilesMergedTotal =
        Counter.builder()
            .name(COMPACTION_FILES_MERGED_TOTAL_NAME)
            .help(COMPACTION_FILES_MERGED_TOTAL_HELP)
            .labelNames(LABEL_TABLE)
            .register();

    this.compactionFilesCreatedTotal =
        Counter.builder()
            .name(COMPACTION_FILES_CREATED_TOTAL_NAME)
            .help(COMPACTION_FILES_CREATED_TOTAL_HELP)
            .labelNames(LABEL_TABLE)
            .register();

    this.compactionBytesReadTotal =
        Counter.builder()
            .name(COMPACTION_BYTES_READ_TOTAL_NAME)
            .help(COMPACTION_BYTES_READ_TOTAL_HELP)
            .labelNames(LABEL_TABLE)
            .register();

    this.compactionBytesWrittenTotal =
        Counter.builder()
            .name(COMPACTION_BYTES_WRITTEN_TOTAL_NAME)
            .help(COMPACTION_BYTES_WRITTEN_TOTAL_HELP)
            .labelNames(LABEL_TABLE)
            .register();

    logger.info("Maintenance Prometheus metrics initialized");
  }

  public void recordMaintenanceStarted() {
    inProgress.set(1.0);
  }

  public void recordMaintenanceCompleted(boolean success, double durationSecs) {
    String status = success ? "success" : "failure";
    runsTotal.labelValues(status).inc();
    durationSeconds.observe(durationSecs);
    lastRunTimestamp.labelValues(status).set(System.currentTimeMillis() / 1000.0);
    inProgress.set(0.0);
  }

  public void recordMaintenanceSkipped() {
    skippedTotal.inc();
  }

  public void recordOrphanFilesFound(String table, int count) {
    orphanFilesFoundTotal.labelValues(table).inc(count);
  }

  public void recordOrphanFilesDeleted(String table, int count) {
    orphanFilesDeletedTotal.labelValues(table).inc(count);
  }

  public void recordOrphanFilesExcluded(String table, int count) {
    orphanFilesExcludedTotal.labelValues(table).inc(count);
  }

  public void recordOrphanDeleteFailure(String table) {
    orphanDeleteFailuresTotal.labelValues(table).inc();
  }

  public void recordCompactionFilesMerged(String table, int count) {
    compactionFilesMergedTotal.labelValues(table).inc(count);
  }

  public void recordCompactionFileCreated(String table) {
    compactionFilesCreatedTotal.labelValues(table).inc();
  }

  public void recordCompactionBytesRead(String table, long bytes) {
    compactionBytesReadTotal.labelValues(table).inc(bytes);
  }

  public void recordCompactionBytesWritten(String table, long bytes) {
    compactionBytesWrittenTotal.labelValues(table).inc(bytes);
  }
}
