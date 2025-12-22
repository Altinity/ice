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

/** Constants for Iceberg Prometheus metric names, help strings, and labels. */
public final class IcebergMetricNames {

  private IcebergMetricNames() {}

  // ==========================================================================
  // Labels
  // ==========================================================================

  public static final String LABEL_CATALOG = "catalog";
  public static final String LABEL_NAMESPACE = "namespace";
  public static final String LABEL_TABLE = "table";
  public static final String LABEL_OPERATION = "operation";
  public static final String LABEL_TYPE = "type";

  public static final String[] SCAN_LABELS = {LABEL_CATALOG, LABEL_NAMESPACE, LABEL_TABLE};
  public static final String[] COMMIT_LABELS = {
    LABEL_CATALOG, LABEL_NAMESPACE, LABEL_TABLE, LABEL_OPERATION
  };

  // ==========================================================================
  // Reporter Info Metrics
  // ==========================================================================

  public static final String REPORTER_ACTIVE_NAME = "iceberg_metrics_reporter_active";
  public static final String REPORTER_ACTIVE_HELP =
      "Iceberg metrics reporter status (value 1 means reporter is active)";

  public static final String REPORT_ERRORS_NAME = "iceberg_metrics_report_errors_total";
  public static final String REPORT_ERRORS_HELP =
      "Total number of errors while processing metrics reports";

  // ==========================================================================
  // Scan Metrics
  // ==========================================================================

  public static final String SCANS_TOTAL_NAME = "iceberg_scans_total";
  public static final String SCANS_TOTAL_HELP = "Total number of Iceberg table scans";

  public static final String SCAN_RESULT_DATA_FILES_NAME = "iceberg_scan_result_data_files_total";
  public static final String SCAN_RESULT_DATA_FILES_HELP =
      "Total number of data files in scan results";

  public static final String SCAN_RESULT_DELETE_FILES_NAME =
      "iceberg_scan_result_delete_files_total";
  public static final String SCAN_RESULT_DELETE_FILES_HELP =
      "Total number of delete files in scan results";

  public static final String SCAN_INDEXED_DELETE_FILES_NAME =
      "iceberg_scan_indexed_delete_files_total";
  public static final String SCAN_INDEXED_DELETE_FILES_HELP =
      "Total number of indexed delete files in scan results";

  public static final String SCAN_POSITIONAL_DELETE_FILES_NAME =
      "iceberg_scan_positional_delete_files_total";
  public static final String SCAN_POSITIONAL_DELETE_FILES_HELP =
      "Total number of positional delete files in scan results";

  public static final String SCAN_EQUALITY_DELETE_FILES_NAME =
      "iceberg_scan_equality_delete_files_total";
  public static final String SCAN_EQUALITY_DELETE_FILES_HELP =
      "Total number of equality delete files in scan results";

  public static final String SCAN_TOTAL_DATA_MANIFESTS_NAME = "iceberg_scan_total_data_manifests";
  public static final String SCAN_TOTAL_DATA_MANIFESTS_HELP =
      "Total number of data manifests considered during scans";

  public static final String SCAN_TOTAL_DELETE_MANIFESTS_NAME =
      "iceberg_scan_total_delete_manifests";
  public static final String SCAN_TOTAL_DELETE_MANIFESTS_HELP =
      "Total number of delete manifests considered during scans";

  public static final String SCAN_SCANNED_DATA_MANIFESTS_NAME =
      "iceberg_scan_scanned_data_manifests";
  public static final String SCAN_SCANNED_DATA_MANIFESTS_HELP =
      "Total number of data manifests actually scanned";

  public static final String SCAN_SKIPPED_DATA_MANIFESTS_NAME =
      "iceberg_scan_skipped_data_manifests";
  public static final String SCAN_SKIPPED_DATA_MANIFESTS_HELP =
      "Total number of data manifests skipped during scans";

  public static final String SCAN_TOTAL_FILE_SIZE_BYTES_NAME = "iceberg_scan_total_file_size_bytes";
  public static final String SCAN_TOTAL_FILE_SIZE_BYTES_HELP =
      "Total file size in bytes for scanned data files";

  public static final String SCAN_TOTAL_DELETE_FILE_SIZE_BYTES_NAME =
      "iceberg_scan_total_delete_file_size_bytes";
  public static final String SCAN_TOTAL_DELETE_FILE_SIZE_BYTES_HELP =
      "Total file size in bytes for scanned delete files";

  public static final String SCAN_PLANNING_DURATION_NAME = "iceberg_scan_planning_duration_seconds";
  public static final String SCAN_PLANNING_DURATION_HELP = "Duration of scan planning in seconds";

  public static final String SCAN_DATA_FILES_PER_SCAN_NAME = "iceberg_scan_data_files_per_scan";
  public static final String SCAN_DATA_FILES_PER_SCAN_HELP = "Distribution of data files per scan";

  // ==========================================================================
  // Commit Metrics
  // ==========================================================================

  public static final String COMMITS_TOTAL_NAME = "iceberg_commits_total";
  public static final String COMMITS_TOTAL_HELP = "Total number of Iceberg table commits";

  public static final String COMMIT_ADDED_DATA_FILES_NAME = "iceberg_commit_added_data_files_total";
  public static final String COMMIT_ADDED_DATA_FILES_HELP =
      "Total number of data files added in commits";

  public static final String COMMIT_REMOVED_DATA_FILES_NAME =
      "iceberg_commit_removed_data_files_total";
  public static final String COMMIT_REMOVED_DATA_FILES_HELP =
      "Total number of data files removed in commits";

  public static final String COMMIT_ADDED_DELETE_FILES_NAME =
      "iceberg_commit_added_delete_files_total";
  public static final String COMMIT_ADDED_DELETE_FILES_HELP =
      "Total number of delete files added in commits";

  public static final String COMMIT_REMOVED_DELETE_FILES_NAME =
      "iceberg_commit_removed_delete_files_total";
  public static final String COMMIT_REMOVED_DELETE_FILES_HELP =
      "Total number of delete files removed in commits";

  public static final String COMMIT_ADDED_RECORDS_NAME = "iceberg_commit_added_records_total";
  public static final String COMMIT_ADDED_RECORDS_HELP = "Total number of records added in commits";

  public static final String COMMIT_REMOVED_RECORDS_NAME = "iceberg_commit_removed_records_total";
  public static final String COMMIT_REMOVED_RECORDS_HELP =
      "Total number of records removed in commits";

  public static final String COMMIT_ADDED_EQUALITY_DELETES_NAME =
      "iceberg_commit_added_equality_deletes_total";
  public static final String COMMIT_ADDED_EQUALITY_DELETES_HELP =
      "Total number of equality deletes added in commits";

  public static final String COMMIT_TOTAL_FILES_SIZE_BYTES_NAME =
      "iceberg_commit_total_files_size_bytes";
  public static final String COMMIT_TOTAL_FILES_SIZE_BYTES_HELP =
      "Total size in bytes of files involved in commits";

  public static final String COMMIT_DURATION_NAME = "iceberg_commit_duration_seconds";
  public static final String COMMIT_DURATION_HELP = "Duration of commit operations in seconds";

  // ==========================================================================
  // Histogram Buckets
  // ==========================================================================

  /** Duration histogram buckets (in seconds) - suitable for typical Iceberg operations. */
  public static final double[] DURATION_BUCKETS = {
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60
  };

  // ==========================================================================
  // HTTP/REST Endpoint Metrics
  // ==========================================================================

  public static final String LABEL_METHOD = "method";
  public static final String LABEL_ROUTE = "route";
  public static final String LABEL_STATUS_CLASS = "status_class";

  public static final String[] HTTP_REQUEST_LABELS = {LABEL_METHOD, LABEL_ROUTE};
  public static final String[] HTTP_RESPONSE_LABELS = {
    LABEL_METHOD, LABEL_ROUTE, LABEL_STATUS_CLASS
  };

  public static final String HTTP_REQUESTS_TOTAL_NAME = "iceberg_http_requests_total";
  public static final String HTTP_REQUESTS_TOTAL_HELP = "Total number of HTTP requests";

  public static final String HTTP_REQUEST_DURATION_NAME = "iceberg_http_request_duration_seconds";
  public static final String HTTP_REQUEST_DURATION_HELP = "HTTP request duration in seconds";

  public static final String HTTP_RESPONSES_TOTAL_NAME = "iceberg_http_responses_total";
  public static final String HTTP_RESPONSES_TOTAL_HELP =
      "Total number of HTTP responses by status class";

  public static final String HTTP_REQUESTS_IN_FLIGHT_NAME = "iceberg_http_requests_in_flight";
  public static final String HTTP_REQUESTS_IN_FLIGHT_HELP =
      "Number of HTTP requests currently being processed";

  public static final String HTTP_RESPONSE_SIZE_BYTES_NAME = "iceberg_http_response_size_bytes";
  public static final String HTTP_RESPONSE_SIZE_BYTES_HELP = "HTTP response size in bytes";

  /** HTTP request duration buckets (in seconds) - suitable for REST API calls. */
  public static final double[] HTTP_DURATION_BUCKETS = {
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
  };

  // ==========================================================================
  // Catalog-Level Metrics
  // ==========================================================================

  public static final String CATALOG_TABLES_NAME = "iceberg_catalog_tables";
  public static final String CATALOG_TABLES_HELP = "Current number of tables in the catalog";

  public static final String CATALOG_NAMESPACES_NAME = "iceberg_catalog_namespaces";
  public static final String CATALOG_NAMESPACES_HELP =
      "Current number of namespaces in the catalog";

  public static final String CATALOG_OPERATIONS_TOTAL_NAME = "iceberg_catalog_operations_total";
  public static final String CATALOG_OPERATIONS_TOTAL_HELP =
      "Total number of catalog operations (create/drop table/namespace)";

  public static final String[] CATALOG_OPERATION_LABELS = {LABEL_CATALOG, LABEL_OPERATION};

  // ==========================================================================
  // Table-Level Metrics (from CommitReport)
  // ==========================================================================

  public static final String TABLE_SNAPSHOTS_TOTAL_NAME = "iceberg_table_snapshots_total";
  public static final String TABLE_SNAPSHOTS_TOTAL_HELP = "Total number of snapshots per table";

  public static final String TABLE_SCHEMA_UPDATES_TOTAL_NAME = "iceberg_table_schema_updates_total";
  public static final String TABLE_SCHEMA_UPDATES_TOTAL_HELP =
      "Total number of schema evolutions per table";
}
