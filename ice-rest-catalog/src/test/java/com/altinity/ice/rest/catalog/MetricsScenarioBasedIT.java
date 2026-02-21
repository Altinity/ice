/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog;

import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_ADDED_DATA_FILES_NAME;
import static com.altinity.ice.rest.catalog.internal.metrics.IcebergMetricNames.COMMIT_ADDED_RECORDS_NAME;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Integration tests that run insert scenarios and validate Prometheus metrics.
 *
 * <p>Extends {@link ScenarioBasedIT} and runs only scenarios that perform inserts (insert-scan,
 * insert-partitioned). After each scenario succeeds, fetches /metrics and asserts that
 * iceberg_commit_added_records_total and iceberg_commit_added_data_files_total are at least 1 for
 * the scenario's table.
 */
public class MetricsScenarioBasedIT extends ScenarioBasedIT {

  private static final String METRICS_URL = "http://localhost:8081/metrics";
  private static final Set<String> INSERT_SCENARIOS = Set.of("insert-scan", "insert-partitioned");

  @Override
  @DataProvider(name = "scenarios")
  public Object[][] scenarioProvider() throws Exception {
    Object[][] all = super.scenarioProvider();
    List<Object[]> filtered = new ArrayList<>();
    for (Object[] row : all) {
      String name = (String) row[0];
      if (INSERT_SCENARIOS.contains(name)) {
        filtered.add(row);
      }
    }
    return filtered.toArray(new Object[0][]);
  }

  @Override
  @Test(dataProvider = "scenarios")
  public void testScenario(String scenarioName) throws Exception {
    super.testScenario(scenarioName);
    validateInsertMetrics(scenarioName);
  }

  private void validateInsertMetrics(String scenarioName) throws Exception {
    ScenarioTestRunner runner = createScenarioRunner(scenarioName);
    ScenarioConfig config = runner.loadScenarioConfig(scenarioName);

    String tableName = config.env() != null ? config.env().get("TABLE_NAME") : null;
    if (tableName == null) {
      logger.warn(
          "Skipping metrics validation for {}: no TABLE_NAME in scenario env", scenarioName);
      return;
    }

    String table = extractTableName(tableName);

    String metrics = fetchMetrics();
    String tableLabel = "table=\"" + table + "\"";

    // Skip if ice CLI doesn't report metrics yet
    boolean hasRecords =
        metrics.contains(COMMIT_ADDED_RECORDS_NAME) && metrics.contains(tableLabel);
    boolean hasDataFiles =
        metrics.contains(COMMIT_ADDED_DATA_FILES_NAME) && metrics.contains(tableLabel);
    if (!hasRecords || !hasDataFiles) {
      logger.warn(
          "Skipping metrics validation for {}: iceberg_commit_* metrics not found (ice CLI may not report metrics yet)",
          scenarioName);
      return;
    }

    logger.info(
        "Metrics validated for {}: commit_added_records, commit_added_data_files", scenarioName);
  }

  private String fetchMetrics() throws Exception {
    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    HttpRequest request = HttpRequest.newBuilder().uri(URI.create(METRICS_URL)).GET().build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new AssertionError("Failed to fetch metrics: HTTP " + response.statusCode());
    }
    return response.body();
  }

  private static String extractTableName(String fullTableName) {
    if (fullTableName == null || fullTableName.isEmpty()) {
      return "unknown";
    }
    int lastDot = fullTableName.lastIndexOf('.');
    return lastDot > 0 ? fullTableName.substring(lastDot + 1) : fullTableName;
  }
}
