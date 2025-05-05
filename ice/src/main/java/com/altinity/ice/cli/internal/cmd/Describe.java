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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.*;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

public final class Describe {

  private Describe() {}

  public enum Option {
    INCLUDE_SCHEMA,
    INCLUDE_PROPERTIES,
    INCLUDE_METRICS,
  }

  public static void run(RESTCatalog catalog, String target, boolean json, Option... options)
      throws IOException {
    var optionsSet = Set.of(options);

    String targetNamespace = null;
    String targetTable = null;
    if (target != null && !target.isEmpty()) {
      var s = target.split("[.]", 2);
      switch (s.length) {
        case 2:
          targetNamespace = s[0];
          targetTable = s[1];
          break;
        case 1:
          targetNamespace = s[0];
          break;
      }
    }

    List<Table> tablesMetadata = new ArrayList<>();
    List<Namespace> namespaces = catalog.listNamespaces();
    for (Namespace namespace : namespaces) {
      if (targetNamespace != null && !targetNamespace.equals(namespace.toString())) {
        continue;
      }
      List<TableIdentifier> tables = catalog.listTables(namespace);
      for (TableIdentifier tableId : tables) {
        if (targetTable != null && !targetTable.equals(tableId.name())) {
          continue;
        }
        org.apache.iceberg.Table table = catalog.loadTable(tableId);
        Snapshot snapshot = table.currentSnapshot();
        Table.Snapshot snapshotInfo = null;
        if (snapshot != null) {
          snapshotInfo =
              new Table.Snapshot(
                  snapshot.sequenceNumber(),
                  snapshot.snapshotId(),
                  snapshot.parentId(),
                  snapshot.timestampMillis(),
                  Instant.ofEpochMilli(snapshot.timestampMillis()).toString(),
                  Instant.ofEpochMilli(snapshot.timestampMillis())
                      .atZone(ZoneId.systemDefault())
                      .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                  snapshot.operation(),
                  snapshot.summary(),
                  snapshot.manifestListLocation());
        }

        List<Table.Metrics> metrics = null;
        if (optionsSet.contains(Option.INCLUDE_METRICS)) {
          metrics = gatherTableMetrics(table);
        }

        boolean includeSchema = optionsSet.contains(Option.INCLUDE_SCHEMA);
        Table.Data tableData =
            new Table.Data(
                includeSchema ? table.schema().toString() : null,
                includeSchema ? table.spec().toString() : null,
                includeSchema ? table.sortOrder().toString() : null,
                optionsSet.contains(Option.INCLUDE_PROPERTIES) ? table.properties() : null,
                table.location(),
                snapshotInfo,
                metrics);

        tablesMetadata.add(new Table("Table", new Table.Metadata(tableId.toString()), tableData));
      }
    }

    if (!tablesMetadata.isEmpty()) {
      ObjectMapper mapper = json ? new ObjectMapper() : new ObjectMapper(new YAMLFactory());
      mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      String output = mapper.writeValueAsString(tablesMetadata);
      System.out.println(output);
    }
  }

  private static List<Table.Metrics> gatherTableMetrics(org.apache.iceberg.Table table)
      throws IOException {
    List<Table.Metrics> metricsList = new ArrayList<>();
    TableScan scan = table.newScan().includeColumnStats();
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    for (FileScanTask task : tasks) {
      DataFile dataFile = task.file();
      List<Table.ColumnMetrics> columnMetrics = new ArrayList<>();

      Map<Integer, Long> valueCounts = dataFile.valueCounts();
      Map<Integer, Long> nullCounts = dataFile.nullValueCounts();
      Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds();
      Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();

      if (valueCounts == null && nullCounts == null && lowerBounds == null && upperBounds == null) {
        continue;
      }

      for (Types.NestedField field : table.schema().columns()) {
        int id = field.fieldId();
        String lowerBound = null;
        String upperBound = null;

        if (lowerBounds != null) {
          ByteBuffer lower = lowerBounds.get(id);
          lowerBound =
              lower != null ? Conversions.fromByteBuffer(field.type(), lower).toString() : null;
        }
        if (upperBounds != null) {
          ByteBuffer upper = upperBounds.get(id);
          upperBound =
              upper != null ? Conversions.fromByteBuffer(field.type(), upper).toString() : null;
        }

        columnMetrics.add(
            new Table.ColumnMetrics(
                field.name(),
                valueCounts != null ? valueCounts.get(id) : null,
                nullCounts != null ? nullCounts.get(id) : null,
                lowerBound,
                upperBound));
      }

      metricsList.add(
          new Table.Metrics(dataFile.location(), dataFile.recordCount(), columnMetrics));
    }

    tasks.close();
    return metricsList;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  record Table(String kind, Table.Metadata metadata, Table.Data data) {
    public Table {
      if (kind == null) {
        kind = "Table";
      }
    }

    record Metadata(String id) {}

    record Data(
        String schemaRaw,
        String partitionSpecRaw,
        String sortOrderRaw,
        Map<String, String> properties,
        String location,
        Table.Snapshot currentSnapshot,
        List<Table.Metrics> metrics) {}

    record Snapshot(
        long sequenceNumber,
        long id,
        Long parentID,
        long timestamp,
        String timestampISO,
        String timestampISOLocal,
        String operation,
        Map<String, String> summary,
        String location) {}

    record Metrics(String file, long recordCount, List<Table.ColumnMetrics> columns) {}

    record ColumnMetrics(
        String name, Long valueCount, Long nullCount, String lowerBound, String upperBound) {}
  }
}
