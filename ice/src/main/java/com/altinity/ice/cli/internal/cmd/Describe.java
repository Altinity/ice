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
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ServiceFailureException;
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

        Describe.Table.Data tableData = null;
        Table.Metadata tableMetadata;
        try {
          tableData = gatherTableData(catalog, tableId, optionsSet);
          tableMetadata = new Table.Metadata(tableId.toString());
        } catch (ServiceFailureException e) {
          tableMetadata = new Table.Metadata(tableId.toString(), e.getMessage());
        }

        tablesMetadata.add(new Table("Table", tableMetadata, tableData));
      }
    }

    if (!tablesMetadata.isEmpty()) {
      ObjectMapper mapper =
          json
              ? new ObjectMapper()
              : new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
      mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      String output = mapper.writeValueAsString(tablesMetadata);
      System.out.println(output);
    }
  }

  private static Table.Data gatherTableData(
      RESTCatalog catalog, TableIdentifier tableId, Set<Describe.Option> optionsSet)
      throws IOException {

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
    return new Table.Data(
        includeSchema ? table.schema().toString() : null,
        includeSchema ? table.spec().toString() : null,
        includeSchema ? table.sortOrder().toString() : null,
        optionsSet.contains(Option.INCLUDE_PROPERTIES) ? table.properties() : null,
        table.location(),
        snapshotInfo,
        metrics);
  }

  private static List<Table.Metrics> gatherTableMetrics(org.apache.iceberg.Table table)
      throws IOException {
    List<Table.Metrics> metricsList = new ArrayList<>();
    TableScan scan = table.newScan().includeColumnStats();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile dataFile = task.file();
        List<Table.ColumnMetrics> columnMetrics = new ArrayList<>();

        Map<Integer, Long> valueCounts = dataFile.valueCounts();
        Map<Integer, Long> nullCounts = dataFile.nullValueCounts();
        Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds();
        Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();

        if (valueCounts == null
            && nullCounts == null
            && lowerBounds == null
            && upperBounds == null) {
          continue;
        }

        gatherNestedTableMetrics(
            table.schema().columns(),
            valueCounts,
            nullCounts,
            lowerBounds,
            upperBounds,
            columnMetrics);

        metricsList.add(
            new Table.Metrics(dataFile.location(), dataFile.recordCount(), columnMetrics));
      }
    }
    return metricsList;
  }

  private static void gatherNestedTableMetrics(
      List<Types.NestedField> fields,
      @Nullable Map<Integer, Long> valueCounts,
      @Nullable Map<Integer, Long> nullCounts,
      @Nullable Map<Integer, ByteBuffer> lowerBounds,
      @Nullable Map<Integer, ByteBuffer> upperBounds,
      List<Table.ColumnMetrics> result) {
    for (Types.NestedField field : fields) {
      int id = field.fieldId();

      String lowerBound = null;
      if (lowerBounds != null) {
        ByteBuffer lower = lowerBounds.get(id);
        if (lower != null) {
          lowerBound = Conversions.fromByteBuffer(field.type(), lower).toString();
        }
      }

      String upperBound = null;
      if (upperBounds != null) {
        ByteBuffer upper = upperBounds.get(id);
        if (upper != null) {
          upperBound = Conversions.fromByteBuffer(field.type(), upper).toString();
        }
      }

      List<Table.ColumnMetrics> nested = new ArrayList<>();

      if (field.type().isStructType()) {
        gatherNestedTableMetrics(
            field.type().asStructType().fields(),
            valueCounts,
            nullCounts,
            lowerBounds,
            upperBounds,
            nested);
      }

      result.add(
          new Table.ColumnMetrics(
              field.name(),
              valueCounts != null ? valueCounts.get(id) : null,
              nullCounts != null ? nullCounts.get(id) : null,
              lowerBound,
              upperBound,
              nested));
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  record Table(String kind, Table.Metadata metadata, Table.Data data) {
    public Table {
      if (kind == null) {
        kind = "Table";
      }
    }

    record Metadata(String id, String status) {
      static final String STATUS_OK = "OK";

      Metadata(String id) {
        this(id, STATUS_OK);
      }
    }

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
        String name,
        Long valueCount,
        Long nullCount,
        String lowerBound,
        String upperBound,
        List<ColumnMetrics> nested) {}
  }
}
