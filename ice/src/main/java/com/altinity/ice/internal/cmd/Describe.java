package com.altinity.ice.internal.cmd;

import com.altinity.ice.internal.model.TableMetadata;
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
import java.util.stream.Collectors;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

public final class Describe {
  private Describe() {}

  public static void run(RESTCatalog catalog, String target, boolean json, boolean includeMetrics)
      throws IOException {
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

    List<TableMetadata> tables = new ArrayList<>();
    List<Namespace> namespaces = catalog.listNamespaces();

    for (Namespace namespace : namespaces) {
      if (targetNamespace != null && !targetNamespace.equals(namespace.toString())) {
        continue;
      }

      List<TableIdentifier> tableIds = catalog.listTables(namespace);
      for (TableIdentifier tableId : tableIds) {
        if (targetTable != null && !targetTable.equals(tableId.name())) {
          continue;
        }

        Table table = catalog.loadTable(tableId);
        TableMetadata tableMetadata =
            new TableMetadata("Table", new TableMetadata.Metadata(tableId.toString()), null);

        // Set data
        TableMetadata.TableData data =
            new TableMetadata.TableData(
                table.schema().toString(),
                table.spec().toString(),
                table.sortOrder().toString(),
                table.properties(),
                table.location(),
                null,
                null);

        // Set current snapshot
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot != null) {
          TableMetadata.SnapshotInfo snapshotInfo =
              new TableMetadata.SnapshotInfo(
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
          data =
              new TableMetadata.TableData(
                  data.schema_raw(),
                  data.partition_spec_raw(),
                  data.sort_order_raw(),
                  data.properties(),
                  data.location(),
                  snapshotInfo,
                  data.metrics());
        }

        if (includeMetrics) {
          List<TableMetadata.MetricsInfo> metrics = getTableMetrics(table);
          data =
              new TableMetadata.TableData(
                  data.schema_raw(),
                  data.partition_spec_raw(),
                  data.sort_order_raw(),
                  data.properties(),
                  data.location(),
                  data.current_snapshot(),
                  metrics);
        }

        tableMetadata = new TableMetadata(tableMetadata.kind(), tableMetadata.metadata(), data);
        tables.add(tableMetadata);
      }
    }

    ObjectMapper mapper;
    if (json) {
      mapper = new ObjectMapper();
    } else {
      YAMLFactory yamlFactory =
          new YAMLFactory()
              .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
              .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
              .enable(YAMLGenerator.Feature.INDENT_ARRAYS);
      mapper = new ObjectMapper(yamlFactory);
    }

    StringBuilder output = new StringBuilder();
    for (TableMetadata table : tables) {
      if (!json) {
        output.append("---\n");
      }
      output.append(mapper.writeValueAsString(table));
      if (!json) {
        output.append("\n");
      }
    }
    System.out.print(output.toString());
  }

  private static List<TableMetadata.MetricsInfo> getTableMetrics(Table table) throws IOException {
    List<TableMetadata.MetricsInfo> metrics = new ArrayList<>();
    TableScan scan = table.newScan().includeColumnStats();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile dataFile = task.file();
        List<TableMetadata.ColumnMetrics> columns = new ArrayList<>();

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

        for (Types.NestedField field : table.schema().columns()) {
          int id = field.fieldId();
          String name = field.name();
          Long valueCount = valueCounts != null ? valueCounts.get(id) : null;
          Long nullCount = nullCounts != null ? nullCounts.get(id) : null;
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

          columns.add(
              new TableMetadata.ColumnMetrics(name, valueCount, nullCount, lowerBound, upperBound));
        }

        metrics.add(
            new TableMetadata.MetricsInfo(
                dataFile.path().toString(), dataFile.recordCount(), columns));
      }
    }

    return metrics;
  }

  private static String prefixEachLine(String v, String prefix) {
    return v.lines().map(line -> prefix + line).collect(Collectors.joining("\n"));
  }
}
