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
        TableMetadata tableMetadata = new TableMetadata();

        // Set metadata
        TableMetadata.Metadata metadata = new TableMetadata.Metadata();
        metadata.setId(tableId.toString());
        tableMetadata.setMetadata(metadata);

        // Set data
        TableMetadata.TableData data = new TableMetadata.TableData();
        data.setSchema_raw(table.schema().toString());
        data.setPartition_spec_raw(table.spec().toString());
        data.setSort_order_raw(table.sortOrder().toString());
        data.setProperties(table.properties());
        data.setLocation(table.location());

        // Set current snapshot
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot != null) {
          TableMetadata.SnapshotInfo snapshotInfo = new TableMetadata.SnapshotInfo();
          snapshotInfo.setSequence_number(snapshot.sequenceNumber());
          snapshotInfo.setId(snapshot.snapshotId());
          snapshotInfo.setParent_id(snapshot.parentId());
          snapshotInfo.setTimestamp(snapshot.timestampMillis());
          snapshotInfo.setTimestamp_iso(
              Instant.ofEpochMilli(snapshot.timestampMillis()).toString());
          snapshotInfo.setTimestamp_iso_local(
              Instant.ofEpochMilli(snapshot.timestampMillis())
                  .atZone(ZoneId.systemDefault())
                  .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
          snapshotInfo.setOperation(snapshot.operation());
          snapshotInfo.setSummary(snapshot.summary());
          snapshotInfo.setLocation(snapshot.manifestListLocation());
          data.setCurrent_snapshot(snapshotInfo);
        }

        if (includeMetrics) {
          data.setMetrics(getTableMetrics(table));
        }

        tableMetadata.setData(data);
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
        TableMetadata.MetricsInfo metricsInfo = new TableMetadata.MetricsInfo();
        metricsInfo.setFile(dataFile.path().toString());
        metricsInfo.setRecord_count(dataFile.recordCount());
        metricsInfo.setColumns(new ArrayList<>());

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
          TableMetadata.ColumnMetrics columnMetrics = new TableMetadata.ColumnMetrics();
          columnMetrics.setName(field.name());

          if (valueCounts != null) {
            columnMetrics.setValue_count(valueCounts.get(id));
          }
          if (nullCounts != null) {
            columnMetrics.setNull_count(nullCounts.get(id));
          }
          if (lowerBounds != null) {
            ByteBuffer lower = lowerBounds.get(id);
            columnMetrics.setLower_bound(
                lower != null ? Conversions.fromByteBuffer(field.type(), lower).toString() : null);
          }
          if (upperBounds != null) {
            ByteBuffer upper = upperBounds.get(id);
            columnMetrics.setUpper_bound(
                upper != null ? Conversions.fromByteBuffer(field.type(), upper).toString() : null);
          }

          metricsInfo.getColumns().add(columnMetrics);
        }

        metrics.add(metricsInfo);
      }
    }

    return metrics;
  }

  private static String prefixEachLine(String v, String prefix) {
    return v.lines().map(line -> prefix + line).collect(Collectors.joining("\n"));
  }
}
