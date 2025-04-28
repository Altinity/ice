package com.altinity.ice.internal.cmd;

import com.altinity.ice.internal.model.TableMetadata;
import com.altinity.ice.internal.model.TableMetadata.*;
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
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

public final class Describe {

  private Describe() {}

  public static void run(RESTCatalog catalog, String target, boolean json) throws IOException {
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

    List<TableMetadata> tablesMetadata = new ArrayList<>();
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
        Table table = catalog.loadTable(tableId);
        Snapshot snapshot = table.currentSnapshot();

        SnapshotInfo snapshotInfo = null;
        if (snapshot != null) {
          snapshotInfo =
              new SnapshotInfo(
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

        List<MetricsInfo> metrics = getTableMetrics(table);

        TableData tableData =
            new TableData(
                table.schema().toString(),
                table.spec().toString(),
                table.sortOrder().toString(),
                table.properties(),
                table.location(),
                snapshotInfo,
                metrics);

        tablesMetadata.add(new TableMetadata("Table", new Metadata(tableId.toString()), tableData));
      }
    }

    ObjectMapper mapper = json ? new ObjectMapper() : new ObjectMapper(new YAMLFactory());
    String output = mapper.writeValueAsString(tablesMetadata);
    System.out.println(output);
  }

  private static List<MetricsInfo> getTableMetrics(Table table) throws IOException {
    List<MetricsInfo> metricsList = new ArrayList<>();
    TableScan scan = table.newScan().includeColumnStats();
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    for (FileScanTask task : tasks) {
      DataFile dataFile = task.file();
      List<ColumnMetrics> columnMetrics = new ArrayList<>();

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
            new ColumnMetrics(
                field.name(),
                valueCounts != null ? valueCounts.get(id) : null,
                nullCounts != null ? nullCounts.get(id) : null,
                lowerBound,
                upperBound));
      }

      metricsList.add(
          new MetricsInfo(dataFile.path().toString(), dataFile.recordCount(), columnMetrics));
    }

    tasks.close();
    return metricsList;
  }
}
