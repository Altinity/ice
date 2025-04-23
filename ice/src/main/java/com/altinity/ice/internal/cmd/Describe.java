package com.altinity.ice.internal.cmd;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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

  // TODO: refactor: the use of StringBuilder below is absolutely criminal
  public static void run(RESTCatalog catalog, String target, boolean json, boolean includeMetrics)
      throws IOException {
    String targetNamespace = null;
    String targetTable = null;
    if (target != null && !target.isEmpty()) {
      // TODO: support catalog.ns.table
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
    // FIXME: there is no need to list nss/tables when target is given
    var sb = new StringBuilder();
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
        sb.append("---\n");
        sb.append("kind: Table\n");
        sb.append("metadata:\n");
        sb.append("\tid: " + tableId + "\n");
        Table table = catalog.loadTable(tableId);
        sb.append("data:\n");
        sb.append("\tschema_raw: |-\n" + prefixEachLine(table.schema().toString(), "\t\t") + "\n");
        sb.append(
            "\tpartition_spec_raw: |-\n" + prefixEachLine(table.spec().toString(), "\t\t") + "\n");
        sb.append(
            "\tsort_order_raw: |-\n" + prefixEachLine(table.sortOrder().toString(), "\t\t") + "\n");
        sb.append("\tproperties: \n");
        for (var property : table.properties().entrySet()) {
          var v = property.getValue();
          if (v.contains("\n")) {
            sb.append("\t\t" + property.getKey() + ": |-\n" + prefixEachLine(v, "\t\t\t") + "\n");
          } else {
            sb.append("\t\t" + property.getKey() + ": \"" + v + "\"\n");
          }
        }
        sb.append("\tlocation: " + table.location() + "\n");
        sb.append("\tcurrent_snapshot: \n");
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot != null) {
          sb.append("\t\tsequence_number: " + snapshot.sequenceNumber() + "\n");
          sb.append("\t\tid: " + snapshot.snapshotId() + "\n");
          sb.append("\t\tparent_id: " + snapshot.parentId() + "\n");
          sb.append("\t\ttimestamp: " + snapshot.timestampMillis() + "\n");
          sb.append(
              "\t\ttimestamp_iso: \""
                  + Instant.ofEpochMilli(snapshot.timestampMillis()).toString()
                  + "\"\n");
          sb.append(
              "\t\ttimestamp_iso_local: \""
                  + Instant.ofEpochMilli(snapshot.timestampMillis())
                      .atZone(ZoneId.systemDefault())
                      .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                  + "\"\n");
          sb.append("\t\toperation: " + snapshot.operation() + "\n");
          sb.append("\t\tsummary:\n");
          for (var property : snapshot.summary().entrySet()) {
            sb.append("\t\t\t" + property.getKey() + ": \"" + property.getValue() + "\"\n");
          }
          sb.append("\t\tlocation: " + snapshot.manifestListLocation() + "\n");
        }

        if (includeMetrics) {
          printTableMetrics(table, sb);
        }
      }
    }
    String r = sb.toString().replace("\t", "  ");
    if (json) {
      r = convertYamlToJson(r);
    }
    System.out.println(r);
  }

  private static void printTableMetrics(Table table, StringBuilder buffer) throws IOException {
    TableScan scan = table.newScan().includeColumnStats();
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    for (FileScanTask task : tasks) {
      DataFile dataFile = task.file();
      buffer.append("\tmetrics:\n");
      buffer.append("\t\tfile: " + dataFile.path() + "\n");
      buffer.append("\t\trecord_count: " + dataFile.recordCount() + "\n");

      Map<Integer, Long> valueCounts = dataFile.valueCounts();
      Map<Integer, Long> nullCounts = dataFile.nullValueCounts();
      Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds();
      Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();

      if (valueCounts == null && nullCounts == null && lowerBounds == null && upperBounds == null) {
        continue;
      }

      buffer.append("\t\tcolumns:\n");
      for (Types.NestedField field : table.schema().columns()) {
        int id = field.fieldId();
        buffer.append("\t\t\t" + field.name() + ":\n");
        if (valueCounts != null) {
          buffer.append("\t\t\t\tvalue_count: " + valueCounts.get(id) + "\n");
        }
        if (nullCounts != null) {
          buffer.append("\t\t\t\tnull_count: " + nullCounts.get(id) + "\n");
        }
        if (lowerBounds != null) {
          ByteBuffer lower = lowerBounds.get(id);
          String lowerStr =
              lower != null ? Conversions.fromByteBuffer(field.type(), lower).toString() : "null";
          buffer.append("\t\t\t\tlower_bound: " + lowerStr + "\n");
        }
        if (upperBounds != null) {
          ByteBuffer upper = upperBounds.get(id);
          String upperStr =
              upper != null ? Conversions.fromByteBuffer(field.type(), upper).toString() : "null";
          buffer.append("\t\t\t\tupper_bound: " + upperStr + "\n");
        }
      }
    }

    tasks.close();
  }

  private static String convertYamlToJson(String yaml) throws IOException {
    YAMLFactory yamlFactory = new YAMLFactory();
    ObjectMapper yamlReader = new ObjectMapper(yamlFactory);
    ObjectMapper jsonWriter = new ObjectMapper();
    StringBuilder result = new StringBuilder();
    try (JsonParser parser = yamlFactory.createParser(yaml)) {
      while (!parser.isClosed()) {
        JsonNode node = yamlReader.readTree(parser);
        if (node != null) {
          String json = jsonWriter.writeValueAsString(node);
          result.append(json).append("\n");
        }
      }
    }
    return result.toString().trim();
  }

  private static String prefixEachLine(String v, String prefix) {
    return v.lines().map(line -> prefix + line).collect(Collectors.joining("\n"));
  }
}
