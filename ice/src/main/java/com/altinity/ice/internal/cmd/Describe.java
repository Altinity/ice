package com.altinity.ice.internal.cmd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public final class Describe {

  private Describe() {}

  // TODO: refactor: the use of StringBuilder below is absolutely criminal
  public static void run(RESTCatalog catalog, String target, boolean json) throws IOException {
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
    sb.append("default:");
    boolean nsMatched = false;
    for (Namespace namespace : namespaces) {
      if (targetNamespace != null && !targetNamespace.equals(namespace.toString())) {
        continue;
      }
      if (!nsMatched) {
        sb.append("\n");
        nsMatched = true;
      }
      sb.append("- " + namespace + ":");
      List<TableIdentifier> tables = catalog.listTables(namespace);
      boolean tableMatched = false;
      for (TableIdentifier tableId : tables) {
        if (targetTable != null && !targetTable.equals(tableId.name())) {
          continue;
        }
        if (!tableMatched) {
          sb.append("\n");
          tableMatched = true;
        }
        sb.append("\t- " + tableId.name() + ":\n");
        Table table = catalog.loadTable(tableId);
        sb.append(
            "\t\t\tschema_raw: |-\n"
                + prefixEachLine(table.schema().toString(), "\t\t\t\t")
                + "\n");
        sb.append(
            "\t\t\tpartition_spec_raw: |-\n"
                + prefixEachLine(table.spec().toString(), "\t\t\t\t")
                + "\n");
        sb.append(
            "\t\t\tsort_order_raw: |-\n"
                + prefixEachLine(table.sortOrder().toString(), "\t\t\t\t")
                + "\n");
        sb.append("\t\t\tproperties: \n");
        for (var property : table.properties().entrySet()) {
          sb.append("\t\t\t\t" + property.getKey() + ": \"" + property.getValue() + "\"\n");
        }
        sb.append("\t\t\tlocation: " + table.location() + "\n");
        sb.append("\t\t\tcurrent_snapshot: \n");
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot != null) {
          sb.append("\t\t\t\tsequence_number: " + snapshot.sequenceNumber() + "\n");
          sb.append("\t\t\t\tid: " + snapshot.snapshotId() + "\n");
          sb.append("\t\t\t\tparent_id: " + snapshot.parentId() + "\n");
          sb.append("\t\t\t\ttimestamp: " + snapshot.timestampMillis() + "\n");
          sb.append(
              "\t\t\t\ttimestamp_iso: \""
                  + Instant.ofEpochMilli(snapshot.timestampMillis()).toString()
                  + "\"\n");
          sb.append(
              "\t\t\t\ttimestamp_iso_local: \""
                  + Instant.ofEpochMilli(snapshot.timestampMillis())
                      .atZone(ZoneId.systemDefault())
                      .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                  + "\"\n");
          sb.append("\t\t\t\toperation: " + snapshot.operation() + "\n");
          sb.append("\t\t\t\tsummary:\n");
          for (var property : snapshot.summary().entrySet()) {
            sb.append("\t\t\t\t\t" + property.getKey() + ": \"" + property.getValue() + "\"\n");
          }
          sb.append("\t\t\t\tlocation: " + snapshot.manifestListLocation() + "\n");
        }

        printTableMetrics(table, sb);
      }
      if (!tableMatched) {
        sb.append(" []\n");
      }
    }
    if (!nsMatched) {
      sb.append(" []\n");
    }
    String r = sb.toString().replace("\t", "  ");
    if (json) {
      r = convertYamlToJson(r);
    }
    System.out.println(r);
  }

  /**
   * Print table metrics
   *
   * @param table
   */
  private static void printTableMetrics(Table table, StringBuilder buffer) throws IOException {
    TableScan scan = table.newScan().includeColumnStats();
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    for (FileScanTask task : tasks) {
      DataFile dataFile = task.file();
      buffer.append("\tMetrics:\n");
      buffer.append("\t  File: " + dataFile.path() + "\n");
      buffer.append("\t  Record Count: " + dataFile.recordCount() + "\n");

      Map<Integer, Long> valueCounts = dataFile.valueCounts();
      Map<Integer, Long> nullCounts = dataFile.nullValueCounts();
      Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds();
      Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();

      for (Types.NestedField field : table.schema().columns()) {
        int id = field.fieldId();
        buffer.append("\n\t  Column: " + field.name() + "\n");
        buffer.append("\t    valueCount  = " + valueCounts.get(id) + "\n");
        buffer.append("\t    nullCount   = " + nullCounts.get(id) + "\n");

        ByteBuffer lower = lowerBounds.get(id);
        ByteBuffer upper = upperBounds.get(id);

        String lowerStr =
            lower != null ? Conversions.fromByteBuffer(field.type(), lower).toString() : "null";
        String upperStr =
            upper != null ? Conversions.fromByteBuffer(field.type(), upper).toString() : "null";

        buffer.append("\t    lowerBound  = " + lowerStr + "\n");
        buffer.append("\t    upperBound  = " + upperStr + "\n");
      }
    }

    tasks.close();
  }

  public static Object convertByteBufferToNumber(ByteBuffer buffer, Type type) {
    ByteBuffer copy = buffer.duplicate(); // Don't mutate original

    if (type.isPrimitiveType()) {
      switch (type.asPrimitiveType().typeId()) {
        case INTEGER:
          return copy.getInt();
        case LONG:
          return copy.getLong();
        case FLOAT:
          return copy.getFloat();
        case DOUBLE:
          return copy.getDouble();
        case BOOLEAN:
          return copy.get() != 0;
        case DATE:
          return copy.getInt(); // Often encoded as int days since epoch
        case TIME:
          return copy.getLong(); // microseconds since midnight
        case TIMESTAMP:
          return copy.getLong(); // microseconds since epoch
        case STRING:
          byte[] strBytes = new byte[copy.remaining()];
          copy.get(strBytes);
          return new String(strBytes, StandardCharsets.UTF_8);
        case FIXED:
        case BINARY:
          byte[] binBytes = new byte[copy.remaining()];
          copy.get(binBytes);
          return binBytes;
        case DECIMAL:
          // Example assumes precision/scale = 10,2; adjust for your schema
          BigInteger unscaled = new BigInteger(copy.array());
          return new BigDecimal(unscaled, 2);
        default:
          return "Unsupported type: " + type;
      }
    }
    return "Non-primitive type: " + type;
  }

  private static String convertYamlToJson(String yaml) throws IOException {
    ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    Object obj = yamlReader.readValue(yaml, Object.class);
    ObjectMapper jsonWriter = new ObjectMapper();
    return jsonWriter.writeValueAsString(obj);
  }

  private static String prefixEachLine(String v, String prefix) {
    return v.lines().map(line -> prefix + line).collect(Collectors.joining("\n"));
  }
}
