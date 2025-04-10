package com.altinity.ice.internal.cmd;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;

public final class Describe {

  private Describe() {}

  // TODO: refactor to objects -> yaml/json; StringBuilder nonsense below is unreadable
  public static void run(RESTCatalog catalog) throws IOException {
    var sb = new StringBuilder();
    List<Namespace> namespaces = catalog.listNamespaces();
    sb.append("default:\n");
    for (Namespace namespace : namespaces) {
      sb.append("- " + namespace + ":\n");
      List<TableIdentifier> tables = catalog.listTables(namespace);
      for (TableIdentifier tableId : tables) {
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
      }
    }
    System.out.println(sb.toString().replace("\t", "  "));
  }

  private static String prefixEachLine(String v, String prefix) {
    return v.lines().map(line -> prefix + line).collect(Collectors.joining("\n"));
  }
}
