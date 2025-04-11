package com.altinity.ice.internal.cmd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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
