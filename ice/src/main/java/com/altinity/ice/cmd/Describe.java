package com.altinity.ice.cmd;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;

public final class Describe {

  private Describe() {}

  // TODO: refactor to yaml/json
  public static void run(RESTCatalog catalog) throws IOException {
    var sb = new StringBuilder();
    List<Namespace> namespaces = catalog.listNamespaces();
    for (Namespace namespace : namespaces) {
      sb.append("- " + namespace + "\n");
      // List tables in the namespace
      List<TableIdentifier> tables = catalog.listTables(namespace);
      for (TableIdentifier tableId : tables) {
        sb.append("\t- " + tableId + "\n");
        Table table = catalog.loadTable(tableId);
        sb.append(
            "\t\tschema:\n"
                + table
                    .schema()
                    .toString()
                    .lines()
                    .map(line -> "\t\t\t" + line)
                    .collect(Collectors.joining("\n"))
                + "\n");
        sb.append("\t\tpartition spec: " + table.spec() + "\n");
        sb.append("\t\tlocation: " + table.location() + "\n");
        sb.append(
            "\t\tcurrent snapshot id: "
                + (table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : "none")
                + "\n");
      }
    }
    System.out.println(sb.toString().replace("\t", "  "));
  }
}
