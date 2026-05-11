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

import com.altinity.ice.cli.internal.util.TreePrinter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;

public final class Files {

  private Files() {}

  public static void run(RESTCatalog catalog, TableIdentifier tableId) throws IOException {
    Table table = catalog.loadTable(tableId);
    Snapshot snapshot = table.currentSnapshot();

    String tableName = tableId.toString();
    String rootLabel = "Snapshots: " + tableName;

    if (snapshot == null) {
      System.out.println(rootLabel);
      System.out.println("(no snapshots)");
      return;
    }

    int schemaId = snapshot.schemaId() != null ? snapshot.schemaId() : 0;
    String manifestListLocation = snapshot.manifestListLocation();
    String locationStr = manifestListLocation != null ? manifestListLocation : "(embedded)";
    String snapshotLabel =
        "Snapshot " + snapshot.snapshotId() + ", schema " + schemaId + ": " + locationStr;

    FileIO tableIO = table.io();
    List<ManifestFile> manifests;
    try {
      manifests = snapshot.allManifests(tableIO);
    } catch (Exception e) {
      TreePrinter.print(
          new TreePrinter.Node(rootLabel, List.of(new TreePrinter.Node(snapshotLabel))));
      System.out.println("    (failed to read manifests: " + e.getMessage() + ")");
      return;
    }

    List<TreePrinter.Node> manifestNodes = new ArrayList<>(manifests.size());
    for (ManifestFile manifest : manifests) {
      List<TreePrinter.Node> dataFileNodes = new ArrayList<>();
      try (CloseableIterable<DataFile> files = ManifestFiles.read(manifest, tableIO)) {
        for (DataFile file : files) {
          dataFileNodes.add(new TreePrinter.Node("Datafile: " + file.location()));
        }
      } catch (Exception e) {
        dataFileNodes.add(
            new TreePrinter.Node("Datafile: (failed to read: " + e.getMessage() + ")"));
      }
      manifestNodes.add(new TreePrinter.Node("Manifest: " + manifest.path(), dataFileNodes));
    }

    TreePrinter.Node root =
        new TreePrinter.Node(
            rootLabel, List.of(new TreePrinter.Node(snapshotLabel, manifestNodes)));
    TreePrinter.print(root);
  }
}
