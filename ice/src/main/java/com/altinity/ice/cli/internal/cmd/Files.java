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

    if (snapshot == null) {
      System.out.println("Snapshots: " + tableId);
      System.out.println("(no snapshots)");
      return;
    }

    String tableName = tableId.toString();
    int schemaId = snapshot.schemaId() != null ? snapshot.schemaId() : 0;
    String manifestListLocation = snapshot.manifestListLocation();
    String locationStr = manifestListLocation != null ? manifestListLocation : "(embedded)";

    System.out.println("Snapshots: " + tableName);
    System.out.println(
        "└── Snapshot " + snapshot.snapshotId() + ", schema " + schemaId + ": " + locationStr);

    FileIO tableIO = table.io();
    List<ManifestFile> manifests;
    try {
      manifests = snapshot.allManifests(tableIO);
    } catch (Exception e) {
      System.out.println("    (failed to read manifests: " + e.getMessage() + ")");
      return;
    }

    for (int m = 0; m < manifests.size(); m++) {
      ManifestFile manifest = manifests.get(m);
      boolean isLastManifest = (m == manifests.size() - 1);
      String manifestPrefix = isLastManifest ? "└── " : "├── ";
      String childConnector = isLastManifest ? "    " : "│   ";

      List<String> dataFileLocations = new ArrayList<>();
      try (CloseableIterable<DataFile> files = ManifestFiles.read(manifest, tableIO)) {
        for (DataFile file : files) {
          dataFileLocations.add(file.location());
        }
      } catch (Exception e) {
        dataFileLocations.add("(failed to read: " + e.getMessage() + ")");
      }

      System.out.println("    " + manifestPrefix + "Manifest: " + manifest.path());

      String dataFileIndent = "    " + childConnector;
      for (int f = 0; f < dataFileLocations.size(); f++) {
        boolean isLastFile = (f == dataFileLocations.size() - 1);
        String filePrefix = isLastFile ? "└── " : "├── ";
        System.out.println(dataFileIndent + filePrefix + "Datafile: " + dataFileLocations.get(f));
      }
    }
  }
}
