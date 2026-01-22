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

import com.altinity.ice.cli.Main.PartitionFilter;
import com.altinity.ice.cli.internal.iceberg.Partitioning;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Delete {

  private static final Logger logger = LoggerFactory.getLogger(Delete.class);

  private Delete() {}

  public static void run(
      RESTCatalog catalog,
      TableIdentifier tableId,
      List<PartitionFilter> partitions,
      boolean dryRun)
      throws IOException, URISyntaxException {

    Table table = catalog.loadTable(tableId);

    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot == null) {
      logger.error("There are no snapshots in this table");
      return;
    }

    FileIO io = table.io();
    Map<Integer, PartitionSpec> specsById = table.specs();

    List<ManifestFile> dataManifests = currentSnapshot.dataManifests(io);
    List<DataFile> filesToDelete = new ArrayList<>();

    Expression expression = null;

    if (partitions != null) {
      for (PartitionFilter pf : partitions) {
        String fieldName = pf.name();

        Expression fieldExpr = null;
        for (Object value : pf.values()) {
          Integer transformed = Partitioning.applyTimestampTransform(table, fieldName, value);
          if (transformed != null) {
            value = transformed;
          }

          Expression singleValueExpr = Expressions.equal(fieldName, value);
          fieldExpr =
              fieldExpr == null ? singleValueExpr : Expressions.or(fieldExpr, singleValueExpr);
        }
        if (fieldExpr == null) {
          continue;
        }
        expression = expression == null ? fieldExpr : Expressions.and(expression, fieldExpr);
      }
    }

    for (ManifestFile manifest : dataManifests) {
      ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io, specsById);
      if (expression != null) {
        reader.filterPartitions(expression);
      }
      try (reader) {
        for (DataFile dataFile : reader) {
          filesToDelete.add(dataFile);
        }
      }
    }

    if (!filesToDelete.isEmpty()) {
      if (dryRun) {
        for (DataFile file : filesToDelete) {
          logger.info("To be deleted: {}", file.location());
        }
      } else {
        RewriteFiles rewrite = table.newRewrite();
        for (DataFile deleteFile : filesToDelete) {
          logger.info("Deleting {}", deleteFile.location());
          rewrite.deleteFile(deleteFile);
        }
        rewrite.commit();
      }
    } else {
      logger.info("No files to delete");
    }
  }
}
