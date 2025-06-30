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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Delete {

  private static final Logger logger = LoggerFactory.getLogger(Delete.class);

  private Delete() {}

  public static void run(
      RESTCatalog catalog,
      TableIdentifier tableId,
      List<com.altinity.ice.cli.Main.PartitionFilter> partitions,
      boolean dryRun)
      throws IOException, URISyntaxException {

    Table table = catalog.loadTable(tableId);
    TableScan scan = table.newScan();
    if (partitions != null && !partitions.isEmpty()) {
      org.apache.iceberg.expressions.Expression expr = null;
      for (com.altinity.ice.cli.Main.PartitionFilter pf : partitions) {
        org.apache.iceberg.expressions.Expression e = null;
        for (Object value : pf.values()) {
          org.apache.iceberg.expressions.Expression valueExpr =
              org.apache.iceberg.expressions.Expressions.equal(pf.name(), value);
          e = (e == null) ? valueExpr : org.apache.iceberg.expressions.Expressions.or(e, valueExpr);
        }
        expr = (expr == null) ? e : org.apache.iceberg.expressions.Expressions.and(expr, e);
      }
      scan = scan.filter(expr);
    }
    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    List<DataFile> filesToDelete = new ArrayList<>();
    for (FileScanTask task : tasks) {
      filesToDelete.add(task.file());
    }
    tasks.close();

    if (!filesToDelete.isEmpty()) {
      if (dryRun) {
        logger.info("Dry run: The following files would be deleted:");
        for (DataFile file : filesToDelete) {
          logger.info("  {}", file.path());
        }
      } else {
        RewriteFiles rewrite = table.newRewrite();
        for (DataFile deleteFile : filesToDelete) {
          rewrite.deleteFile(deleteFile);
        }
        rewrite.commit();
        logger.info("Partition(s) deleted.");
      }
    } else {
      logger.info("No files found for the partition(s).");
    }
  }
}
