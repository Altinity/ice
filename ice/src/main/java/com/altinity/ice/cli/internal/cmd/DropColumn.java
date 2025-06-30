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
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropColumn {
  private static final Logger logger = LoggerFactory.getLogger(DropColumn.class);

  private DropColumn() {}

  public static void run(Catalog catalog, TableIdentifier tableId, String columnName)
      throws IOException {

    Table table = catalog.loadTable(tableId);

    // Validate that the column exists
    if (table.schema().findField(columnName) == null) {
      throw new IllegalArgumentException(
          "Column '" + columnName + "' does not exist in table: " + tableId);
    }

    // Apply schema change
    Transaction transaction = table.newTransaction();
    UpdateSchema updateSchema = transaction.updateSchema();

    updateSchema.deleteColumn(columnName);

    updateSchema.commit();
    transaction.commitTransaction();

    logger.info("Successfully dropped column '{}' from table: {}", columnName, tableId);
  }
}
