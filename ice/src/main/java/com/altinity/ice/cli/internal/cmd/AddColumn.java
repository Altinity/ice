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
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddColumn {
  private static final Logger logger = LoggerFactory.getLogger(AddColumn.class);

  private AddColumn() {}

  public static void run(Catalog catalog, TableIdentifier tableId, String columnDefinition)
      throws IOException {

    Table table = catalog.loadTable(tableId);

    // Parse column definition
    ColumnSpec columnSpec = parseColumnDefinition(columnDefinition);

    // Apply schema change
    Transaction transaction = table.newTransaction();
    UpdateSchema updateSchema = transaction.updateSchema();

    updateSchema.addColumn(columnSpec.name, columnSpec.type, columnSpec.comment);

    updateSchema.commit();
    transaction.commitTransaction();

    logger.info("Successfully added column '{}' to table: {}", columnSpec.name, tableId);
  }

  private static ColumnSpec parseColumnDefinition(String columnDefinition) {
    String[] parts = columnDefinition.split(":");
    if (parts.length < 2) {
      throw new IllegalArgumentException(
          "Invalid column definition format. Expected: name:type[:comment] (e.g. 'age:int:User age')");
    }

    String columnName = parts[0];
    String columnType = parts[1];
    String comment = parts.length > 2 ? parts[2] : null;

    Types.NestedField field = parseColumnType(columnName, columnType, comment);

    return new ColumnSpec(columnName, field.type(), comment);
  }

  private static Types.NestedField parseColumnType(String name, String type, String comment) {
    Types.NestedField field;

    switch (type.toLowerCase()) {
      case "string":
      case "varchar":
        field = Types.NestedField.optional(-1, name, Types.StringType.get(), comment);
        break;
      case "int":
      case "integer":
        field = Types.NestedField.optional(-1, name, Types.IntegerType.get(), comment);
        break;
      case "long":
      case "bigint":
        field = Types.NestedField.optional(-1, name, Types.LongType.get(), comment);
        break;
      case "double":
        field = Types.NestedField.optional(-1, name, Types.DoubleType.get(), comment);
        break;
      case "float":
        field = Types.NestedField.optional(-1, name, Types.FloatType.get(), comment);
        break;
      case "boolean":
        field = Types.NestedField.optional(-1, name, Types.BooleanType.get(), comment);
        break;
      case "date":
        field = Types.NestedField.optional(-1, name, Types.DateType.get(), comment);
        break;
      case "timestamp":
        field = Types.NestedField.optional(-1, name, Types.TimestampType.withoutZone(), comment);
        break;
      case "timestamptz":
        field = Types.NestedField.optional(-1, name, Types.TimestampType.withZone(), comment);
        break;
      case "binary":
        field = Types.NestedField.optional(-1, name, Types.BinaryType.get(), comment);
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported column type: "
                + type
                + ". Supported types: string, int, long, double, float, boolean, date, timestamp, timestamptz, binary");
    }

    return field;
  }

  private static class ColumnSpec {
    final String name;
    final org.apache.iceberg.types.Type type;
    final String comment;

    ColumnSpec(String name, org.apache.iceberg.types.Type type, String comment) {
      this.name = name;
      this.type = type;
      this.comment = comment;
    }
  }
}
