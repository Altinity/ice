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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlterTable {
  private static final Logger logger = LoggerFactory.getLogger(AlterTable.class);

  private AlterTable() {}

  public enum OperationType {
    ADD("add"),
    DROP("drop");

    private final String key;

    OperationType(String key) {
      this.key = key;
    }

    public String getKey() {
      return key;
    }

    public static OperationType fromKey(String key) {
      for (OperationType type : values()) {
        if (type.key.equals(key)) {
          return type;
        }
      }
      throw new IllegalArgumentException("Unsupported operation type: " + key);
    }
  }

  public static void run(
      Catalog catalog, TableIdentifier tableId, List<Map<String, String>> operations)
      throws IOException {

    Table table = catalog.loadTable(tableId);

    // Apply schema changes
    Transaction transaction = table.newTransaction();
    UpdateSchema updateSchema = transaction.updateSchema();

    for (Map<String, String> operation : operations) {
      validateOperation(operation);

      OperationType operationType = getOperationType(operation);

      switch (operationType) {
        case ADD:
          String columnDefinition = operation.get(OperationType.ADD.getKey());
          ColumnSpec columnSpec = parseColumnDefinition(columnDefinition);
          updateSchema.addColumn(columnSpec.name, columnSpec.type, columnSpec.comment);
          logger.info("Adding column '{}' to table: {}", columnSpec.name, tableId);
          break;
        case DROP:
          String columnName = operation.get(OperationType.DROP.getKey());
          // Validate that the column exists
          if (table.schema().findField(columnName) == null) {
            throw new IllegalArgumentException(
                "Column '" + columnName + "' does not exist in table: " + tableId);
          }
          updateSchema.deleteColumn(columnName);
          logger.info("Dropping column '{}' from table: {}", columnName, tableId);
          break;
        default:
          throw new IllegalArgumentException("Unsupported operation type: " + operationType);
      }
    }

    updateSchema.commit();
    transaction.commitTransaction();

    logger.info("Successfully applied {} operations to table: {}", operations.size(), tableId);
  }

  private static void validateOperation(Map<String, String> operation) {
    if (operation == null || operation.isEmpty()) {
      throw new IllegalArgumentException("Operation cannot be null or empty");
    }

    Set<String> keys = operation.keySet();
    if (keys.size() != 1) {
      throw new IllegalArgumentException(
          "Each operation must contain exactly one key. Found keys: " + keys);
    }

    String key = keys.iterator().next();
    try {
      OperationType.fromKey(key);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid operation. Supported operations: "
              + java.util.Arrays.stream(OperationType.values())
                  .map(OperationType::getKey)
                  .reduce((a, b) -> a + ", " + b)
                  .orElse("none"));
    }
  }

  private static OperationType getOperationType(Map<String, String> operation) {
    String key = operation.keySet().iterator().next();
    return OperationType.fromKey(key);
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
