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

import com.fasterxml.jackson.annotation.JsonProperty;
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
    ADD("add column"),
    DROP("drop column");

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

  public record ColumnDefinition(
      @JsonProperty("operation") String operation,
      @JsonProperty("column_name") String columnName,
      @JsonProperty("type") String type,
      @JsonProperty("comment") String comment) {}

  public static void run(
      Catalog catalog, TableIdentifier tableId, List<Map<String, String>> operations)
      throws IOException {

    Table table = catalog.loadTable(tableId);

    // Apply schema changes
    Transaction transaction = table.newTransaction();
    UpdateSchema updateSchema = transaction.updateSchema();

    for (Map<String, String> operation : operations) {

      // get the operation type
      ColumnDefinition columnDef = parseColumnDefinitionMap(operation);
      OperationType operationType = OperationType.fromKey(columnDef.operation());

      switch (operationType) {
        case ADD:
          Types.NestedField field =
              parseColumnType(columnDef.columnName(), columnDef.type(), columnDef.comment());
          updateSchema.addColumn(columnDef.columnName(), field.type(), columnDef.comment());
          logger.info("Adding column '{}' to table: {}", columnDef.columnName(), tableId);
          break;
        case DROP:
          // Validate that the column exists
          if (table.schema().findField(columnDef.columnName()) == null) {
            throw new IllegalArgumentException(
                "Column '" + columnDef.columnName() + "' does not exist in table: " + tableId);
          }
          updateSchema.deleteColumn(columnDef.columnName());
          logger.info("Dropping column '{}' from table: {}", columnDef.columnName(), tableId);
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

  static ColumnDefinition parseColumnDefinitionMap(Map<String, String> operationMap) {
    try {
      String operation = operationMap.get("operation");
      String columnName = operationMap.get("column_name");
      String type = operationMap.get("type");
      String comment = operationMap.get("comment");

      if (operation == null || operation.trim().isEmpty()) {
        throw new IllegalArgumentException("operation is required and cannot be empty");
      }

      if (columnName == null || columnName.trim().isEmpty()) {
        throw new IllegalArgumentException("column_name is required and cannot be empty");
      }

      // For drop column operations, type is not required
      OperationType operationType = OperationType.fromKey(operation);
      if (operationType == OperationType.ADD) {
        if (type == null || type.trim().isEmpty()) {
          throw new IllegalArgumentException(
              "type is required and cannot be empty for add column operations");
        }
      }

      return new ColumnDefinition(operation, columnName, type, comment);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid column definition: " + e.getMessage());
    }
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
}
