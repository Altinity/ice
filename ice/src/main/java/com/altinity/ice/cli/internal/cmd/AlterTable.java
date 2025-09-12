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

  // Static constants for column definition keys
  private static final String OPERATION_KEY = "operation";
  private static final String COLUMN_NAME_KEY = "column_name";
  private static final String TYPE_KEY = "type";
  private static final String COMMENT_KEY = "comment";

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

  static ColumnDefinition parseColumnDefinitionMap(Map<String, String> operationMap) {
    try {
      String operation = operationMap.get(OPERATION_KEY);
      String columnName = operationMap.get(COLUMN_NAME_KEY);
      String type = operationMap.get(TYPE_KEY);
      String comment = operationMap.get(COMMENT_KEY);

      if (operation == null || operation.trim().isEmpty()) {
        throw new IllegalArgumentException(OPERATION_KEY + " is required and cannot be empty");
      }

      if (columnName == null || columnName.trim().isEmpty()) {
        throw new IllegalArgumentException(COLUMN_NAME_KEY + " is required and cannot be empty");
      }

      // For drop column operations, type is not required
      OperationType operationType = OperationType.fromKey(operation);
      if (operationType == OperationType.ADD) {
        if (type == null || type.trim().isEmpty()) {
          throw new IllegalArgumentException(
              TYPE_KEY + " is required and cannot be empty for add column operations");
        }
        if (comment == null || comment.trim().isEmpty()) {
          throw new IllegalArgumentException(
              COMMENT_KEY + " is required and cannot be empty for add column operations");
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
