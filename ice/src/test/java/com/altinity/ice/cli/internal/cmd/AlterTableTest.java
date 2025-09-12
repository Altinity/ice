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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class AlterTableTest {

  @Test
  public void testParseColumnDefinitionJson() {

    Map<String, String> operation = new HashMap<>();
    operation.put("operation", "add column");
    operation.put("column_name", "age");
    operation.put("type", "int");
    operation.put("comment", "User age");
    AlterTable.ColumnDefinition columnDef = AlterTable.parseColumnDefinitionMap(operation);

    assertEquals(columnDef.columnName(), "age");
    assertEquals(columnDef.type(), "int");
    assertEquals(columnDef.comment(), "User age");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseColumnDefinitionJsonWithoutComment() {

    Map<String, String> operation = new HashMap<>();
    operation.put("operation", "add column");
    operation.put("column_name", "name");
    operation.put("type", "string");
    AlterTable.ColumnDefinition columnDef = AlterTable.parseColumnDefinitionMap(operation);

    assertEquals(columnDef.columnName(), "name");
    assertEquals(columnDef.type(), "string");
    assertNull(columnDef.comment());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseColumnDefinitionJsonMissingColumnName() {

    Map<String, String> operation = new HashMap<>();
    operation.put("operation", "add column");
    operation.put("type", "int");
    operation.put("comment", "User age");
    AlterTable.parseColumnDefinitionMap(operation);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseColumnDefinitionJsonMissingType() {

    Map<String, String> operation = new HashMap<>();
    operation.put("operation", "add column");
    operation.put("column_name", "age");
    operation.put("comment", "User age");
    AlterTable.parseColumnDefinitionMap(operation);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseColumnDefinitionJsonEmptyColumnName() {

    Map<String, String> operation = new HashMap<>();
    operation.put("operation", "add column");
    operation.put("column_name", "");
    operation.put("type", "int");
    AlterTable.parseColumnDefinitionMap(operation);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseColumnDefinitionJsonEmptyType() {

    Map<String, String> operation = new HashMap<>();
    operation.put("operation", "add column");
    operation.put("column_name", "age");
    operation.put("type", "");
    AlterTable.parseColumnDefinitionMap(operation);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseColumnDefinitionJsonInvalidJson() {

    Map<String, String> operation = new HashMap<>();
    operation.put("operation2", "add column");
    operation.put("column_name", "age");
    operation.put("type", "int");
    AlterTable.parseColumnDefinitionMap(operation);
  }

  @Test
  public void testOperationTypeFromKey() {
    assertEquals(AlterTable.OperationType.fromKey("add column"), AlterTable.OperationType.ADD);
    assertEquals(AlterTable.OperationType.fromKey("drop column"), AlterTable.OperationType.DROP);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testOperationTypeFromKeyInvalid() {
    AlterTable.OperationType.fromKey("invalid");
  }

  @Test
  public void testParseColumnDefinitionMapForDropColumn() {

    // Test drop column operation - only needs column_name
    Map<String, String> operation = new HashMap<>();
    operation.put("operation", "drop column");
    operation.put("column_name", "age");
    AlterTable.ColumnDefinition columnDef = AlterTable.parseColumnDefinitionMap(operation);

    assertEquals(columnDef.operation(), "drop column");
    assertEquals(columnDef.columnName(), "age");
    assertNull(columnDef.type());
    assertNull(columnDef.comment());
  }
}
