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

import org.testng.annotations.Test;

public class AlterTableTest {

  @Test
  public void testParseColumnDefinitionJson() {
    // Test valid JSON with all fields
    String validJson = "{\"column_name\": \"age\", \"type\": \"int\", \"comment\": \"User age\"}";
    AlterTable.ColumnDefinition columnDef = AlterTable.parseColumnDefinitionJson(validJson);

    assertEquals(columnDef.columnName(), "age");
    assertEquals(columnDef.type(), "int");
    assertEquals(columnDef.comment(), "User age");
  }

  @Test
  public void testParseColumnDefinitionJsonWithoutComment() {
    // Test valid JSON without comment
    String validJson = "{\"column_name\": \"name\", \"type\": \"string\"}";
    AlterTable.ColumnDefinition columnDef = AlterTable.parseColumnDefinitionJson(validJson);

    assertEquals(columnDef.columnName(), "name");
    assertEquals(columnDef.type(), "string");
    assertNull(columnDef.comment());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseColumnDefinitionJsonMissingColumnName() {
    // Test JSON missing column_name
    String invalidJson = "{\"type\": \"int\", \"comment\": \"User age\"}";
    AlterTable.parseColumnDefinitionJson(invalidJson);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseColumnDefinitionJsonMissingType() {
    // Test JSON missing type
    String invalidJson = "{\"column_name\": \"age\", \"comment\": \"User age\"}";
    AlterTable.parseColumnDefinitionJson(invalidJson);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseColumnDefinitionJsonEmptyColumnName() {
    // Test JSON with empty column_name
    String invalidJson = "{\"column_name\": \"\", \"type\": \"int\"}";
    AlterTable.parseColumnDefinitionJson(invalidJson);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseColumnDefinitionJsonEmptyType() {
    // Test JSON with empty type
    String invalidJson = "{\"column_name\": \"age\", \"type\": \"\"}";
    AlterTable.parseColumnDefinitionJson(invalidJson);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseColumnDefinitionJsonInvalidJson() {
    // Test invalid JSON format
    String invalidJson = "{\"column_name\": \"age\", \"type\": \"int\"";
    AlterTable.parseColumnDefinitionJson(invalidJson);
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
}
