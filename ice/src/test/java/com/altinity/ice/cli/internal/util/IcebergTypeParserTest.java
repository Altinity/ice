/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

public class IcebergTypeParserTest {

  @Test
  public void testPrimitiveString() {
    Type type = IcebergTypeParser.parseType("string");
    assertThat(type).isInstanceOf(Types.StringType.class);
  }

  @Test
  public void testPrimitiveLong() {
    Type type = IcebergTypeParser.parseType("long");
    assertThat(type).isInstanceOf(Types.LongType.class);
  }

  @Test
  public void testPrimitiveInt() {
    Type type = IcebergTypeParser.parseType("int");
    assertThat(type).isInstanceOf(Types.IntegerType.class);
  }

  @Test
  public void testPrimitiveDecimal() {
    Type type = IcebergTypeParser.parseType("decimal(10,2)");
    assertThat(type).isInstanceOf(Types.DecimalType.class);
    Types.DecimalType decimal = (Types.DecimalType) type;
    assertThat(decimal.precision()).isEqualTo(10);
    assertThat(decimal.scale()).isEqualTo(2);
  }

  @Test
  public void testListOfString() {
    Type type = IcebergTypeParser.parseType("list<string>");
    assertThat(type).isInstanceOf(Types.ListType.class);
    Types.ListType listType = (Types.ListType) type;
    assertThat(listType.elementType()).isInstanceOf(Types.StringType.class);
    assertThat(listType.isElementOptional()).isTrue();
  }

  @Test
  public void testListOfLong() {
    Type type = IcebergTypeParser.parseType("list<long>");
    assertThat(type).isInstanceOf(Types.ListType.class);
    Types.ListType listType = (Types.ListType) type;
    assertThat(listType.elementType()).isInstanceOf(Types.LongType.class);
  }

  @Test
  public void testMapStringLong() {
    Type type = IcebergTypeParser.parseType("map<string,long>");
    assertThat(type).isInstanceOf(Types.MapType.class);
    Types.MapType mapType = (Types.MapType) type;
    assertThat(mapType.keyType()).isInstanceOf(Types.StringType.class);
    assertThat(mapType.valueType()).isInstanceOf(Types.LongType.class);
  }

  @Test
  public void testMapWithSpaces() {
    Type type = IcebergTypeParser.parseType("map< string , long >");
    assertThat(type).isInstanceOf(Types.MapType.class);
    Types.MapType mapType = (Types.MapType) type;
    assertThat(mapType.keyType()).isInstanceOf(Types.StringType.class);
    assertThat(mapType.valueType()).isInstanceOf(Types.LongType.class);
  }

  @Test
  public void testSimpleStruct() {
    Type type = IcebergTypeParser.parseType("struct<a:string,b:long>");
    assertThat(type).isInstanceOf(Types.StructType.class);
    Types.StructType structType = (Types.StructType) type;
    assertThat(structType.fields()).hasSize(2);
    assertThat(structType.field("a").type()).isInstanceOf(Types.StringType.class);
    assertThat(structType.field("b").type()).isInstanceOf(Types.LongType.class);
  }

  @Test
  public void testNestedListOfStruct() {
    Type type = IcebergTypeParser.parseType("list<struct<id:long,name:string>>");
    assertThat(type).isInstanceOf(Types.ListType.class);
    Types.ListType listType = (Types.ListType) type;
    assertThat(listType.elementType()).isInstanceOf(Types.StructType.class);
    Types.StructType inner = (Types.StructType) listType.elementType();
    assertThat(inner.fields()).hasSize(2);
    assertThat(inner.field("id").type()).isInstanceOf(Types.LongType.class);
    assertThat(inner.field("name").type()).isInstanceOf(Types.StringType.class);
  }

  @Test
  public void testMapWithComplexValue() {
    Type type = IcebergTypeParser.parseType("map<string,list<int>>");
    assertThat(type).isInstanceOf(Types.MapType.class);
    Types.MapType mapType = (Types.MapType) type;
    assertThat(mapType.keyType()).isInstanceOf(Types.StringType.class);
    assertThat(mapType.valueType()).isInstanceOf(Types.ListType.class);
    Types.ListType valueList = (Types.ListType) mapType.valueType();
    assertThat(valueList.elementType()).isInstanceOf(Types.IntegerType.class);
  }

  @Test
  public void testStructWithNestedList() {
    Type type = IcebergTypeParser.parseType("struct<name:string,tags:list<string>>");
    assertThat(type).isInstanceOf(Types.StructType.class);
    Types.StructType structType = (Types.StructType) type;
    assertThat(structType.field("name").type()).isInstanceOf(Types.StringType.class);
    assertThat(structType.field("tags").type()).isInstanceOf(Types.ListType.class);
  }

  @Test
  public void testCaseInsensitive() {
    Type type = IcebergTypeParser.parseType("LIST<STRING>");
    assertThat(type).isInstanceOf(Types.ListType.class);
    Types.ListType listType = (Types.ListType) type;
    assertThat(listType.elementType()).isInstanceOf(Types.StringType.class);
  }

  @Test
  public void testWhitespaceHandling() {
    Type type = IcebergTypeParser.parseType("  list< string >  ");
    assertThat(type).isInstanceOf(Types.ListType.class);
  }

  @Test
  public void testEmptyTypeStringThrows() {
    assertThatThrownBy(() -> IcebergTypeParser.parseType(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be empty");
  }

  @Test
  public void testEmptyListThrows() {
    assertThatThrownBy(() -> IcebergTypeParser.parseType("list<>"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be empty");
  }

  @Test
  public void testMapWrongArityThrows() {
    assertThatThrownBy(() -> IcebergTypeParser.parseType("map<string>"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactly 2 type parameters");
  }

  @Test
  public void testStructMissingColonThrows() {
    assertThatThrownBy(() -> IcebergTypeParser.parseType("struct<name>"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("name:type");
  }

  @Test
  public void testInvalidPrimitiveThrows() {
    assertThatThrownBy(() -> IcebergTypeParser.parseType("foobar"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
