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

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

public class IceSchemaParserTest {

  @Test
  public void testSimpleSchema() throws IOException {
    Schema schema =
        IceSchemaParser.parse(
            "[{\"name\":\"id\",\"type\":\"long\",\"required\":true},"
                + "{\"name\":\"payload\",\"type\":\"unknown\"}]");
    assertThat(schema.columns()).hasSize(2);

    Types.NestedField id = schema.findField("id");
    assertThat(id.isRequired()).isTrue();
    assertThat(id.type().typeId()).isEqualTo(Type.TypeID.LONG);

    Types.NestedField payload = schema.findField("payload");
    assertThat(payload.isOptional()).isTrue();
    assertThat(payload.type().typeId()).isEqualTo(Type.TypeID.UNKNOWN);
  }

  @Test
  public void testV3OnlyTypes() throws IOException {
    Schema schema =
        IceSchemaParser.parse(
            "[{\"name\":\"u\",\"type\":\"unknown\"},"
                + "{\"name\":\"t\",\"type\":\"timestamp_ns\"},"
                + "{\"name\":\"g\",\"type\":\"geometry\"}]");
    assertThat(schema.findField("u").type().typeId()).isEqualTo(Type.TypeID.UNKNOWN);
    assertThat(schema.findField("t").type().typeId()).isEqualTo(Type.TypeID.TIMESTAMP_NANO);
    assertThat(schema.findField("g").type().typeId()).isEqualTo(Type.TypeID.GEOMETRY);
  }

  @Test
  public void testNestedStructWithUnknown() throws IOException {
    Schema schema =
        IceSchemaParser.parse(
            "[{\"name\":\"id\",\"type\":\"long\"},"
                + "{\"name\":\"s\",\"type\":\"struct<a:string,b:unknown>\"}]");
    Types.StructType struct = (Types.StructType) schema.findField("s").type();
    assertThat(struct.field("a").type().typeId()).isEqualTo(Type.TypeID.STRING);
    assertThat(struct.field("b").type().typeId()).isEqualTo(Type.TypeID.UNKNOWN);
  }

  @Test
  public void testFieldIdsAreUniqueAcrossNestedFields() throws IOException {
    Schema schema =
        IceSchemaParser.parse(
            "[{\"name\":\"a\",\"type\":\"struct<x:string,y:long>\"},"
                + "{\"name\":\"b\",\"type\":\"list<string>\"}]");
    Map<Integer, Types.NestedField> byId = TypeUtil.indexById(schema.asStruct());
    // IDs are reassigned via TypeUtil.assignIncreasingFreshIds to match the IDs the catalog
    // assigns at table creation: top-level fields first, then nested fields.
    assertThat(byId).hasSize(5);
    assertThat(byId.get(1).name()).isEqualTo("a");
    assertThat(byId.get(2).name()).isEqualTo("b");
    assertThat(byId.get(3).name()).isEqualTo("x");
    assertThat(byId.get(4).name()).isEqualTo("y");
    assertThat(byId.get(5).name()).isEqualTo("element");
  }

  @Test
  public void testRequiredAndDoc() throws IOException {
    Schema schema =
        IceSchemaParser.parse(
            "[{\"name\":\"id\",\"type\":\"long\",\"required\":true,\"doc\":\"primary key\"}]");
    Types.NestedField id = schema.findField("id");
    assertThat(id.isRequired()).isTrue();
    assertThat(id.doc()).isEqualTo("primary key");
  }

  @Test
  public void testDuplicateFieldNames() {
    assertThatThrownBy(
            () ->
                IceSchemaParser.parse(
                    "[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"id\",\"type\":\"string\"}]"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate field name");
  }

  @Test
  public void testEmptySchema() {
    assertThatThrownBy(() -> IceSchemaParser.parse("[]"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one field");
  }

  @Test
  public void testMalformedJson() {
    assertThatThrownBy(() -> IceSchemaParser.parse("[{\"name\":\"id\""))
        .isInstanceOf(IOException.class);
  }

  @Test
  public void testMissingType() {
    assertThatThrownBy(() -> IceSchemaParser.parse("[{\"name\":\"id\"}]"))
        .isInstanceOf(IOException.class);
  }
}
