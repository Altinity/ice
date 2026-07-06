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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AlterTableTest {

  private InMemoryCatalog catalog;
  private TableIdentifier tableId;
  private Schema schema;

  @BeforeMethod
  public void setUp() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test-catalog", java.util.Map.of());
    tableId = TableIdentifier.of("test", "table1");

    // create namespace.
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("test"));
    schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "timestamp_col", Types.TimestampType.withZone()),
            Types.NestedField.required(4, "date_col", Types.DateType.get()));
  }

  @Test
  public void testDropPartitionField() throws Exception {
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("name").year("timestamp_col").build();

    Table table = catalog.buildTable(tableId, schema).withPartitionSpec(partitionSpec).create();

    assertThat(table.spec().fields()).hasSize(2);
    assertThat(table.spec().fields().get(0).name()).isEqualTo("name");
    assertThat(table.spec().fields().get(1).name()).isEqualTo("timestamp_col_year");

    List<AlterTable.Update> updates = Arrays.asList(new AlterTable.DropPartitionField("name"));

    AlterTable.run(catalog, tableId, updates);

    table = catalog.loadTable(tableId);
    assertThat(table.spec().fields()).hasSize(1);
    assertThat(table.spec().fields().get(0).name()).isEqualTo("timestamp_col_year");
  }

  @Test
  public void testDropPartitionFieldByTransformName() throws Exception {
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("name").year("timestamp_col").build();

    Table table = catalog.buildTable(tableId, schema).withPartitionSpec(partitionSpec).create();

    assertThat(table.spec().fields()).hasSize(2);

    List<AlterTable.Update> updates =
        Arrays.asList(new AlterTable.DropPartitionField("timestamp_col_year"));

    AlterTable.run(catalog, tableId, updates);

    table = catalog.loadTable(tableId);
    assertThat(table.spec().fields()).hasSize(1);
    assertThat(table.spec().fields().get(0).name()).isEqualTo("name");
  }

  @Test
  public void testDropNonExistentPartitionField() throws Exception {
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("name").build();

    catalog.buildTable(tableId, schema).withPartitionSpec(partitionSpec).create();

    List<AlterTable.Update> updates =
        Arrays.asList(new AlterTable.DropPartitionField("non_existent_field"));

    assertThatThrownBy(() -> AlterTable.run(catalog, tableId, updates))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testDropAllPartitionFields() throws Exception {
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("name").year("timestamp_col").build();

    Table table = catalog.buildTable(tableId, schema).withPartitionSpec(partitionSpec).create();

    assertThat(table.spec().fields()).hasSize(2);

    List<AlterTable.Update> updates =
        Arrays.asList(
            new AlterTable.DropPartitionField("name"),
            new AlterTable.DropPartitionField("timestamp_col_year"));

    AlterTable.run(catalog, tableId, updates);

    table = catalog.loadTable(tableId);
    assertThat(table.spec().fields()).isEmpty();
  }

  @Test
  public void testAddColumnAfter() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(
            new AlterTable.AddColumn("age", "long", null, "name", null, null, null, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    assertThat(table.schema().columns().stream().map(Types.NestedField::name).toList())
        .containsExactly("id", "name", "age", "timestamp_col", "date_col");
  }

  @Test
  public void testAddColumnBefore() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(
            new AlterTable.AddColumn("age", "long", null, null, "timestamp_col", null, null, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    assertThat(table.schema().columns().stream().map(Types.NestedField::name).toList())
        .containsExactly("id", "name", "age", "timestamp_col", "date_col");
  }

  @Test
  public void testAddColumnFirst() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(new AlterTable.AddColumn("age", "long", null, null, null, true, null, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    assertThat(table.schema().columns().get(0).name()).isEqualTo("age");
    assertThat(table.schema().columns().stream().map(Types.NestedField::name).toList())
        .containsExactly("age", "id", "name", "timestamp_col", "date_col");
  }

  @Test
  public void testAddColumnAfterWinsWhenBothAfterAndBeforeSet() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(
            new AlterTable.AddColumn("bad", "string", null, "name", "id", null, null, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    assertThat(table.schema().columns().stream().map(Types.NestedField::name).toList())
        .containsExactly("id", "name", "bad", "timestamp_col", "date_col");
  }

  @Test
  public void testAddRequiredColumnOnEmptyTable() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(new AlterTable.AddColumn("age", "long", null, null, null, null, true, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    assertThat(table.schema().findField("age").isRequired()).isTrue();
  }

  @Test
  public void testAddOptionalColumnByDefault() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(new AlterTable.AddColumn("age", "long", null, null, null, null, null, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    assertThat(table.schema().findField("age").isOptional()).isTrue();
  }

  @Test
  public void testAddListOfStringColumn() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(
            new AlterTable.AddColumn("tags", "list<string>", null, null, null, null, null, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    Types.NestedField field = table.schema().findField("tags");
    assertThat(field).isNotNull();
    assertThat(field.isOptional()).isTrue();
    assertThat(field.type()).isInstanceOf(Types.ListType.class);
    Types.ListType listType = (Types.ListType) field.type();
    assertThat(listType.elementType()).isInstanceOf(Types.StringType.class);
  }

  @Test
  public void testAddListOfLongColumn() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(
            new AlterTable.AddColumn("scores", "list<long>", null, null, null, null, null, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    Types.NestedField field = table.schema().findField("scores");
    assertThat(field).isNotNull();
    assertThat(field.type()).isInstanceOf(Types.ListType.class);
    Types.ListType listType = (Types.ListType) field.type();
    assertThat(listType.elementType()).isInstanceOf(Types.LongType.class);
  }

  @Test
  public void testAddListColumnAfter() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(
            new AlterTable.AddColumn("tags", "list<string>", null, "name", null, null, null, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    assertThat(table.schema().columns().stream().map(Types.NestedField::name).toList())
        .containsExactly("id", "name", "tags", "timestamp_col", "date_col");
    assertThat(table.schema().findField("tags").type()).isInstanceOf(Types.ListType.class);
  }

  @Test
  public void testAddMapColumn() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(
            new AlterTable.AddColumn(
                "metadata", "map<string,long>", null, null, null, null, null, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    Types.NestedField field = table.schema().findField("metadata");
    assertThat(field).isNotNull();
    assertThat(field.type()).isInstanceOf(Types.MapType.class);
    Types.MapType mapType = (Types.MapType) field.type();
    assertThat(mapType.keyType()).isInstanceOf(Types.StringType.class);
    assertThat(mapType.valueType()).isInstanceOf(Types.LongType.class);
  }

  @Test
  public void testAddStructColumn() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(
            new AlterTable.AddColumn(
                "address", "struct<street:string,zip:int>", null, null, null, null, null, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    Types.NestedField field = table.schema().findField("address");
    assertThat(field).isNotNull();
    assertThat(field.type()).isInstanceOf(Types.StructType.class);
    Types.StructType structType = (Types.StructType) field.type();
    assertThat(structType.fields()).hasSize(2);
    assertThat(structType.field("street").type()).isInstanceOf(Types.StringType.class);
    assertThat(structType.field("zip").type()).isInstanceOf(Types.IntegerType.class);
  }

  @Test
  public void testAddNestedListOfStructColumn() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(
            new AlterTable.AddColumn(
                "items",
                "list<struct<product_id:long,name:string>>",
                null,
                null,
                null,
                null,
                null,
                null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    Types.NestedField field = table.schema().findField("items");
    assertThat(field).isNotNull();
    assertThat(field.type().typeId()).isEqualTo(Type.TypeID.LIST);
    Types.ListType listType = (Types.ListType) field.type();
    assertThat(listType.elementType().typeId()).isEqualTo(Type.TypeID.STRUCT);
    Types.StructType inner = (Types.StructType) listType.elementType();
    assertThat(inner.fields()).hasSize(2);
    assertThat(inner.field("product_id").type()).isInstanceOf(Types.LongType.class);
    assertThat(inner.field("name").type()).isInstanceOf(Types.StringType.class);
  }

  @Test
  public void testPrimitiveTypeStillWorks() throws Exception {
    catalog.buildTable(tableId, schema).create();

    List<AlterTable.Update> updates =
        Arrays.asList(
            new AlterTable.AddColumn("score", "double", null, null, null, null, null, null));

    AlterTable.run(catalog, tableId, updates);

    Table table = catalog.loadTable(tableId);
    Types.NestedField field = table.schema().findField("score");
    assertThat(field).isNotNull();
    assertThat(field.type()).isInstanceOf(Types.DoubleType.class);
  }
}
