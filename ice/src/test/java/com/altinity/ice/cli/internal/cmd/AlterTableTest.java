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
}
