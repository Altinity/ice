/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

public class RecordComparatorTest {

  @Test
  public void testCompare() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "x", Types.IntegerType.get()),
            Types.NestedField.optional(2, "y", Types.IntegerType.get()));

    Record r1 = GenericRecord.create(schema);
    r1.setField("x", 1);
    r1.setField("y", 1);
    Record r2 = GenericRecord.create(schema);
    r2.setField("x", 2);
    Record r3 = GenericRecord.create(schema);
    r3.setField("y", 2);

    assertThat(
            Stream.of(r1, r2, r3)
                .sorted(new RecordComparator(SortOrder.builderFor(schema).build(), schema))
                .toList())
        .isEqualTo(List.of(r1, r2, r3));

    assertThat(
            Stream.of(r1, r2, r3)
                .sorted(
                    new RecordComparator(
                        SortOrder.builderFor(schema)
                            .sortBy("x", SortDirection.ASC, NullOrder.NULLS_FIRST)
                            .build(),
                        schema))
                .toList())
        .isEqualTo(List.of(r3, r1, r2));

    assertThat(
            Stream.of(r1, r2, r3)
                .sorted(
                    new RecordComparator(
                        SortOrder.builderFor(schema)
                            .sortBy("x", SortDirection.ASC, NullOrder.NULLS_LAST)
                            .build(),
                        schema))
                .toList())
        .isEqualTo(List.of(r1, r2, r3));

    assertThat(
            Stream.of(r1, r2, r3)
                .sorted(
                    new RecordComparator(
                        SortOrder.builderFor(schema)
                            .sortBy("y", SortDirection.DESC, NullOrder.NULLS_FIRST)
                            .build(),
                        schema))
                .toList())
        .isEqualTo(List.of(r2, r3, r1));

    assertThat(
            Stream.of(r1, r2, r3)
                .sorted(
                    new RecordComparator(
                        SortOrder.builderFor(schema)
                            .sortBy("y", SortDirection.DESC, NullOrder.NULLS_LAST)
                            .build(),
                        schema))
                .toList())
        .isEqualTo(List.of(r3, r1, r2));
  }
}
