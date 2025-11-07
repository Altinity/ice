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

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

public class SchemaEvolutionTest {

  @Test
  public void testIsSubset() {
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "x", Types.IntegerType.get())),
                new Schema(
                    Types.NestedField.optional(1, "x", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "y", Types.IntegerType.get()))))
        .isTrue();

    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(1, "x", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "y", Types.IntegerType.get())),
                new Schema(Types.NestedField.optional(1, "x", Types.IntegerType.get()))))
        .isFalse();

    // required/subset
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get())),
                new Schema(Types.NestedField.optional(1, "x", Types.IntegerType.get()))))
        .isTrue();

    // required/superset
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "x", Types.IntegerType.get())),
                new Schema(
                    Types.NestedField.optional(1, "x", Types.IntegerType.get()),
                    Types.NestedField.required(2, "y", Types.IntegerType.get()))))
        .isFalse();
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "x", Types.IntegerType.get())),
                new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get()))))
        .isFalse();

    // different type
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "x", Types.IntegerType.get())),
                new Schema(Types.NestedField.optional(1, "x", Types.DoubleType.get()))))
        .isFalse();
  }

  @Test
  public void testIsSubsetStruct() {
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get())))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get()),
                            Types.NestedField.optional(4, "d", Types.IntegerType.get()))))))
        .isTrue();

    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get()),
                            Types.NestedField.optional(4, "d", Types.IntegerType.get())))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get()))))))
        .isFalse();

    // required/missing
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get()),
                            Types.NestedField.optional(4, "d", Types.IntegerType.get())))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.required(5, "x", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get()))))))
        .isFalse();
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.required(
                                2,
                                "b",
                                Types.StructType.of(
                                    Types.NestedField.optional(
                                        3, "c", Types.IntegerType.get())))))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.required(
                                2,
                                "b",
                                Types.StructType.of(
                                    Types.NestedField.required(
                                        4, "c", Types.IntegerType.get()))))))))
        .isFalse();

    // required/subset
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.required(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get())))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get()),
                            Types.NestedField.optional(4, "d", Types.IntegerType.get()))))))
        .isTrue();

    // required/superset
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get())))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.required(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get()),
                            Types.NestedField.optional(4, "d", Types.IntegerType.get()))))))
        .isFalse();

    // different type
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get())))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.optional(2, "b", Types.DoubleType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get()),
                            Types.NestedField.optional(4, "d", Types.IntegerType.get()))))))
        .isFalse();
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get())))),
                new Schema(Types.NestedField.optional(1, "a", Types.StringType.get()))))
        .isFalse();
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "a", Types.StringType.get())),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.StructType.of(
                            Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                            Types.NestedField.optional(3, "c", Types.IntegerType.get()))))))
        .isFalse();
  }

  @Test
  public void testIsSubsetListStruct() {
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.ListType.ofOptional(
                            2,
                            Types.StructType.of(
                                Types.NestedField.optional(3, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "c", Types.IntegerType.get()))))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.ListType.ofOptional(
                            2,
                            Types.StructType.of(
                                Types.NestedField.optional(3, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "c", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "d", Types.IntegerType.get())))))))
        .isTrue();

    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.ListType.ofOptional(
                            2,
                            Types.StructType.of(
                                Types.NestedField.optional(3, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "c", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "d", Types.IntegerType.get()))))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.ListType.ofOptional(
                            2,
                            Types.StructType.of(
                                Types.NestedField.optional(3, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "c", Types.IntegerType.get())))))))
        .isFalse();

    // required/subset
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.ListType.ofRequired(
                            2,
                            Types.StructType.of(
                                Types.NestedField.required(3, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "c", Types.IntegerType.get()))))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.ListType.ofOptional(
                            2,
                            Types.StructType.of(
                                Types.NestedField.optional(3, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "c", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "d", Types.IntegerType.get())))))))
        .isTrue();

    // required/superset
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.ListType.ofOptional(
                            2,
                            Types.StructType.of(
                                Types.NestedField.optional(3, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "c", Types.IntegerType.get()))))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.ListType.ofRequired(
                            2,
                            Types.StructType.of(
                                Types.NestedField.required(3, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "c", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "d", Types.IntegerType.get())))))))
        .isFalse();

    // different type
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.ListType.ofOptional(
                            2,
                            Types.StructType.of(
                                Types.NestedField.optional(3, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "c", Types.IntegerType.get()))))),
                new Schema(
                    Types.NestedField.optional(
                        1, "a", Types.ListType.ofOptional(2, Types.StringType.get())))))
        .isFalse();
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.ListType.ofOptional(
                            2,
                            Types.StructType.of(
                                Types.NestedField.optional(3, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "c", Types.IntegerType.get()))))),
                new Schema(Types.NestedField.optional(1, "a", Types.StringType.get()))))
        .isFalse();
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "a", Types.StringType.get())),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.ListType.ofOptional(
                            2,
                            Types.StructType.of(
                                Types.NestedField.optional(3, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "c", Types.IntegerType.get())))))))
        .isFalse();
  }

  @Test
  public void testIsSubsetMapStruct() {
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofOptional(
                            2,
                            3,
                            Types.StringType.get(),
                            Types.StructType.of(
                                Types.NestedField.optional(4, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "c", Types.IntegerType.get()))))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofOptional(
                            2,
                            3,
                            Types.StringType.get(),
                            Types.StructType.of(
                                Types.NestedField.optional(4, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "c", Types.IntegerType.get()),
                                Types.NestedField.optional(6, "d", Types.IntegerType.get())))))))
        .isTrue();

    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofOptional(
                            2,
                            3,
                            Types.StringType.get(),
                            Types.StructType.of(
                                Types.NestedField.optional(4, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "c", Types.IntegerType.get()),
                                Types.NestedField.optional(6, "d", Types.IntegerType.get()))))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofOptional(
                            2,
                            3,
                            Types.StringType.get(),
                            Types.StructType.of(
                                Types.NestedField.optional(4, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "c", Types.IntegerType.get())))))))
        .isFalse();

    // required/subset
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofRequired(
                            2,
                            3,
                            Types.StringType.get(),
                            Types.StructType.of(
                                Types.NestedField.required(4, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "c", Types.IntegerType.get()))))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofOptional(
                            2,
                            3,
                            Types.StringType.get(),
                            Types.StructType.of(
                                Types.NestedField.optional(4, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "c", Types.IntegerType.get()),
                                Types.NestedField.optional(6, "d", Types.IntegerType.get())))))))
        .isTrue();

    // required/superset
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofOptional(
                            2,
                            3,
                            Types.StringType.get(),
                            Types.StructType.of(
                                Types.NestedField.optional(4, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "c", Types.IntegerType.get()))))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofRequired(
                            2,
                            3,
                            Types.StringType.get(),
                            Types.StructType.of(
                                Types.NestedField.required(4, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "c", Types.IntegerType.get()),
                                Types.NestedField.optional(6, "d", Types.IntegerType.get())))))))
        .isFalse();

    // different type
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofOptional(
                            2,
                            3,
                            Types.StringType.get(),
                            Types.StructType.of(
                                Types.NestedField.optional(4, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "c", Types.IntegerType.get()))))),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofOptional(
                            2, 3, Types.StringType.get(), Types.StringType.get())))))
        .isFalse();
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofOptional(
                            2,
                            3,
                            Types.StringType.get(),
                            Types.StructType.of(
                                Types.NestedField.optional(4, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "c", Types.IntegerType.get()))))),
                new Schema(Types.NestedField.optional(1, "a", Types.StringType.get()))))
        .isFalse();
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "a", Types.StringType.get())),
                new Schema(
                    Types.NestedField.optional(
                        1,
                        "a",
                        Types.MapType.ofOptional(
                            2,
                            3,
                            Types.StringType.get(),
                            Types.StructType.of(
                                Types.NestedField.optional(4, "b", Types.IntegerType.get()),
                                Types.NestedField.optional(5, "c", Types.IntegerType.get())))))))
        .isFalse();
  }

  @Test
  public void testIsSubsetWithTypePromo() {
    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "x", Types.IntegerType.get())),
                new Schema(Types.NestedField.optional(1, "x", Types.LongType.get()))))
        .isTrue();

    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "x", Types.LongType.get())),
                new Schema(Types.NestedField.optional(1, "x", Types.IntegerType.get()))))
        .isFalse();

    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "x", Types.FloatType.get())),
                new Schema(Types.NestedField.optional(1, "x", Types.DoubleType.get()))))
        .isTrue();

    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "x", Types.DoubleType.get())),
                new Schema(Types.NestedField.optional(1, "x", Types.FloatType.get()))))
        .isFalse();

    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "x", Types.DecimalType.of(6, 3))),
                new Schema(Types.NestedField.optional(1, "x", Types.DecimalType.of(9, 3)))))
        .isTrue();

    assertThat(
            SchemaEvolution.isSubset(
                new Schema(Types.NestedField.optional(1, "x", Types.DecimalType.of(9, 3))),
                new Schema(Types.NestedField.optional(1, "x", Types.DecimalType.of(6, 3)))))
        .isFalse();
  }
}
