/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

public class ParquetConversionsTest {

  private static final long NANOS = 1_786_467_726_576_815_622L;
  private static final long MICROS = 1_786_467_726_576_815L;

  @Test
  public void nanosBoundsWidenAroundTheInstant() {
    PrimitiveType column = timestamp(LogicalTypeAnnotation.TimeUnit.NANOS);
    Type type = Types.TimestampType.withZone();
    assertThat(bound(type, column, NANOS, true)).isEqualTo(MICROS);
    assertThat(bound(type, column, NANOS, false)).isEqualTo(MICROS + 1);
    assertThat(bound(type, column, MICROS * 1000L, true)).isEqualTo(MICROS);
    assertThat(bound(type, column, MICROS * 1000L, false)).isEqualTo(MICROS);
    assertThat(bound(type, column, -1_500L, true)).isEqualTo(-2L);
    assertThat(bound(type, column, -1_500L, false)).isEqualTo(-1L);
  }

  @Test
  public void millisScaleUpAndMicrosStay() {
    Type type = Types.TimestampType.withZone();
    assertThat(bound(type, timestamp(LogicalTypeAnnotation.TimeUnit.MILLIS), 5L, true))
        .isEqualTo(5_000L);
    assertThat(bound(type, timestamp(LogicalTypeAnnotation.TimeUnit.MICROS), MICROS, false))
        .isEqualTo(MICROS);
  }

  private static long bound(Type type, PrimitiveType column, long value, boolean lowerBound) {
    return (Long) ParquetConversions.fromParquetPrimitive(type, column, value, lowerBound).value();
  }

  private static PrimitiveType timestamp(LogicalTypeAnnotation.TimeUnit unit) {
    return org.apache.parquet.schema.Types.required(PrimitiveType.PrimitiveTypeName.INT64)
        .as(LogicalTypeAnnotation.timestampType(true, unit))
        .named("ts");
  }
}
