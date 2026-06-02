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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.testng.annotations.Test;

public class UserInputParserTest {

  @Test
  public void testBoolean() {
    assertThat(UserInputParser.parseLiteral(Types.BooleanType.get(), "true").value())
        .isEqualTo(true);
    assertThat(UserInputParser.parseLiteral(Types.BooleanType.get(), "false").value())
        .isEqualTo(false);
  }

  @Test
  public void testInteger() {
    assertThat(UserInputParser.parseLiteral(Types.IntegerType.get(), "42").value()).isEqualTo(42);
  }

  @Test
  public void testLong() {
    assertThat(UserInputParser.parseLiteral(Types.LongType.get(), "123456789012").value())
        .isEqualTo(123456789012L);
  }

  @Test
  public void testFloat() {
    assertThat(UserInputParser.parseLiteral(Types.FloatType.get(), "3.14").value())
        .isEqualTo(3.14f);
  }

  @Test
  public void testDouble() {
    assertThat(UserInputParser.parseLiteral(Types.DoubleType.get(), "2.718281828").value())
        .isEqualTo(2.718281828);
  }

  @Test
  public void testString() {
    assertThat(UserInputParser.parseLiteral(Types.StringType.get(), "hello").value())
        .isEqualTo("hello");
  }

  @Test
  public void testDate() {
    String value = "2024-06-15";
    assertThat(UserInputParser.parseLiteral(Types.DateType.get(), value).value())
        .isEqualTo(DateTimeUtil.daysFromDate(LocalDate.parse(value)));
  }

  @Test
  public void testTimestampWithoutZone() {
    String value = "2024-06-15T12:30:45";
    long expected = DateTimeUtil.microsFromTimestamp(LocalDateTime.parse(value));
    assertThat(UserInputParser.parseLiteral(Types.TimestampType.withoutZone(), value).value())
        .isEqualTo(expected);
  }

  @Test
  public void testTimestampWithZone() {
    String value = "2024-06-15T12:30:45+00:00";
    long expected =
        DateTimeUtil.microsFromTimestamp(
            LocalDateTime.ofInstant(OffsetDateTime.parse(value).toInstant(), ZoneOffset.UTC));
    assertThat(UserInputParser.parseLiteral(Types.TimestampType.withZone(), value).value())
        .isEqualTo(expected);
  }

  @Test
  public void testUnsupportedPrimitive() {
    assertThatThrownBy(() -> UserInputParser.parseLiteral(Types.TimeType.get(), "12:30:45"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("initial_default is not supported");
  }

  @Test
  public void testNonPrimitiveType() {
    Type listType = Types.ListType.ofRequired(1, Types.StringType.get());
    assertThatThrownBy(() -> UserInputParser.parseLiteral(listType, "x"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("initial_default is only supported for primitive column types");
  }
}
