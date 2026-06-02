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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

public final class UserInputParser {

  private UserInputParser() {}

  public static Literal<?> parseLiteral(Type type, String value) {
    if (!(type instanceof Type.PrimitiveType primitive)) {
      throw new UnsupportedOperationException(
          "initial_default is only supported for primitive column types, got: " + type);
    }
    return switch (primitive.typeId()) {
      case BOOLEAN -> Literal.of(Boolean.parseBoolean(value));
      case INTEGER -> Literal.of(Integer.parseInt(value));
      case LONG -> Literal.of(Long.parseLong(value));
      case FLOAT -> Literal.of(Float.parseFloat(value));
      case DOUBLE -> Literal.of(Double.parseDouble(value));
      case STRING -> Literal.of(value);
      case DATE -> Literal.of(DateTimeUtil.daysFromDate(LocalDate.parse(value)));
      case TIMESTAMP -> {
        if (((Types.TimestampType) primitive).shouldAdjustToUTC()) {
          yield Literal.of(
              DateTimeUtil.microsFromTimestamp(
                  LocalDateTime.from(OffsetDateTime.parse(value).toInstant())));
        }
        yield Literal.of(
            DateTimeUtil.microsFromTimestamp(
                LocalDateTime.from(LocalDateTime.parse(value).toInstant(ZoneOffset.UTC))));
      }
      case TIME, UUID, FIXED, BINARY, DECIMAL ->
          throw new UnsupportedOperationException(
              "initial_default is not supported for column type: " + primitive);
      default ->
          throw new UnsupportedOperationException(
              "initial_default is not supported for column type: " + primitive);
    };
  }
}
