/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.iceberg.parquet;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.MessageTypeToType;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetTypeVisitor;
import org.apache.iceberg.types.TypeUtil;
import org.apache.parquet.schema.MessageType;

public class MessageTypeToSchema {

  public static org.apache.iceberg.Schema convertInitial(MessageType t) {
    // https://github.com/apache/iceberg/blob/16fa673782233eb2b692d463deb8be3e52c5f378/core/src/main/java/org/apache/iceberg/TableMetadata.java#L117-L120
    return TypeUtil.assignIncreasingFreshIds(convert(t));
  }

  public static org.apache.iceberg.Schema convert(MessageType t) {
    return convert(t, null);
  }

  public static org.apache.iceberg.Schema convert(
      MessageType type, @Nullable NameMapping nameMapping) {
    Function<String[], Integer> fieldWithoutIdToIdFunc;
    if (ParquetSchemaUtil.hasIds(type)) {
      fieldWithoutIdToIdFunc =
          name -> {
            throw new IllegalStateException(
                String.format("Field missing id: %s", String.join(".", name)));
          };
    } else {
      AtomicInteger nextId =
          new AtomicInteger(1000); // same offset as used by ParquetSchemaUtil.convert
      fieldWithoutIdToIdFunc =
          name -> {
            MappedField mappedField = nameMapping != null ? nameMapping.find(name) : null;
            if (mappedField != null) {
              return mappedField.id();
            }
            return nextId.getAndIncrement();
          };
    }
    MessageTypeToType converter = new MessageTypeToType(fieldWithoutIdToIdFunc);
    return new Schema(
        ParquetTypeVisitor.visit(type, converter).asNestedType().fields(), converter.getAliases());
  }
}
