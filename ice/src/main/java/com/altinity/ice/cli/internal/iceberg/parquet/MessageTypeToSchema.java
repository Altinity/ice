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
import javax.annotation.Nullable;
import org.apache.iceberg.Schema;
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
    AtomicInteger nextId =
        new AtomicInteger(1000); // same offset as used by ParquetSchemaUtil.convert
    MessageTypeToType converter = new MessageTypeToType(name -> nextId.getAndIncrement());
    return new Schema(
        ParquetTypeVisitor.visit(getParquetTypeWithIds(type, nameMapping), converter)
            .asNestedType()
            .fields(),
        converter.getAliases());
  }

  private static MessageType getParquetTypeWithIds(
      MessageType type, @Nullable NameMapping nameMapping) {
    if (ParquetSchemaUtil.hasIds(type)) {
      return type;
    }
    if (nameMapping != null) {
      return ParquetSchemaUtil.applyNameMapping(type, nameMapping);
    }
    return ParquetSchemaUtil.addFallbackIds(type);
  }
}
