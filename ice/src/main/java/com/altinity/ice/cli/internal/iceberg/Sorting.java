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

import com.altinity.ice.cli.Main;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderBuilder;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

public final class Sorting {

  private Sorting() {}

  public static SortOrder newSortOrder(Schema schema, List<Main.IceSortOrder> columns) {
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    apply(builder, columns);
    return builder.build();
  }

  public static void apply(SortOrderBuilder<?> op, List<Main.IceSortOrder> columns) {
    for (Main.IceSortOrder column : columns) {
      NullOrder nullOrder = column.nullFirst() ? NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST;
      if (column.desc()) {
        op.desc(column.column(), nullOrder);
      } else {
        op.asc(column.column(), nullOrder);
      }
    }
  }

  // TODO: check metadata first to avoid full scan when unsorted
  public static boolean isSorted(InputFile inputFile, Schema tableSchema, SortOrder sortOrder)
      throws IOException {
    if (sortOrder.isUnsorted()) {
      return false;
    }

    List<SortField> sortOrderFields = sortOrder.fields();
    List<org.apache.iceberg.types.Types.NestedField> projection = new ArrayList<>();
    List<String> columnNames = new ArrayList<>();

    // Project sortOrder fields over table schema.
    for (SortField sortField : sortOrderFields) {
      String name = tableSchema.findColumnName(sortField.sourceId());
      if (name == null) {
        throw new IllegalArgumentException(
            "Sort column not found in schema: ID=" + sortField.sourceId());
      }
      Types.NestedField field = tableSchema.findField(sortField.sourceId());
      projection.add(field);
      columnNames.add(name);
    }
    Schema projectedSchema = new Schema(projection);

    try (CloseableIterable<org.apache.iceberg.data.Record> records =
        Parquet.read(inputFile)
            .createReaderFunc(s -> GenericParquetReaders.buildReader(projectedSchema, s))
            .project(projectedSchema)
            .build()) {

      CloseableIterator<org.apache.iceberg.data.Record> iter = records.iterator();
      if (iter.hasNext()) {
        org.apache.iceberg.data.Record prev = iter.next();
        while (iter.hasNext()) {
          Record curr = iter.next();
          for (int i = 0; i < sortOrderFields.size(); i++) {
            SortField sortField = sortOrderFields.get(i);
            String columnName = columnNames.get(i);
            boolean asc = sortField.direction() == SortDirection.ASC;

            Object left = prev.getField(columnName);
            Object right = curr.getField(columnName);

            if (left == null && right == null) {
              continue;
            }

            if (left == null || right == null) {
              boolean nullsLast = sortField.nullOrder() == NullOrder.NULLS_LAST;
              if ((left != null && !nullsLast) || (right != null && nullsLast)) {
                return false;
              }
              continue;
            }

            int cmp = ((Comparable<Object>) left).compareTo(right);
            if (cmp > 0 && asc || cmp < 0 && !asc) {
              return false;
            }
          }
        }
      }
    }
    return true;
  }
}
