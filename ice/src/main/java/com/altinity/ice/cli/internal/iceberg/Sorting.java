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
import javax.annotation.Nullable;
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

  public record SortCheckResult(
      boolean ok,
      List<Types.NestedField> projection,
      int unsortedColumnIndex,
      @Nullable Record unsortedPrev,
      @Nullable Record unsortedNext) {

    public SortCheckResult(boolean ok) {
      this(ok, List.of(), -1, null, null);
    }

    @Override
    public String toString() {
      if (ok) {
        return "sorted";
      }
      if (projection.isEmpty()) {
        return "unsorted";
      }
      return String.format("unsorted: %s", toUnsortedDiffString());
    }

    public String toUnsortedDiffString() {
      StringBuilder r1 = new StringBuilder("{");
      StringBuilder r2 = new StringBuilder("{");
      int i = 0, e = projection.size() - 1;
      for (Types.NestedField field : projection) {
        String columnName = field.name();
        r1.append(
            String.format(
                i == unsortedColumnIndex ? "*%s:%s" : "%s:%s",
                columnName,
                unsortedPrev.getField(columnName)));
        r2.append(
            String.format(
                i == unsortedColumnIndex ? "*%s:%s" : "%s:%s",
                columnName,
                unsortedNext.getField(columnName)));
        if (i != e) {
          r1.append(", ");
          r2.append(", ");
        }
        i++;
      }
      r1.append("}");
      r2.append("}");
      return String.format("expected %s to be before %s", r2, r1);
    }
  }

  public static boolean isSorted(InputFile inputFile, Schema tableSchema, SortOrder sortOrder)
      throws IOException {
    return checkSorted(inputFile, tableSchema, sortOrder).ok;
  }

  // TODO: check metadata first to avoid full scan when unsorted
  public static SortCheckResult checkSorted(
      InputFile inputFile, Schema tableSchema, SortOrder sortOrder) throws IOException {
    if (sortOrder.isUnsorted()) {
      return new SortCheckResult(false);
    }

    // TODO:
    // https://javadoc.io/doc/org.apache.parquet/parquet-format/2.4.0/org/apache/parquet/format/RowGroup.html sorting_columns
    /*
        FileMetaData fileMetaData = org.apache.parquet.format.Util.readFileMetaData(null);
        for (RowGroup rowGroup : fileMetaData.row_groups) {
          rowGroup.sorting_columns....
        }
    */

    List<SortField> sortOrderFields = sortOrder.fields();
    List<Types.NestedField> projection = new ArrayList<>();

    // Project sortOrder fields over table schema.
    for (SortField sortField : sortOrderFields) {
      String name = tableSchema.findColumnName(sortField.sourceId());
      if (name == null) {
        throw new IllegalArgumentException(
            "Sort column not found in schema: ID=" + sortField.sourceId());
      }
      Types.NestedField field = tableSchema.findField(sortField.sourceId());
      projection.add(field);
    }
    Schema projectedSchema = new Schema(projection);

    try (CloseableIterable<Record> records =
        Parquet.read(inputFile)
            .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
            .project(projectedSchema)
            .build()) {

      CloseableIterator<Record> iter = records.iterator();
      if (iter.hasNext()) {
        Record prev = iter.next();
        nextRecord:
        while (iter.hasNext()) {
          Record curr = iter.next();

          for (int i = 0; i < sortOrderFields.size(); i++) {
            SortField sortField = sortOrderFields.get(i);
            String columnName = projection.get(i).name();
            boolean asc = sortField.direction() == SortDirection.ASC;

            Object left = prev.getField(columnName);
            Object right = curr.getField(columnName);

            if (left == null && right == null) {
              continue;
            }

            if (left == null || right == null) {
              boolean nullsLast = sortField.nullOrder() == NullOrder.NULLS_LAST;
              if ((left != null && !nullsLast) || (right != null && nullsLast)) {
                return new SortCheckResult(false, projection, i, prev, curr);
              }
              // record is sorted
              continue nextRecord;
            }

            int cmp = ((Comparable<Object>) left).compareTo(right);
            if (cmp != 0) {
              if (cmp > 0 && asc || cmp < 0 && !asc) {
                return new SortCheckResult(false, projection, i, prev, curr);
              }
              // record is sorted
              continue nextRecord;
            }
          }
          prev = curr;
        }
      }
    }
    return new SortCheckResult(true);
  }
}
