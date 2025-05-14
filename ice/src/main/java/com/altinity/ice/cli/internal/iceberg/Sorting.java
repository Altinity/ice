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
import java.util.List;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderBuilder;

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
}
