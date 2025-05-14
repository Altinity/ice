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

import java.util.Comparator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.Record;

public class RecordComparator implements Comparator<Record> {

  private record SortFieldWithName(SortField field, String name) {}

  private final SortFieldWithName[] sortFields;

  public RecordComparator(SortOrder sortOrder, Schema schema) {
    this.sortFields =
        sortOrder.fields().stream()
            .map(field -> new SortFieldWithName(field, schema.findColumnName(field.sourceId())))
            .toArray(SortFieldWithName[]::new);
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compare(Record r1, Record r2) {
    for (SortFieldWithName sf : sortFields) {
      Comparable<Object> v1 = (Comparable<Object>) r1.getField(sf.name);
      Comparable<Object> v2 = (Comparable<Object>) r2.getField(sf.name);

      if (v1 == null && v2 == null) continue;

      SortDirection direction = sf.field.direction();
      if (v1 == null) return direction == SortDirection.ASC ? -1 : 1;
      if (v2 == null) return direction == SortDirection.ASC ? 1 : -1;

      int cmp = v1.compareTo(v2);
      if (cmp != 0) {
        return direction == SortDirection.ASC ? cmp : -cmp;
      }
    }
    return 0;
  }
}
