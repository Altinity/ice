/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.internal.cmd;

import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.Record;

public class RecordSortComparator implements Comparator<Record> {
  private final List<SortField> sortFields;
  private final Schema schema;

  public RecordSortComparator(SortOrder sortOrder, Schema schema) {
    this.sortFields = sortOrder.fields();
    this.schema = schema;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compare(Record r1, Record r2) {
    for (SortField sf : sortFields) {
      String fieldName = schema.findColumnName(sf.sourceId());
      Comparable<Object> v1 = (Comparable<Object>) r1.getField(fieldName);
      Comparable<Object> v2 = (Comparable<Object>) r2.getField(fieldName);

      if (v1 == null && v2 == null) continue;
      if (v1 == null) return sf.direction() == SortDirection.ASC ? -1 : 1;
      if (v2 == null) return sf.direction() == SortDirection.ASC ? 1 : -1;

      int cmp = v1.compareTo(v2);
      if (cmp != 0) {
        return sf.direction() == SortDirection.ASC ? cmp : -cmp;
      }
    }
    return 0;
  }
}
