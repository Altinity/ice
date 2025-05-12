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
import java.util.Objects;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.expressions.Expressions;

public final class Partitioning {

  private Partitioning() {}

  public static PartitionSpec newPartitionSpec(Schema schema, List<Main.IcePartition> columns) {
    final PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    if (!columns.isEmpty()) {
      for (Main.IcePartition partition : columns) {
        String transform = Objects.requireNonNullElse(partition.transform(), "").toLowerCase();
        if (transform.startsWith("bucket[")) {
          int numBuckets = Integer.parseInt(transform.substring(7, transform.length() - 1));
          builder.bucket(partition.column(), numBuckets);
        } else if (transform.startsWith("truncate[")) {
          int width = Integer.parseInt(transform.substring(9, transform.length() - 1));
          builder.truncate(partition.column(), width);
        } else {
          switch (transform) {
            case "year":
              builder.year(partition.column());
              break;
            case "month":
              builder.month(partition.column());
              break;
            case "day":
              builder.day(partition.column());
              break;
            case "hour":
              builder.hour(partition.column());
              break;
            case "identity":
            case "":
              builder.identity(partition.column());
              break;
            default:
              throw new IllegalArgumentException("unexpected transform: " + transform);
          }
        }
      }
    }
    return builder.build();
  }

  public static void apply(UpdatePartitionSpec op, List<Main.IcePartition> columns) {
    for (Main.IcePartition partition : columns) {
      String transform = Objects.requireNonNullElse(partition.transform(), "").toLowerCase();
      if (transform.startsWith("bucket[")) {
        int numBuckets = Integer.parseInt(transform.substring(7, transform.length() - 1));
        op.addField(
            partition.column() + "_bucket", Expressions.bucket(partition.column(), numBuckets));
      } else if (transform.startsWith("truncate[")) {
        int width = Integer.parseInt(transform.substring(9, transform.length() - 1));
        op.addField(partition.column() + "_trunc", Expressions.truncate(partition.column(), width));
      } else {
        switch (transform) {
          case "year":
            op.addField(partition.column() + "_year", Expressions.year(partition.column()));
            break;
          case "month":
            op.addField(partition.column() + "_month", Expressions.month(partition.column()));
            break;
          case "day":
            op.addField(partition.column() + "_day", Expressions.day(partition.column()));
            break;
          case "hour":
            op.addField(partition.column() + "_hour", Expressions.hour(partition.column()));
            break;
          case "identity":
          case "":
            op.addField(partition.column());
            break;
          default:
            throw new IllegalArgumentException("unexpected transform: " + transform);
        }
      }
    }
  }
}
