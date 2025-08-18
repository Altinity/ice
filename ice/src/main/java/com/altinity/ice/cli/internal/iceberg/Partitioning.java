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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

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

  // TODO: fall back to path when statistics is not available
  public static @Nullable PartitionKey inferPartitionKey(
      ParquetMetadata metadata, PartitionSpec spec) {
    Schema schema = spec.schema();

    List<BlockMetaData> blocks = metadata.getBlocks();

    Record partitionRecord = GenericRecord.create(schema);

    for (PartitionField field : spec.fields()) {
      int sourceId = field.sourceId();
      String sourceName = schema.findField(sourceId).name();
      Type type = schema.findField(sourceId).type();

      Object value = null;
      Object valueTransformed = null;
      boolean same = true;

      for (BlockMetaData block : blocks) {
        Statistics<?> stats =
            block.getColumns().stream()
                .filter(c -> c.getPath().toDotString().equals(sourceName))
                .findFirst()
                .map(ColumnChunkMetaData::getStatistics)
                .orElse(null);

        if (stats == null
            || !stats.hasNonNullValue()
            || stats.genericGetMin() == null
            || stats.genericGetMax() == null) {
          same = false;
          break;
        }

        Transform<Object, Object> transform = (Transform<Object, Object>) field.transform();
        SerializableFunction<Object, Object> boundTransform = transform.bind(type);

        Object minTransformed = boundTransform.apply(stats.genericGetMin());
        Object maxTransformed = boundTransform.apply(stats.genericGetMax());

        if (!minTransformed.equals(maxTransformed)) {
          same = false;
          break;
        }

        if (valueTransformed == null) {
          valueTransformed = minTransformed;
          value = stats.genericGetMin();
        } else if (!valueTransformed.equals(minTransformed)) {
          same = false;
          break;
        }
      }

      if (same && value != null) {
        partitionRecord.setField(sourceName, decodeStatValue(value, type));
      } else {
        return null;
      }
    }

    PartitionKey partitionKey = new PartitionKey(spec, schema);
    partitionKey.wrap(partitionRecord);
    return partitionKey;
  }

  private static Object decodeStatValue(Object parquetStatValue, Type icebergType) {
    if (parquetStatValue == null) return null;
    return switch (icebergType.typeId()) {
      case STRING -> ((org.apache.parquet.io.api.Binary) parquetStatValue).toStringUsingUTF8();
      case INTEGER -> ((Number) parquetStatValue).intValue();
      case LONG -> ((Number) parquetStatValue).longValue();
      case FLOAT -> ((Number) parquetStatValue).floatValue();
      case DOUBLE -> ((Number) parquetStatValue).doubleValue();
      case BOOLEAN -> parquetStatValue;
      case DATE ->
          // Parquet DATE (INT32) is days since epoch (same as Iceberg DATE)
          ((Number) parquetStatValue).intValue();
      case TIMESTAMP ->
          // Parquet timestamp might come as INT64 (micros) or Binary; assuming long micros for now
          ((Number) parquetStatValue).longValue();
      case DECIMAL -> throw new UnsupportedOperationException();
      default -> null;
    };
  }

  public static Map<PartitionKey, List<org.apache.iceberg.data.Record>> partition(
      InputFile inputFile, Schema tableSchema, PartitionSpec partitionSpec) throws IOException {
    PartitionKey partitionKeyMold = new PartitionKey(partitionSpec, tableSchema);
    Map<PartitionKey, List<org.apache.iceberg.data.Record>> partitionedRecords = new HashMap<>();

    Parquet.ReadBuilder readBuilder =
        Parquet.read(inputFile)
            .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
            .project(tableSchema);

    try (CloseableIterable<org.apache.iceberg.data.Record> records = readBuilder.build()) {
      org.apache.iceberg.data.Record partitionRecord = GenericRecord.create(tableSchema);
      for (org.apache.iceberg.data.Record record : records) {
        for (PartitionField field : partitionSpec.fields()) {
          org.apache.iceberg.types.Types.NestedField fieldSpec =
              tableSchema.findField(field.sourceId());
          String sourceFieldName = fieldSpec.name();

          Object value = record.getField(sourceFieldName);
          if (value == null) {
            partitionRecord.setField(sourceFieldName, null); // reset as partitionRecord is reused
            continue;
          }
          Transform<?, ?> transform = field.transform();
          if (transform.isIdentity()) {
            partitionRecord.setField(
                field.name(), toGenericRecordFieldValue(value, fieldSpec.type()));
            continue;
          }
          String transformName = transform.toString();
          switch (transformName) {
            case "hour", "day", "month", "year":
              if (fieldSpec.type().typeId() != Type.TypeID.DATE) {
                value = toEpochMicros(value);
              }
              partitionRecord.setField(
                  sourceFieldName, toGenericRecordFieldValue(value, fieldSpec.type()));
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported transformation: " + transformName);
          }
        }

        partitionKeyMold.partition(partitionRecord);

        List<Record> r = partitionedRecords.get(partitionKeyMold);
        if (r == null) {
          r = new ArrayList<>();
          partitionedRecords.put(partitionKeyMold.copy(), r);
        }
        r.add(record);
      }
    }
    return partitionedRecords;
  }

  private static Object toGenericRecordFieldValue(Object v, Type icebergType) {
    if (v == null) {
      return null;
    }
    switch (icebergType.typeId()) {
      case DATE:
        if (v instanceof LocalDate) {
          return (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), (LocalDate) v);
        }
      default:
        return v;
    }
  }

  public static long toEpochMicros(Object tsValue) {
    switch (tsValue) {
      case Long l -> {
        return l;
      }
      case String s -> {
        LocalDateTime ldt = LocalDateTime.parse(s);
        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
      }
      case LocalDate localDate -> {
        return localDate.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
      }
      case LocalDateTime localDateTime -> {
        return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
      }
      case OffsetDateTime offsetDateTime -> {
        return offsetDateTime.toInstant().toEpochMilli() * 1000L;
      }
      case Instant instant -> {
        return instant.toEpochMilli() * 1000L;
      }
      default ->
          throw new UnsupportedOperationException("unexpected value type: " + tsValue.getClass());
    }
  }
}
