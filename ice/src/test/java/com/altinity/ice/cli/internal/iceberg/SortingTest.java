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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.altinity.ice.cli.internal.iceberg.parquet.Metadata;
import com.altinity.ice.test.Resource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

public class SortingTest {

  @Test
  public void testIsSorted() throws Exception {
    InputFile inputFile =
        Resource.asInputFile("com/altinity/ice/cli/internal/iceberg/parquet/sample-001.parquet");
    ParquetMetadata metadata = Metadata.read(inputFile);

    MessageType type = metadata.getFileMetaData().getSchema();
    Schema schema = ParquetSchemaUtil.convert(type);

    assertThat(
            Sorting.isSorted(
                inputFile,
                schema,
                SortOrder.builderFor(schema)
                    .sortBy("t", SortDirection.ASC, NullOrder.NULLS_FIRST)
                    .sortBy("seq", SortDirection.ASC, NullOrder.NULLS_FIRST)
                    .build()))
        .isTrue();
  }

  // A Parquet file without Iceberg field IDs where the sort
  // column's physical position differs from its declared field ID. Without a name mapping, the
  // field-ID projection falls back to physical position and reads the wrong column, so a file that
  // is actually sorted by "b" is wrongly reported as unsorted.
  @Test
  public void testIsSortedNoFieldIdsMovedSortColumn() throws Exception {
    Path file = Files.createTempFile("sorting-no-id-moved", ".parquet");
    Files.deleteIfExists(file);
    // Physical column order is [b, a]: fallback IDs map id 1 -> b, id 2 -> a.
    writeNoIdParquet(
        file,
        List.of("b", "a"),
        List.of(
            Map.of("b", "1", "a", "9"), Map.of("b", "2", "a", "5"), Map.of("b", "3", "a", "1")));

    // Schema declares a=id1, b=id2, so field IDs do NOT match physical positions.
    Schema schema =
        new Schema(
            required(1, "a", Types.StringType.get()), required(2, "b", Types.StringType.get()));
    SortOrder sortByB =
        SortOrder.builderFor(schema).sortBy("b", SortDirection.ASC, NullOrder.NULLS_FIRST).build();

    InputFile in = org.apache.iceberg.Files.localInput(file.toFile());

    // The file is genuinely sorted by "b" (values 1,2,3). Correct name resolution must return true.
    assertThat(Sorting.isSorted(in, schema, sortByB)).isTrue();
  }

  // Control: when physical order matches the field IDs, positional fallback happens to be correct,
  // so this passes both before and after the fix, isolating the mismatch as the cause of failure.
  @Test
  public void testIsSortedNoFieldIdsAlignedSortColumn() throws Exception {
    Path file = Files.createTempFile("sorting-no-id-aligned", ".parquet");
    Files.deleteIfExists(file);
    // Physical column order is [a, b]: fallback IDs map id 1 -> a, id 2 -> b.
    writeNoIdParquet(
        file,
        List.of("a", "b"),
        List.of(
            Map.of("a", "9", "b", "1"), Map.of("a", "5", "b", "2"), Map.of("a", "1", "b", "3")));

    Schema schema =
        new Schema(
            required(1, "a", Types.StringType.get()), required(2, "b", Types.StringType.get()));
    SortOrder sortByB =
        SortOrder.builderFor(schema).sortBy("b", SortDirection.ASC, NullOrder.NULLS_FIRST).build();

    InputFile in = org.apache.iceberg.Files.localInput(file.toFile());

    assertThat(Sorting.isSorted(in, schema, sortByB)).isTrue();
  }

  private static void writeNoIdParquet(
      Path file, List<String> columnOrder, List<Map<String, String>> rows) throws Exception {
    SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fields =
        SchemaBuilder.record("row").namespace("com.altinity.ice.test").fields();
    for (String column : columnOrder) {
      fields = fields.name(column).type().stringType().noDefault();
    }
    org.apache.avro.Schema avroSchema = fields.endRecord();

    try (ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(file))
            .withSchema(avroSchema)
            .withConf(new Configuration())
            .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
            .build()) {
      for (Map<String, String> row : rows) {
        GenericRecord record = new GenericData.Record(avroSchema);
        for (String column : columnOrder) {
          record.put(column, row.get(column));
        }
        writer.write(record);
      }
    }
  }
}
