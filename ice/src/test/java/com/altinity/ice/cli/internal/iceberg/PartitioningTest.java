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

import static org.assertj.core.api.Assertions.assertThat;

import com.altinity.ice.cli.internal.iceberg.parquet.Metadata;
import com.altinity.ice.test.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

public class PartitioningTest {

  @Test
  public void testInferPartitionKeyResultMatchesScan() throws Exception {
    InputFile inputFile =
        Resource.asInputFile("com/altinity/ice/cli/internal/iceberg/parquet/sample-001.parquet");
    ParquetMetadata metadata = Metadata.read(inputFile);

    MessageType type = metadata.getFileMetaData().getSchema();
    Schema schema = ParquetSchemaUtil.convert(type);

    assertThat(
            partitionOf(inputFile, metadata, PartitionSpec.builderFor(schema).year("t").build())
                .toString())
        .isEqualTo("[49]");
    assertThat(
            partitionOf(inputFile, metadata, PartitionSpec.builderFor(schema).month("t").build())
                .toString())
        .isEqualTo("[588]");
    assertThat(
            partitionOf(inputFile, metadata, PartitionSpec.builderFor(schema).day("t").build())
                .toString())
        .isEqualTo("[17897]");

    assertThat(
            partitionOf(inputFile, metadata, PartitionSpec.builderFor(schema).identity("t").build())
                .toString())
        .isEqualTo("[17897]");
    assertThat(
            partitionOf(
                inputFile, metadata, PartitionSpec.builderFor(schema).identity("seq").build()))
        .isNull();
    assertThat(
            partitionOf(inputFile, metadata, PartitionSpec.builderFor(schema).identity("v").build())
                .toString())
        .isEqualTo("[x]");
  }

  @Test
  public void testInferPartitionKeyTransform() throws Exception {
    InputFile inputFile =
        Resource.asInputFile("com/altinity/ice/cli/internal/iceberg/parquet/sample-002.parquet");
    ParquetMetadata metadata = Metadata.read(inputFile);

    MessageType type = metadata.getFileMetaData().getSchema();
    Schema schema = ParquetSchemaUtil.convert(type);

    assertThat(
            partitionOf(inputFile, metadata, PartitionSpec.builderFor(schema).year("t").build())
                .toString())
        .isEqualTo("[49]");
    assertThat(
            partitionOf(inputFile, metadata, PartitionSpec.builderFor(schema).month("t").build())
                .toString())
        .isEqualTo("[588]");
    assertThat(partitionOf(inputFile, metadata, PartitionSpec.builderFor(schema).day("t").build()))
        .isNull();
  }

  private PartitionKey partitionOf(
      InputFile inputFile, ParquetMetadata metadata, PartitionSpec partitionSpec)
      throws IOException {
    Map<PartitionKey, List<Record>> partition =
        Partitioning.partition(inputFile, partitionSpec.schema(), partitionSpec);
    PartitionKey result = Partitioning.inferPartitionKey(metadata, partitionSpec).partitionKey();
    if (result != null) {
      assertThat(partition.size()).isEqualTo(1);
      PartitionKey expected = partition.keySet().stream().findFirst().get();
      assertThat(result).isEqualTo(expected);
    } else {
      assertThat(partition.size()).isGreaterThan(1);
    }
    return result;
  }
}
