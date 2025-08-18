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
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
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
}
