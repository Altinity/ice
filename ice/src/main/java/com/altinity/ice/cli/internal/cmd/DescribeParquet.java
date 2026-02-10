/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.cmd;

import com.altinity.ice.cli.internal.iceberg.io.Input;
import com.altinity.ice.cli.internal.iceberg.parquet.Metadata;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import software.amazon.awssdk.services.s3.internal.crossregion.S3CrossRegionSyncClient;
import software.amazon.awssdk.utils.Lazy;
import software.amazon.awssdk.services.s3.S3Client;

public final class DescribeParquet {

  private DescribeParquet() {}

  public enum Option {
    ALL,
    SUMMARY,
    COLUMNS,
    ROW_GROUPS,
    ROW_GROUP_DETAILS
  }

  public static void run(
      RESTCatalog catalog,
      String filePath,
      boolean json,
      boolean s3NoSignRequest,
      Option... options)
      throws IOException {

    Lazy<S3Client> s3ClientLazy =
        new Lazy<>(
            () ->
                new S3CrossRegionSyncClient(
                    com.altinity.ice.cli.internal.s3.S3.newClient(s3NoSignRequest)));
    FileIO io = Input.newIO(filePath, null, s3ClientLazy);
    InputFile inputFile = Input.newFile(filePath, catalog, io);
    run(inputFile, json, options);
  }

  public static void run(InputFile inputFile, boolean json, Option... options) throws IOException {
    ParquetMetadata metadata = Metadata.read(inputFile);

    ParquetInfo info = extractParquetInfo(metadata, options);

    ObjectMapper mapper = json ? new ObjectMapper() : new ObjectMapper(new YAMLFactory());
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    String output = mapper.writeValueAsString(info);
    System.out.println(output);
  }

  private static ParquetInfo extractParquetInfo(ParquetMetadata metadata, Option... options) {
    var optionsSet = java.util.Set.of(options);
    boolean includeAll = optionsSet.contains(Option.ALL);

    FileMetaData fileMetadata = metadata.getFileMetaData();

    // Summary info
    Summary summary = null;
    if (includeAll || optionsSet.contains(Option.SUMMARY)) {
      long totalRows = metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();

      long compressedSize =
          metadata.getBlocks().stream().mapToLong(BlockMetaData::getCompressedSize).sum();

      long uncompressedSize =
          metadata.getBlocks().stream().mapToLong(BlockMetaData::getTotalByteSize).sum();

      summary =
          new Summary(
              totalRows,
              metadata.getBlocks().size(),
              compressedSize,
              uncompressedSize,
              fileMetadata.getCreatedBy(),
              fileMetadata.getSchema().getFieldCount());
    }

    // Column info
    List<Column> columns = null;
    if (includeAll || optionsSet.contains(Option.COLUMNS)) {
      columns = extractColumns(fileMetadata.getSchema());
    }

    // Row group info
    List<RowGroup> rowGroups = null;
    if (includeAll
        || optionsSet.contains(Option.ROW_GROUPS)
        || optionsSet.contains(Option.ROW_GROUP_DETAILS)) {
      boolean includeDetails = includeAll || optionsSet.contains(Option.ROW_GROUP_DETAILS);
      rowGroups = extractRowGroups(metadata.getBlocks(), includeDetails);
    }

    return new ParquetInfo(summary, columns, rowGroups);
  }

  private static List<Column> extractColumns(MessageType schema) {
    List<Column> columns = new ArrayList<>();
    for (Type field : schema.getFields()) {
      String logicalType = null;
      if (field.isPrimitive()) {
        var annotation = field.asPrimitiveType().getLogicalTypeAnnotation();
        logicalType = annotation != null ? annotation.toString() : null;
      }
      columns.add(
          new Column(
              field.getName(),
              field.isPrimitive() ? field.asPrimitiveType().getPrimitiveTypeName().name() : "GROUP",
              field.getRepetition().name(),
              logicalType));
    }
    return columns;
  }

  private static List<RowGroup> extractRowGroups(
      List<BlockMetaData> blocks, boolean includeDetails) {
    List<RowGroup> rowGroups = new ArrayList<>();

    for (int i = 0; i < blocks.size(); i++) {
      BlockMetaData block = blocks.get(i);

      List<ColumnChunk> columnChunks = null;
      if (includeDetails) {
        columnChunks = new ArrayList<>();
        for (ColumnChunkMetaData column : block.getColumns()) {
          Statistics<?> stats = column.getStatistics();

          ColumnStats columnStats = null;
          if (stats != null && !stats.isEmpty()) {
            long nulls = stats.isNumNullsSet() ? stats.getNumNulls() : 0;
            String min = null;
            String max = null;
            if (stats.hasNonNullValue()) {
              Object minVal = stats.genericGetMin();
              Object maxVal = stats.genericGetMax();
              min = minVal != null ? minVal.toString() : null;
              max = maxVal != null ? maxVal.toString() : null;
            }
            columnStats = new ColumnStats(nulls, min, max);
          }

          columnChunks.add(
              new ColumnChunk(
                  column.getPath().toDotString(),
                  column.getPrimitiveType().getName(),
                  column.getEncodings().toString(),
                  column.getCodec().name(),
                  column.getTotalSize(),
                  column.getTotalUncompressedSize(),
                  column.getValueCount(),
                  columnStats));
        }
      }

      rowGroups.add(
          new RowGroup(
              i,
              block.getRowCount(),
              block.getTotalByteSize(),
              block.getCompressedSize(),
              block.getStartingPos(),
              columnChunks));
    }

    return rowGroups;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ParquetInfo(Summary summary, List<Column> columns, List<RowGroup> rowGroups) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record Summary(
      long rows,
      int rowGroups,
      long compressedSize,
      long uncompressedSize,
      String createdBy,
      int columnCount) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record Column(String name, String type, String repetition, String logicalType) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record RowGroup(
      int index,
      long rowCount,
      long totalSize,
      long compressedSize,
      long startingPos,
      List<ColumnChunk> columns) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ColumnChunk(
      String path,
      String type,
      String encodings,
      String codec,
      long totalSize,
      long uncompressedSize,
      long valueCount,
      ColumnStats stats) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ColumnStats(long nulls, String min, String max) {}
}
