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

import com.altinity.ice.internal.iceberg.io.SchemeFileIO;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.*;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DescribeMetadata {

  private static final Logger logger = LoggerFactory.getLogger(DescribeMetadata.class);

  private DescribeMetadata() {}

  public enum Option {
    ALL,
    SUMMARY,
    SCHEMA,
    SNAPSHOTS,
    HISTORY,
    MANIFESTS
  }

  public static void run(
      Map<String, String> icebergConfig, String filePath, boolean json, Option... options)
      throws IOException {
    try (SchemeFileIO io = new SchemeFileIO()) {
      io.initialize(icebergConfig);
      TableMetadata metadata = TableMetadataParser.read(io, filePath);
      MetadataInfo info = extractMetadataInfo(metadata, io, options);

      ObjectMapper mapper = json ? new ObjectMapper() : new ObjectMapper(new YAMLFactory());
      mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      String output = mapper.writeValueAsString(info);
      System.out.println(output);
    }
  }

  private static MetadataInfo extractMetadataInfo(
      TableMetadata metadata, FileIO io, Option... options) {
    var optionsSet = Set.of(options);
    boolean includeAll = optionsSet.contains(Option.ALL);

    Summary summary = null;
    if (includeAll || optionsSet.contains(Option.SUMMARY)) {
      summary = extractSummary(metadata);
    }

    SchemaInfo schema = null;
    if (includeAll || optionsSet.contains(Option.SCHEMA)) {
      schema = extractSchema(metadata);
    }

    List<SnapshotInfo> snapshots = null;
    if (includeAll || optionsSet.contains(Option.SNAPSHOTS)) {
      snapshots = extractSnapshots(metadata);
    }

    HistoryInfo history = null;
    if (includeAll || optionsSet.contains(Option.HISTORY)) {
      history = extractHistory(metadata);
    }

    List<ManifestInfo> manifests = null;
    if (includeAll || optionsSet.contains(Option.MANIFESTS)) {
      manifests = extractManifests(metadata, metadata.schema(), io);
    }

    return new MetadataInfo(summary, schema, snapshots, history, manifests);
  }

  private static Summary extractSummary(TableMetadata metadata) {
    Snapshot currentSnapshot = metadata.currentSnapshot();
    Long currentSnapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : null;
    String partitionSpec =
        metadata.spec().isUnpartitioned() ? "unpartitioned" : metadata.spec().toString();
    String sortOrder =
        metadata.sortOrder().isUnsorted() ? "unsorted" : metadata.sortOrder().toString();
    Map<String, String> properties = metadata.properties().isEmpty() ? null : metadata.properties();

    return new Summary(
        metadata.uuid(),
        metadata.formatVersion(),
        metadata.location(),
        metadata.lastUpdatedMillis(),
        Instant.ofEpochMilli(metadata.lastUpdatedMillis()).toString(),
        currentSnapshotId,
        metadata.snapshots().size(),
        partitionSpec,
        sortOrder,
        properties);
  }

  private static SchemaInfo extractSchema(TableMetadata metadata) {
    Schema schema = metadata.schema();
    List<FieldInfo> fields = new ArrayList<>();
    for (Types.NestedField field : schema.columns()) {
      fields.add(
          new FieldInfo(
              field.fieldId(), field.name(), field.type().toString(), field.isRequired()));
    }
    return new SchemaInfo(schema.schemaId(), fields);
  }

  private static List<SnapshotInfo> extractSnapshots(TableMetadata metadata) {
    List<SnapshotInfo> snapshots = new ArrayList<>();
    for (Snapshot snapshot : metadata.snapshots()) {
      snapshots.add(
          new SnapshotInfo(
              snapshot.snapshotId(),
              snapshot.parentId(),
              snapshot.sequenceNumber(),
              snapshot.timestampMillis(),
              Instant.ofEpochMilli(snapshot.timestampMillis()).toString(),
              snapshot.operation(),
              snapshot.summary(),
              snapshot.manifestListLocation()));
    }
    return snapshots;
  }

  private static HistoryInfo extractHistory(TableMetadata metadata) {
    List<SnapshotLogEntry> snapshotLog = new ArrayList<>();
    for (var entry : metadata.snapshotLog()) {
      snapshotLog.add(
          new SnapshotLogEntry(
              entry.snapshotId(),
              entry.timestampMillis(),
              Instant.ofEpochMilli(entry.timestampMillis()).toString()));
    }

    List<MetadataLogEntry> metadataLog = new ArrayList<>();
    for (TableMetadata.MetadataLogEntry entry : metadata.previousFiles()) {
      metadataLog.add(
          new MetadataLogEntry(
              entry.file(),
              entry.timestampMillis(),
              Instant.ofEpochMilli(entry.timestampMillis()).toString()));
    }

    return new HistoryInfo(
        snapshotLog.isEmpty() ? null : snapshotLog, metadataLog.isEmpty() ? null : metadataLog);
  }

  private static List<ManifestInfo> extractManifests(
      TableMetadata metadata, org.apache.iceberg.Schema schema, FileIO io) {
    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (currentSnapshot == null) {
      return List.of();
    }

    List<ManifestFile> manifestFiles;
    try {
      manifestFiles = currentSnapshot.allManifests(io);
    } catch (Exception e) {
      logger.warn("Failed to read manifest list: {}", e.getMessage());
      return List.of();
    }

    List<ManifestInfo> manifests = new ArrayList<>();
    for (ManifestFile manifest : manifestFiles) {
      List<DataFileInfo> dataFiles = new ArrayList<>();
      try (CloseableIterable<DataFile> files = ManifestFiles.read(manifest, io)) {
        for (DataFile file : files) {
          dataFiles.add(
              new DataFileInfo(
                  file.location(),
                  file.format().name(),
                  file.recordCount(),
                  file.fileSizeInBytes(),
                  file.partition().toString(),
                  file.columnSizes(),
                  file.valueCounts(),
                  file.nullValueCounts(),
                  convertBounds(schema, file.lowerBounds()),
                  convertBounds(schema, file.upperBounds())));
        }
      } catch (Exception e) {
        logger.warn("Failed to read manifest {}: {}", manifest.path(), e.getMessage());
      }

      manifests.add(
          new ManifestInfo(
              manifest.path(),
              manifest.addedFilesCount(),
              manifest.existingFilesCount(),
              manifest.deletedFilesCount(),
              manifest.partitionSpecId(),
              dataFiles.isEmpty() ? null : dataFiles));
    }

    return manifests;
  }

  private static Map<Integer, String> convertBounds(
      org.apache.iceberg.Schema schema, Map<Integer, ByteBuffer> bounds) {
    if (bounds == null) {
      return null;
    }
    Map<Integer, String> result = new HashMap<>();
    for (var entry : bounds.entrySet()) {
      Types.NestedField field = schema.findField(entry.getKey());
      if (field != null) {
        result.put(
            entry.getKey(), Conversions.fromByteBuffer(field.type(), entry.getValue()).toString());
      }
    }
    return result.isEmpty() ? null : result;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record MetadataInfo(
      Summary summary,
      SchemaInfo schema,
      List<SnapshotInfo> snapshots,
      HistoryInfo history,
      List<ManifestInfo> manifests) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record Summary(
      String uuid,
      int formatVersion,
      String location,
      long lastUpdatedMillis,
      String lastUpdated,
      Long currentSnapshotId,
      int numSnapshots,
      String partitionSpec,
      String sortOrder,
      Map<String, String> properties) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record SchemaInfo(int schemaId, List<FieldInfo> fields) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record FieldInfo(int fieldId, String name, String type, boolean required) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record SnapshotInfo(
      long snapshotId,
      Long parentId,
      long sequenceNumber,
      long timestampMillis,
      String timestamp,
      String operation,
      Map<String, String> summary,
      String manifestListLocation) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record HistoryInfo(
      List<SnapshotLogEntry> snapshotLog, List<MetadataLogEntry> metadataLog) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record SnapshotLogEntry(long snapshotId, long timestampMillis, String timestamp) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record MetadataLogEntry(String file, long timestampMillis, String timestamp) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ManifestInfo(
      String path,
      Integer addedFilesCount,
      Integer existingFilesCount,
      Integer deletedFilesCount,
      int partitionSpecId,
      List<DataFileInfo> dataFiles) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record DataFileInfo(
      String path,
      String format,
      long recordCount,
      long fileSizeInBytes,
      String partition,
      Map<Integer, Long> columnSizes,
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullValueCounts,
      Map<Integer, String> lowerBounds,
      Map<Integer, String> upperBounds) {}
}
