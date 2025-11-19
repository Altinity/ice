/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.maintenance;

import com.altinity.ice.cli.internal.cmd.Insert;
import com.altinity.ice.cli.internal.iceberg.RecordComparator;
import com.altinity.ice.internal.iceberg.io.SchemeFileIO;
import com.altinity.ice.internal.strings.Strings;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: avoid full file scans
public record DataCompaction(
    long targetFileSizeInBytes, int minInputFiles, long olderThanMillis, boolean dryRun)
    implements MaintenanceJob {

  private static final Logger logger = LoggerFactory.getLogger(OrphanCleanup.class);

  public DataCompaction {
    Preconditions.checkArgument(targetFileSizeInBytes > 0);
  }

  @Override
  public void perform(Table table) throws IOException {
    FileIO tableIO = table.io();

    SchemeFileIO schemeFileIO;
    if (tableIO instanceof SchemeFileIO) {
      schemeFileIO = (SchemeFileIO) tableIO;
    } else {
      throw new UnsupportedOperationException("SchemeFileIO is required for S3 locations");
    }

    var cutOffTimestamp = System.currentTimeMillis() - olderThanMillis;

    long sizeSoFar = 0;
    Map<StructLike, List<DataFile>> filesSoFarGroupedByPartition = new HashMap<>();
    // TODO: avoid unnecessary work by skipping already considered partitions
    try (CloseableIterable<FileScanTask> scan = table.newScan().planFiles()) {
      for (FileScanTask planFile : scan) {
        if (planFile.sizeBytes() >= targetFileSizeInBytes) {
          continue;
        }

        if (olderThanMillis > 0) {
          long createdAt = createdAt(schemeFileIO, planFile.file().location());
          if (createdAt > cutOffTimestamp) {
            continue;
          }
        }

        sizeSoFar += planFile.sizeBytes();
        StructLike partition = planFile.partition();
        List<DataFile> filesSoFar =
            filesSoFarGroupedByPartition.computeIfAbsent(
                partition, structLike -> new ArrayList<>(5));
        filesSoFar.add(planFile.file());

        // TODO: support custom strategies; greedy is not optimal
        // TODO: planFile.deletes().getFirst()

        if (sizeSoFar > targetFileSizeInBytes) {
          merge(table, filesSoFar, table.sortOrder(), partition, dryRun);
          sizeSoFar = 0;
          filesSoFar.clear();
        }
      }
    }

    filesSoFarGroupedByPartition.forEach(
        (partition, dataFiles) -> {
          if (dataFiles.size() >= minInputFiles) {
            try {
              merge(table, dataFiles, table.sortOrder(), partition, dryRun);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        });
  }

  private static long createdAt(SchemeFileIO tableIO, String location) {
    Iterator<FileInfo> iter = tableIO.listPrefix(location).iterator();
    if (!iter.hasNext()) {
      throw new NotFoundException(location);
    }
    FileInfo next = iter.next();
    if (!location.equals(next.location())) {
      // FIXME
      throw new IllegalStateException("listPrefix(" + location + ") matched " + next.location());
    }
    return next.createdAtMillis();
  }

  private void merge(
      Table table,
      List<DataFile> dataFiles,
      SortOrder sortOrder,
      StructLike partition,
      boolean dryRun)
      throws IOException {

    FileIO tableIO = table.io();
    Schema tableSchema = table.schema();
    PartitionSpec tableSpec = table.spec();

    Transaction tx = table.newTransaction();

    String dstPath = Insert.DataFileNamingStrategy.defaultDataLocation(table);
    String dstDataFile =
        new Insert.DataFileNamingStrategy.Default(dstPath, System.currentTimeMillis() + "-")
            .get("comp");

    logger.info(
        "Combining {} into {}",
        dataFiles.stream().map(ContentFile::location).toList(),
        dstDataFile);

    if (dryRun) {
      return;
    }

    OutputFile outputFile =
        tableIO.newOutputFile(Strings.replacePrefix(dstDataFile, "s3://", "s3a://"));

    MetricsConfig metricsConfig = MetricsConfig.forTable(table);

    Parquet.WriteBuilder writeBuilder =
        Parquet.write(outputFile)
            .overwrite(false)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .metricsConfig(metricsConfig)
            .schema(tableSchema);

    long dataFileSizeInBytes;
    Metrics metrics;

    if (sortOrder.isSorted()) {
      // k-way merge sort

      List<CloseableIterable<Record>> inputs = new ArrayList<>(dataFiles.size());
      for (DataFile dataFile : dataFiles) {
        Parquet.ReadBuilder rb =
            Parquet.read(tableIO.newInputFile(dataFile.location()))
                .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
                .project(tableSchema)
                .reuseContainers();
        inputs.add(rb.build());
      }
      List<PeekingIterator<Record>> iterators =
          inputs.stream().map(CloseableIterable::iterator).map(Iterators::peekingIterator).toList();

      PriorityQueue<PeekingIterator<Record>> heap =
          new PriorityQueue<>(
              Comparator.comparing(
                  PeekingIterator::peek, new RecordComparator(sortOrder, tableSchema)));

      for (PeekingIterator<Record> it : iterators) {
        if (it.hasNext()) heap.add(it);
      }

      try (FileAppender<Record> writer = writeBuilder.build()) {
        while (!heap.isEmpty()) {
          PeekingIterator<Record> it = heap.poll();
          Record next = it.next();
          writer.add(next);
          if (it.hasNext()) heap.add(it);
        }
        writer.close();

        dataFileSizeInBytes = writer.length();
        metrics = writer.metrics();
      }
    } else {
      try (FileAppender<Record> writer = writeBuilder.build()) {
        for (DataFile dataFile : dataFiles) {
          Parquet.ReadBuilder rb =
              Parquet.read(tableIO.newInputFile(dataFile.location()))
                  .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
                  .project(tableSchema)
                  .reuseContainers();

          try (CloseableIterable<org.apache.iceberg.data.Record> r = rb.build()) {
            writer.addAll(r);
          }
        }
        writer.close();

        dataFileSizeInBytes = writer.length();
        metrics = writer.metrics();
      }
    }

    AppendFiles appendOp = tx.newAppend();

    DataFile dataFileObj =
        new DataFiles.Builder(tableSpec)
            .withPath(outputFile.location())
            .withFormat("PARQUET")
            .withFileSizeInBytes(dataFileSizeInBytes)
            .withMetrics(metrics)
            .withPartition(partition)
            .build();
    appendOp.appendFile(dataFileObj);
    appendOp.commit();

    DeleteFiles delOp = tx.newDelete();
    dataFiles.forEach(delOp::deleteFile);
    delOp.commit();

    tx.commitTransaction();
  }
}
