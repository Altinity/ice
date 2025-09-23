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

import com.altinity.ice.internal.iceberg.io.SchemeFileIO;
import com.altinity.ice.internal.io.Matcher;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public record OrphanCleanup(long olderThanMillis, Matcher whitelist, boolean dryRun)
    implements MaintenanceJob {

  private static final Logger logger = LoggerFactory.getLogger(OrphanCleanup.class);

  @Override
  public void perform(Table table) throws IOException {
    String location = table.location();

    logger.info("Searching for orphaned files at {}", location);

    Set<String> orphanedFiles = listOrphanedFiles(table, location, olderThanMillis);

    int excluded = 0;
    Iterator<String> iterator = orphanedFiles.iterator();
    while (iterator.hasNext()) {
      String orphanedFile = iterator.next();
      if (!whitelist.test(orphanedFile)) {
        iterator.remove();
        excluded++;
      }
    }

    logger.info("Found {} orphaned file(s) ({} excluded)", orphanedFiles.size(), excluded);

    if (orphanedFiles.isEmpty()) {
      return;
    }

    if (!dryRun) {
      FileIO tableIO = table.io();

      int numThreads = Math.min(8, orphanedFiles.size());
      try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
        orphanedFiles.forEach(
            file ->
                executor.submit(
                    () -> {
                      try {
                        logger.info("Deleting {}", file);
                        tableIO.deleteFile(file);
                        return file;
                      } catch (Exception e) {
                        logger.warn("Failed to delete file {}", file, e);
                        return null;
                      }
                    }));
      }
    } else {
      orphanedFiles.stream().sorted().forEach(file -> logger.info("To be deleted: {}", file));
    }
  }

  public Set<String> listOrphanedFiles(Table table, String location, long olderThanMillis) {
    FileIO tableIO = table.io();

    SchemeFileIO schemeFileIO;
    if (tableIO instanceof SchemeFileIO) {
      schemeFileIO = (SchemeFileIO) tableIO;
    } else {
      throw new UnsupportedOperationException("SchemeFileIO is required for S3 locations");
    }

    Set<String> allFiles = new HashSet<>();

    var cutOffTimestamp = System.currentTimeMillis() - olderThanMillis;

    Iterable<FileInfo> fileInfos = schemeFileIO.listPrefix(location);
    for (FileInfo fileInfo : fileInfos) {
      if (olderThanMillis == 0 || fileInfo.createdAtMillis() < cutOffTimestamp) {
        allFiles.add(fileInfo.location());
      }
    }

    Set<String> knownFiles = listKnownFiles(table);
    allFiles.removeAll(knownFiles);

    return allFiles;
  }

  private Set<String> listKnownFiles(Table table) {
    Set<String> knownFiles = new HashSet<>();

    FileIO tableIO = table.io();

    Deque<String> metadataFilesToScan = new ArrayDeque<>();
    Set<String> scannedMetadataFiles = new HashSet<>();

    TableMetadata current = ((BaseTable) table).operations().current();
    metadataFilesToScan.add(current.metadataFileLocation());

    while (!metadataFilesToScan.isEmpty()) {
      String metadataFileLocation = metadataFilesToScan.poll();
      if (!scannedMetadataFiles.add(metadataFileLocation)) {
        continue; // already scanned
      }

      knownFiles.add(metadataFileLocation);

      TableMetadata meta = TableMetadataParser.read(tableIO, metadataFileLocation);

      for (Snapshot snapshot : meta.snapshots()) {
        knownFiles.add(snapshot.manifestListLocation());

        List<ManifestFile> manifests;
        try {
          manifests = snapshot.allManifests(tableIO);
        } catch (NotFoundException | NoSuchKeyException e) {
          // deleted
          continue;
        }

        manifests.forEach(
            manifest -> {
              knownFiles.add(manifest.path());
              try (CloseableIterable<DataFile> files = ManifestFiles.read(manifest, tableIO)) {
                for (DataFile file : files) {
                  knownFiles.add(file.location());
                }
              } catch (NotFoundException | NoSuchKeyException e) {
                // deleted
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
      }

      for (StatisticsFile statsFile : meta.statisticsFiles()) {
        knownFiles.add(statsFile.path());
      }

      for (PartitionStatisticsFile psFile : meta.partitionStatisticsFiles()) {
        knownFiles.add(psFile.path());
      }

      for (TableMetadata.MetadataLogEntry prev : meta.previousFiles()) {
        metadataFilesToScan.add(prev.file());
      }
    }
    return knownFiles;
  }
}
