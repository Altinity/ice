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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrphanFileScanner {
  private static final Logger logger = LoggerFactory.getLogger(OrphanFileScanner.class);
  private final Table table;

  public OrphanFileScanner(Table table) {
    this.table = table;
  }

  private Set<String> getAllKnownFiles() throws IOException {
    Set<String> knownFiles = new HashSet<>();

    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.manifestListLocation() != null) {
        knownFiles.add(snapshot.manifestListLocation());
      }

      FileIO io = table.io();

      TableOperations ops = ((BaseTable) table).operations();
      TableMetadata meta = ops.current();

      String currentMetadataFile = meta.metadataFileLocation();
      // Current metadata json file
      knownFiles.add(currentMetadataFile);

      // All the previous metadata JSON(is there a chance there might be
      // json files that are not physically present?.
      for (TableMetadata.MetadataLogEntry previousFile : meta.previousFiles()) {
        knownFiles.add(previousFile.file());
      }

      for (ManifestFile manifest : snapshot.dataManifests(io)) {
        knownFiles.add(manifest.path());

        // Add data files inside each manifest
        try (CloseableIterable<DataFile> files = ManifestFiles.read(manifest, table.io())) {
          for (DataFile dataFile : files) {
            knownFiles.add(dataFile.path().toString());
          }
        } catch (Exception e) {
          throw e;
        }
      }
    }

    return knownFiles;
  }

  public Set<String> findOrphanedFiles(String location, long olderThanMillis) throws IOException {
    Set<String> knownFiles = getAllKnownFiles();

    FileIO tableIO = table.io();

    SchemeFileIO schemeFileIO;
    if (tableIO instanceof SchemeFileIO) {
      schemeFileIO = (SchemeFileIO) tableIO;
    } else {
      throw new UnsupportedOperationException("SchemeFileIO is required for S3 locations");
    }

    Set<String> allFiles = new HashSet<>();

    Iterable<FileInfo> fileInfos = schemeFileIO.listPrefix(location);
    for (FileInfo fileInfo : fileInfos) {
      if (fileInfo.createdAtMillis() > olderThanMillis) {
        allFiles.add(fileInfo.location());
      }
    }

    allFiles.removeAll(knownFiles);

    return allFiles;
  }

  public void removeOrphanedFiles(long olderThanMillis, boolean dryRun) throws IOException {
    String location = table.location();
    logger.info("Looking for Orphaned files in location {}", location);
    Set<String> orphanedFiles = findOrphanedFiles(location, olderThanMillis);

    logger.info("Found {} orphaned files at {}!", orphanedFiles.size(), location);

    if (orphanedFiles.isEmpty()) {
      logger.info("No orphaned files found at {}!", location);
      return;
    }

    if (dryRun) {
      logger.info(
          "(Dry Run) Would delete {} orphaned files at {}!", orphanedFiles.size(), location);
      orphanedFiles.forEach(f -> logger.info("Orphaned file: {}", f));
    } else {

      int numThreads = Math.min(8, orphanedFiles.size());

      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      List<Future<String>> futures =
          orphanedFiles.stream()
              .map(
                  file ->
                      executor.submit(
                          () -> {
                            try {
                              table.io().deleteFile(file);
                              return file;
                            } catch (Exception e) {
                              logger.warn("Failed to delete file {}", file, e);
                              return null;
                            }
                          }))
              .collect(Collectors.toList());

      executor.shutdown();

      List<String> deletedFiles = new ArrayList<>();
      for (Future<String> future : futures) {
        try {
          String result = future.get();
          if (result != null) {
            deletedFiles.add(result);
          }
        } catch (Exception e) {
          logger.error("Error during file deletion", e);
          return;
        }
      }

      logger.info("Deleted {} orphaned files at {}!", deletedFiles.size(), location);
      if (!deletedFiles.isEmpty()) {
        deletedFiles.forEach(f -> logger.info("Deleted: {}", f));
      }
    }
  }
}
