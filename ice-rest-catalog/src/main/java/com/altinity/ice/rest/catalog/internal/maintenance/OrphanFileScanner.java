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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrphanFileScanner {
  private static final Logger LOG = LoggerFactory.getLogger(OrphanFileScanner.class);
  private final Table table;

  public OrphanFileScanner(Table table) {
    this.table = table;
  }

  private Set<String> getAllKnownFiles() {
    Set<String> knownFiles = new HashSet<>();

    for (Snapshot snapshot : table.snapshots()) {
      // Manifest list file
      if (snapshot.manifestListLocation() != null) {
        knownFiles.add(snapshot.manifestListLocation());
      }

      // Manifest files
      FileIO io = table.io();
      for (ManifestFile manifest : snapshot.dataManifests(io)) {
        knownFiles.add(manifest.path());
      }
    }

    return knownFiles;
  }

  public Set<String> findOrphanedFiles(String location, long olderThanMillis) throws IOException {
    Set<String> knownFiles = getAllKnownFiles();

    FileIO fileIO = table.io();
    if (!(fileIO instanceof HadoopFileIO)) {
      throw new IllegalArgumentException("Only HadoopFileIO supported in this implementation");
    }

    Configuration conf = ((HadoopFileIO) fileIO).conf();
    Path rootPath = new Path(location);
    FileSystem fs = rootPath.getFileSystem(conf);

    RemoteIterator<LocatedFileStatus> files = fs.listFiles(rootPath, true);

    long cutoffTime = System.currentTimeMillis() - olderThanMillis;

    Set<String> orphanedFiles = new HashSet<>();

    while (files.hasNext()) {
      LocatedFileStatus status = files.next();
      long modTime = status.getModificationTime();

      if (modTime < cutoffTime) {
        String filePath = status.getPath().toString();
        if (!knownFiles.contains(filePath)) {
          orphanedFiles.add(filePath);
        }
      }
    }

    return orphanedFiles;
  }

  public void removeOrphanedFiles(long olderThanMillis, boolean dryRun) throws IOException {
    String location = table.location();
    Set<String> orphanedFiles = findOrphanedFiles(location, olderThanMillis);

    LOG.info("Found {} orphaned files at {}!", orphanedFiles.size(), location);

    if (orphanedFiles.isEmpty()) {
      LOG.info("No orphaned files found at {}!", location);
      return;
    }

    if (dryRun) {
      LOG.info("(Dry Run) Would delete {} orphaned files at {}!", orphanedFiles.size(), location);
      orphanedFiles.forEach(f -> LOG.info("Orphaned file: {}", f));
    } else {
      ExecutorService executor = Executors.newFixedThreadPool(8);
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
                              LOG.warn("Failed to delete file {}", file, e);
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
          LOG.error("Error during file deletion", e);
        }
      }

      LOG.info("Deleted {} orphaned files at {}!", deletedFiles.size(), location);
      if (!deletedFiles.isEmpty()) {
        deletedFiles.forEach(f -> LOG.info("Deleted: {}", f));
      }

      executor.shutdownNow();
    }
  }
}
