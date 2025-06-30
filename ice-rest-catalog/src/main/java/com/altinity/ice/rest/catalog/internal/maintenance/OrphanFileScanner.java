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

import com.altinity.ice.cli.internal.s3.S3;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

public class OrphanFileScanner {
  private static final Logger logger = LoggerFactory.getLogger(OrphanFileScanner.class);
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
        // Add data files inside each manifest
        try (CloseableIterable<DataFile> files = ManifestFiles.read(manifest, table.io())) {
          for (DataFile dataFile : files) {
            knownFiles.add(dataFile.path().toString());
          }
        } catch (Exception e) {
          logger.error("Error getting list of data files", e);
        }
      }
    }

    return knownFiles;
  }

  public Set<String> findOrphanedFiles(String location, long olderThanMillis) throws IOException {
    Set<String> knownFiles = getAllKnownFiles();

    String bucket = location.replace("s3://", "").split("/")[0];
    String prefix = location.replace("s3://" + bucket + "/", "");

    S3Client s3 = S3.newClient(true);

    ListObjectsV2Request listRequest =
        ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build();

    Set<String> allFiles = new HashSet<>();

    ListObjectsV2Response listResponse;
    do {
      listResponse = s3.listObjectsV2(listRequest);
      listResponse.contents().forEach(obj -> allFiles.add("s3://" + bucket + "/" + obj.key()));
      listRequest =
          listRequest.toBuilder().continuationToken(listResponse.nextContinuationToken()).build();
    } while (listResponse.isTruncated());

    // Set<String> orphanedFiles = new HashSet<>();

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
        }
      }

      logger.info("Deleted {} orphaned files at {}!", deletedFiles.size(), location);
      if (!deletedFiles.isEmpty()) {
        deletedFiles.forEach(f -> logger.info("Deleted: {}", f));
      }
    }
  }
}
