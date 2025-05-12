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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DeleteFile {

  private static final Logger logger = LoggerFactory.getLogger(DeleteFile.class);

  private DeleteFile() {}

  public static void run(
      RESTCatalog catalog, String file, int pos, String namespace, String tableName)
      throws IOException, URISyntaxException {

    // if file is empty,
    if (file == null || file.isEmpty()) {
      throw new IllegalArgumentException("File is empty");
    }

    // Parse the S3 URL
    URI fileUri = new URI(file);
    String scheme = fileUri.getScheme();
    if (!"s3".equals(scheme)) {
      throw new IllegalArgumentException("Only s3:// URLs are supported");
    }

    // Extract bucket and key
    String bucket = fileUri.getHost();
    String key = fileUri.getPath().substring(1); // Remove leading slash

    // Create delete file in the same S3 location
    String deleteKey =
        key.substring(0, key.lastIndexOf('/') + 1)
            + "delete-"
            + System.currentTimeMillis()
            + ".parquet";
    String deleteFileLocation = String.format("s3://%s/%s", bucket, deleteKey);

    // Load the table
    TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
    Table table = catalog.loadTable(tableId);

    // Create the output file
    OutputFile deleteOutput = table.io().newOutputFile(deleteFileLocation);

    // Get the data file info
    var dataFile = table.newScan().planFiles().iterator().next().file();

    org.apache.iceberg.DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath(deleteOutput.location())
            .withPartition(dataFile.partition())
            .withReferencedDataFile(dataFile.location())
            .withFileSizeInBytes(deleteOutput.toInputFile().getLength())
            .withRecordCount(1)
            .build();

    table.newRowDelta().addDeletes(deleteFile).commit();

    logger.info("Position delete committed for file {} at position {}", file, pos);
  }
}
