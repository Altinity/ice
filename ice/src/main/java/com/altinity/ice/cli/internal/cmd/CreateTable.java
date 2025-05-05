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
import com.altinity.ice.cli.internal.s3.S3;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.parquet.schema.MessageType;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.Lazy;

public final class CreateTable {

  private CreateTable() {}

  public static void run(
      RESTCatalog catalog,
      TableIdentifier nsTable,
      String schemaFile,
      String location,
      boolean ignoreAlreadyExists,
      boolean s3NoSignRequest,
      List<String> partitionColumns,
      List<String> sortAscendingColumns,
      List<String> sortDescendingColumns)
      throws IOException {
    Lazy<S3Client> s3ClientLazy = new Lazy<>(() -> S3.newClient(s3NoSignRequest));

    if (schemaFile.startsWith("s3://") && schemaFile.contains("*")) {
      var b = S3.bucketPath(schemaFile);
      List<String> files = S3.listWildcard(s3ClientLazy.getValue(), b.bucket(), b.path(), 1);
      if (files.isEmpty()) {
        throw new BadRequestException(String.format("No files matching \"%s\" found", schemaFile));
      }
      schemaFile = files.getFirst();
    }
    try (var inputIO = Input.newIO(schemaFile, null, s3ClientLazy)) {
      InputFile inputFile = Input.newFile(schemaFile, catalog, inputIO);
      MessageType type = Metadata.read(inputFile).getFileMetaData().getSchema();
      Schema fileSchema = ParquetSchemaUtil.convert(type);
      try {
        Map<String, String> props = null;
        if (!ParquetSchemaUtil.hasIds(type)) {
          // force name-based resolution instead of position-based resolution
          NameMapping mapping = MappingUtil.create(fileSchema);
          String mappingJson = NameMappingParser.toJson(mapping);
          props = Map.of(TableProperties.DEFAULT_NAME_MAPPING, mappingJson);
        }

        // Create partition spec based on provided partition columns
        final PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(fileSchema);
        if (partitionColumns != null && !partitionColumns.isEmpty()) {
          for (String column : partitionColumns) {
            partitionSpecBuilder.identity(column);
          }
        }
        final PartitionSpec partitionSpec = partitionSpecBuilder.build();

        // Create sort order based on provided sort columns (z-order)
        SortOrder sortOrder = null;
        if ((sortAscendingColumns != null && !sortAscendingColumns.isEmpty())
            || (sortDescendingColumns != null && !sortDescendingColumns.isEmpty())) {
          SortOrder.Builder sortOrderBuilder = SortOrder.builderFor(fileSchema);

          // Add ascending columns first
          if (sortAscendingColumns != null) {
            for (String column : sortAscendingColumns) {
              sortOrderBuilder.asc(column);
            }
          }

          // Add descending columns
          if (sortDescendingColumns != null) {
            for (String column : sortDescendingColumns) {
              sortOrderBuilder.desc(column);
            }
          }

          sortOrder = sortOrderBuilder.build();
        }

        // if we don't set location, it's automatically set to $warehouse/$namespace/$table
        createNamespace(catalog, nsTable.namespace());
        catalog.createTable(nsTable, fileSchema, PartitionSpec.unpartitioned(), location, props);
      } catch (AlreadyExistsException e) {
        if (ignoreAlreadyExists) {
          return;
        }
        throw e;
      }
    }
  }

  private static void createNamespace(RESTCatalog catalog, Namespace ns) {
    if (!catalog.namespaceExists(ns)) {
      String[] levels = ns.levels();
      if (levels.length > 1) {
        Namespace parent = Namespace.of(Arrays.copyOf(levels, levels.length - 1));
        createNamespace(catalog, parent);
      }
      catalog.createNamespace(ns);
    }
  }
}
