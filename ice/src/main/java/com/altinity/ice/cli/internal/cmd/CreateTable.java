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

import com.altinity.ice.cli.Main;
import com.altinity.ice.cli.internal.iceberg.Partitioning;
import com.altinity.ice.cli.internal.iceberg.Sorting;
import com.altinity.ice.cli.internal.iceberg.io.Input;
import com.altinity.ice.cli.internal.iceberg.parquet.Metadata;
import com.altinity.ice.cli.internal.s3.S3;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.internal.crossregion.S3CrossRegionSyncClient;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
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
      @Nullable List<Main.IcePartition> partitionList,
      @Nullable List<Main.IceSortOrder> sortOrderList)
      throws IOException {
    if (ignoreAlreadyExists && catalog.tableExists(nsTable)) {
      return;
    }

    Lazy<S3Client> s3ClientLazy =
        new Lazy<>(() -> new S3CrossRegionSyncClient(S3.newClient(s3NoSignRequest)));

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

      ParquetMetadata metadata;
      try {
        metadata = Metadata.read(inputFile);
      } catch (NoSuchKeyException e) { // S3FileInput
        throw new NotFoundException(inputFile.location(), e);
      } // rethrow NotFoundException

      MessageType type = metadata.getFileMetaData().getSchema();
      Schema fileSchema = ParquetSchemaUtil.convert(type);
      try {
        Map<String, String> props = null;
        if (!ParquetSchemaUtil.hasIds(type)) {
          // force name-based resolution instead of position-based resolution
          NameMapping mapping = MappingUtil.create(fileSchema);
          String mappingJson = NameMappingParser.toJson(mapping);
          props = Map.of(TableProperties.DEFAULT_NAME_MAPPING, mappingJson);
        }

        PartitionSpec partitionSpec =
            partitionList == null
                ? PartitionSpec.unpartitioned()
                : Partitioning.newPartitionSpec(fileSchema, partitionList);

        if (ignoreAlreadyExists) { // -p
          createNamespace(catalog, nsTable.namespace());
        }

        // if we don't set location, it's automatically set to $warehouse/$namespace/$table
        Transaction tx =
            catalog.newCreateTableTransaction(nsTable, fileSchema, partitionSpec, location, props);

        if (sortOrderList != null && !sortOrderList.isEmpty()) {
          ReplaceSortOrder op = tx.replaceSortOrder();
          Sorting.apply(op, sortOrderList);
          op.commit();
        }

        tx.commitTransaction();
      } catch (AlreadyExistsException e) {
        if (!ignoreAlreadyExists) {
          throw e;
        }
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
