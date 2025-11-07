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
import com.altinity.ice.cli.internal.iceberg.aws.s3.VendedCredentialsProvider;
import com.altinity.ice.cli.internal.iceberg.io.Input;
import com.altinity.ice.cli.internal.iceberg.parquet.MessageTypeToSchema;
import com.altinity.ice.cli.internal.iceberg.parquet.Metadata;
import com.altinity.ice.cli.internal.s3.S3;
import com.altinity.ice.internal.strings.Strings;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
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
import software.amazon.awssdk.regions.Region;
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
      boolean useVendedCredentials,
      boolean s3NoSignRequest,
      @Nullable List<Main.IcePartition> partitionList,
      @Nullable List<Main.IceSortOrder> sortOrderList)
      throws IOException {
    if (ignoreAlreadyExists && catalog.tableExists(nsTable)) {
      return;
    }

    Lazy<S3Client> s3ClientLazy = newS3Client(catalog, useVendedCredentials, s3NoSignRequest);

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
      Schema initialSchema = MessageTypeToSchema.convertInitial(type);

      if (ParquetSchemaUtil.hasIds(type)
          && !initialSchema.sameSchema(MessageTypeToSchema.convert(type))) {
        // https://github.com/apache/iceberg/blob/16fa673782233eb2b692d463deb8be3e52c5f378/core/src/main/java/org/apache/iceberg/TableMetadata.java#L117-L120
        throw new BadRequestException(
            "parquet file contains field ids that cannot be transferred over to a new table");
      }

      try {
        // force name-based resolution instead of position-based resolution
        NameMapping mapping = MappingUtil.create(initialSchema);
        String mappingJson = NameMappingParser.toJson(mapping);
        var props = Map.of(TableProperties.DEFAULT_NAME_MAPPING, mappingJson);

        PartitionSpec partitionSpec =
            partitionList == null
                ? PartitionSpec.unpartitioned()
                : Partitioning.newPartitionSpec(initialSchema, partitionList);

        if (ignoreAlreadyExists) { // -p
          createNamespace(catalog, nsTable.namespace());
        }

        // if we don't set location, it's automatically set to $warehouse/$namespace/$table
        Transaction tx =
            catalog.newCreateTableTransaction(
                nsTable, initialSchema, partitionSpec, location, props);

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
    } finally {
      if (s3ClientLazy.hasValue()) {
        s3ClientLazy.getValue().close();
      }
    }
  }

  private static Lazy<S3Client> newS3Client(
      RESTCatalog catalog, boolean useVendedCredentials, boolean s3NoSignRequest) {
    final Supplier<S3Client> s3ClientSupplier;
    if (useVendedCredentials) {
      s3ClientSupplier =
          () -> {
            Map<String, String> catalogProps = catalog.properties();
            String uri = catalog.properties().get(CatalogProperties.URI);
            String credentialsURI =
                Strings.removeSuffix(Strings.removeSuffixAll(uri, "/"), "/v1") + "/v1/credentials";
            var b =
                S3Client.builder()
                    .credentialsProvider(
                        VendedCredentialsProvider.create(
                            Map.of(VendedCredentialsProvider.URI, credentialsURI)));
            String prefix = "ice.io.default.";
            String endpoint = catalogProps.get(prefix + S3FileIOProperties.ENDPOINT);
            if (!Strings.isNullOrEmpty(endpoint)) {
              b.endpointOverride(URI.create(endpoint));
            }
            if ("true".equals(catalogProps.get(prefix + S3FileIOProperties.PATH_STYLE_ACCESS))) {
              b.forcePathStyle(true);
            }
            String region = catalogProps.get(prefix + AwsClientProperties.CLIENT_REGION);
            if (!Strings.isNullOrEmpty(region)) {
              b.region(Region.of(region));
            }
            return b.build();
          };
    } else {
      s3ClientSupplier = () -> S3.newClient(s3NoSignRequest);
    }
    return new Lazy<>(() -> new S3CrossRegionSyncClient(s3ClientSupplier.get()));
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
