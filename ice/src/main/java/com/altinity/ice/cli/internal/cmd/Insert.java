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
import com.altinity.ice.cli.internal.iceberg.RecordComparator;
import com.altinity.ice.cli.internal.iceberg.Sorting;
import com.altinity.ice.cli.internal.iceberg.io.Input;
import com.altinity.ice.cli.internal.iceberg.parquet.Metadata;
import com.altinity.ice.cli.internal.jvm.Stats;
import com.altinity.ice.cli.internal.retry.RetryLog;
import com.altinity.ice.cli.internal.s3.S3;
import com.altinity.ice.internal.strings.Strings;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.utils.Lazy;

public final class Insert {

  private static final Logger logger = LoggerFactory.getLogger(Insert.class);

  private Insert() {}

  // TODO: refactor
  public static void run(
      RESTCatalog catalog,
      TableIdentifier nsTable,
      String[] files,
      DataFileNamingStrategy.Name dataFileNamingStrategy,
      boolean skipDuplicates,
      boolean noCommit,
      boolean noCopy,
      boolean forceNoCopy,
      boolean forceTableAuth,
      boolean s3NoSignRequest,
      boolean s3CopyObject,
      @Nullable String retryListFile,
      @Nullable List<Main.IcePartition> partitionList,
      @Nullable List<Main.IceSortOrder> sortOrderList,
      int threadCount)
      throws IOException, InterruptedException {
    if (files.length == 0) {
      // no work to be done
      return;
    }
    Options options =
        Options.builder()
            .skipDuplicates(skipDuplicates)
            .noCommit(noCommit)
            .noCopy(noCopy)
            .forceNoCopy(forceNoCopy)
            .forceTableAuth(forceTableAuth)
            .s3NoSignRequest(s3NoSignRequest)
            .s3CopyObject(s3CopyObject)
            .threadCount(threadCount)
            .build();

    // FIXME: refactor: move to builder
    final Options finalOptions =
        options.forceNoCopy() ? options.toBuilder().noCopy(true).build() : options;
    Table table = catalog.loadTable(nsTable);

    // Create transaction and pass it to updatePartitionAndSortOrderMetadata
    Transaction txn = table.newTransaction();

    try (FileIO tableIO = table.io()) {
      final Supplier<S3Client> s3ClientSupplier;
      if (finalOptions.forceTableAuth()) {
        if (!(tableIO instanceof S3FileIO)) {
          throw new UnsupportedOperationException(
              "--force-table-auth is currently only supported for s3:// tables");
        }
        s3ClientSupplier = ((S3FileIO) tableIO)::client;
      } else {
        s3ClientSupplier = () -> S3.newClient(finalOptions.s3NoSignRequest());
      }
      Lazy<S3Client> s3ClientLazy = new Lazy<>(s3ClientSupplier);
      try {
        var filesExpanded =
            Arrays.stream(files)
                .flatMap(
                    s -> {
                      if (s.startsWith("s3://") && s.contains("*")) {
                        var b = S3.bucketPath(s);
                        return S3
                            .listWildcard(s3ClientLazy.getValue(), b.bucket(), b.path(), -1)
                            .stream();
                      }
                      return Stream.of(s);
                    })
                .toList();
        if (filesExpanded.isEmpty()) {
          throw new BadRequestException("No matching files found");
        }
        if (filesExpanded.size() != new HashSet<>(filesExpanded).size()) {
          throw new BadRequestException("Input contains duplicates");
        }

        Schema tableSchema = table.schema();

        Set<String> tableDataFiles;
        try (var plan = table.newScan().planFiles()) {
          tableDataFiles =
              StreamSupport.stream(plan.spliterator(), false)
                  .map(f -> f.file().location())
                  .collect(Collectors.toUnmodifiableSet());
        }

        var tableEmpty = tableDataFiles.isEmpty();
        // TODO: move to update-table
        var tablePartitionSpec =
            syncTablePartitionSpec(txn, table, tableSchema, tableEmpty, partitionList);
        var tableSortOrder = syncTableSortOrder(txn, table, tableSchema, tableEmpty, sortOrderList);
        if (tablePartitionSpec.isPartitioned() || tableSortOrder.isSorted()) {
          updateWriteDistributionModeIfNotSet(txn, table);
        }

        String dstPath = DataFileNamingStrategy.defaultDataLocation(table);
        DataFileNamingStrategy dstDataFileSource =
            switch (dataFileNamingStrategy) {
              case DEFAULT ->
                  new DataFileNamingStrategy.Default(dstPath, System.currentTimeMillis() + "-");
              case PRESERVE_ORIGINAL -> new DataFileNamingStrategy.PreserveOriginal(dstPath);
            };

        // appendOp to use the same transaction.
        AppendFiles appendOp = txn.newAppend();

        try (FileIO inputIO = Input.newIO(filesExpanded.getFirst(), table, s3ClientLazy);
            RetryLog retryLog =
                retryListFile != null && !retryListFile.isEmpty()
                    ? new RetryLog(retryListFile)
                    : null) {
          boolean atLeastOneFileAppended = false;

          int numThreads = Math.min(finalOptions.threadCount(), filesExpanded.size());
          ExecutorService executor = Executors.newFixedThreadPool(numThreads);
          try {
            var futures = new ArrayList<Future<List<DataFile>>>();
            for (final String file : filesExpanded) {
              // Make a copy of as BaseTable returned by table.schema() is not thread-safe.
              var tableSchemaCopy =
                  new Schema(
                      tableSchema.schemaId(),
                      tableSchema.columns(),
                      tableSchema.getAliases(),
                      tableSchema.identifierFieldIds());
              futures.add(
                  executor.submit(
                      () -> {
                        try {
                          return processFile(
                              catalog,
                              table,
                              tablePartitionSpec,
                              tableSortOrder,
                              tableIO,
                              inputIO,
                              tableDataFiles,
                              finalOptions,
                              s3ClientLazy,
                              dstDataFileSource,
                              tableSchemaCopy,
                              dataFileNamingStrategy,
                              file);
                        } catch (Exception e) {
                          if (retryLog != null) {
                            logger.error(
                                "{}: error (adding to retry list and continuing)", file, e);
                            retryLog.add(file);
                            return Collections.emptyList();
                          } else {
                            throw new IOException(String.format("Error processing %s", file), e);
                          }
                        }
                      }));
            }

            for (var future : futures) {
              try {
                List<DataFile> dataFiles = future.get();
                for (DataFile df : dataFiles) {
                  atLeastOneFileAppended = true;
                  appendOp.appendFile(df); // Only main thread appends now
                }
              } catch (ExecutionException e) {
                if (retryLog == null) {
                  throw new IOException("Error processing file(s)", e.getCause());
                }
              }
            }
          } finally {
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.MINUTES);
          }

          if (!finalOptions.noCommit()) {
            // TODO: log
            if (atLeastOneFileAppended) {
              appendOp.commit();
            } else {
              logger.warn("Table commit skipped (no files to append)");
            }
            if (retryLog != null) {
              retryLog.commit();
            }

            // Commit transaction.
            txn.commitTransaction();
          } else {
            logger.warn("Table commit skipped (--no-commit)");
          }
        }
      } finally {
        if (s3ClientLazy.hasValue()) {
          s3ClientLazy.getValue().close();
        }
      }
    }
  }

  private static PartitionSpec syncTablePartitionSpec(
      Transaction txn,
      Table table,
      Schema tableSchema,
      boolean tableEmpty,
      @Nullable List<Main.IcePartition> partitionList) {
    if (partitionList != null) {
      PartitionSpec partitionSpec = Partitioning.newPartitionSpec(tableSchema, partitionList);
      if (!table.spec().compatibleWith(partitionSpec)) {
        if (!tableEmpty) {
          throw new UnsupportedOperationException(
              "updates to partitioning spec are currently not supported");
        }
        if (!partitionList.isEmpty()) {
          var updateSpec = txn.updateSpec();
          Partitioning.apply(updateSpec, partitionList);
          var r = updateSpec.apply();
          logger.info("Updating partitioning spec to {}", r.toString());
          updateSpec.commit();
          return r;
        }
      }
    }
    return table.spec();
  }

  private static SortOrder syncTableSortOrder(
      Transaction txn,
      Table table,
      Schema tableSchema,
      boolean tableEmpty,
      @Nullable List<Main.IceSortOrder> sortOrderList) {
    if (sortOrderList != null) {
      SortOrder sortOrder = Sorting.newSortOrder(tableSchema, sortOrderList);
      if (!table.sortOrder().sameOrder(sortOrder)) {
        if (!tableEmpty) {
          throw new UnsupportedOperationException(
              "updates to sort order spec are currently not supported");
        }
        if (!sortOrderList.isEmpty()) {
          ReplaceSortOrder op = txn.replaceSortOrder();
          Sorting.apply(op, sortOrderList);
          var r = op.apply();
          op.commit();
          logger.info("Updating ordering spec to {}", r.toString());
          return r;
        }
      }
    }
    return table.sortOrder();
  }

  private static void updateWriteDistributionModeIfNotSet(Transaction txn, Table table) {
    if (!table.properties().containsKey(TableProperties.WRITE_DISTRIBUTION_MODE)) {
      logger.info(
          "Updating {} to \"{}\"",
          TableProperties.WRITE_DISTRIBUTION_MODE,
          TableProperties.WRITE_DISTRIBUTION_MODE_RANGE);
      txn.updateProperties()
          .set(
              TableProperties.WRITE_DISTRIBUTION_MODE,
              // FIXME: may not always be what we want
              TableProperties.WRITE_DISTRIBUTION_MODE_RANGE)
          .commit();
    }
  }

  private static List<DataFile> processFile(
      RESTCatalog catalog,
      Table table,
      PartitionSpec tableSpec,
      SortOrder tableOrderSpec,
      FileIO tableIO,
      FileIO inputIO,
      Set<String> tableDataFiles,
      Options options,
      Lazy<S3Client> s3ClientLazy,
      DataFileNamingStrategy dstDataFileSource,
      Schema tableSchema,
      DataFileNamingStrategy.Name dataFileNamingStrategy,
      String file)
      throws IOException {
    logger.info("{}: processing", file);
    logger.info("{}: jvm: {}", file, Stats.gather());

    Function<String, Boolean> checkNotExists =
        dataFile -> {
          if (tableDataFiles.contains(dataFile)) {
            if (options.skipDuplicates()) {
              logger.info("{}: duplicate (skipping)", file);
              return true;
            }
            throw new AlreadyExistsException(
                String.format("%s is already referenced by the table", dataFile));
          }
          return false;
        };

    InputFile inputFile = Input.newFile(file, catalog, inputIO == null ? tableIO : inputIO);
    ParquetMetadata metadata = Metadata.read(inputFile);

    MessageType type = metadata.getFileMetaData().getSchema();
    Schema fileSchema = ParquetSchemaUtil.convert(type); // nameMapping applied (when present)
    if (!sameSchema(table, fileSchema)) {
      throw new BadRequestException(
          String.format("%s's schema doesn't match table's schema", file));
    }
    // assuming datafiles can be anywhere when table.location() is empty
    var noCopyPossible = file.startsWith(table.location()) || options.forceNoCopy();
    // TODO: check before uploading anything
    if (options.noCopy() && !noCopyPossible) {
      throw new BadRequestException(
          file + " cannot be added to catalog without copy"); // TODO: explain
    }

    long dataFileSizeInBytes;

    var start = System.currentTimeMillis();
    var dataFile = Strings.replacePrefix(file, "s3a://", "s3://");
    if (options.noCopy()) {
      if (checkNotExists.apply(dataFile)) {
        return Collections.emptyList();
      }
      dataFileSizeInBytes = inputFile.getLength();
      // check if partition spec is defined.
      if (tableSpec.isPartitioned()) {
        // For partitioned tables with no-copy, we need to create DataFile objects
        // that reference the original file with the table's partition spec
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        Metrics metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), metricsConfig);
        return createNoCopyPartitionedDataFiles(dataFile, tableSpec, inputFile, metrics);
      }
    } else if (options.s3CopyObject()) {
      if (!dataFile.startsWith("s3://") || !table.location().startsWith("s3://")) {
        throw new BadRequestException("--s3-copy-object is only supported between s3:// buckets");
      }
      String dstDataFile = dstDataFileSource.get(file);
      if (checkNotExists.apply(dstDataFile)) {
        return Collections.emptyList();
      }
      S3.BucketPath src = S3.bucketPath(dataFile);
      S3.BucketPath dst = S3.bucketPath(dstDataFile);
      logger.info("{}: fast copying to {}", file, dstDataFile);
      CopyObjectRequest copyReq =
          CopyObjectRequest.builder()
              .sourceBucket(src.bucket())
              .sourceKey(src.path())
              .destinationBucket(dst.bucket())
              .destinationKey(dst.path())
              .build();
      s3ClientLazy.getValue().copyObject(copyReq);
      dataFileSizeInBytes = inputFile.getLength();
      dataFile = dstDataFile;
    } else if (tableSpec.isPartitioned()) {
      return copyParquetWithPartition(
          file, tableSchema, tableSpec, tableOrderSpec, tableIO, inputFile, dstDataFileSource);
    } else if (tableOrderSpec.isSorted()) {
      return Collections.singletonList(
          copyParquetWithSortOrder(
              file,
              Strings.replacePrefix(dstDataFileSource.get(file), "s3://", "s3a://"),
              tableSchema,
              tableSpec,
              tableOrderSpec,
              tableIO,
              inputFile,
              dataFileNamingStrategy));
    } else {
      String dstDataFile = dstDataFileSource.get(file);
      if (checkNotExists.apply(dstDataFile)) {
        return Collections.emptyList();
      }
      OutputFile outputFile =
          tableIO.newOutputFile(Strings.replacePrefix(dstDataFile, "s3://", "s3a://"));
      // TODO: support transferTo below (note that compression, etc. might be different)
      // try (var d = outputFile.create()) { try (var s = inputFile.newStream()) {
      // s.transferTo(d); }}
      Parquet.ReadBuilder readBuilder =
          Parquet.read(inputFile)
              .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
              .project(tableSchema); // TODO: ?
      // TODO: reuseContainers?
      Parquet.WriteBuilder writeBuilder =
          Parquet.write(outputFile)
              .overwrite(dataFileNamingStrategy == DataFileNamingStrategy.Name.PRESERVE_ORIGINAL)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .schema(tableSchema);
      logger.info("{}: copying to {}", file, dstDataFile);
      // file size may have changed due to different compression, etc.
      dataFileSizeInBytes = copy(readBuilder, writeBuilder);
      dataFile = dstDataFile;
    }
    logger.info(
        "{}: adding data file (copy took {}s)", file, (System.currentTimeMillis() - start) / 1000);
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    Metrics metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), metricsConfig);
    DataFile dataFileObj =
        new DataFiles.Builder(tableSpec)
            .withPath(dataFile)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(dataFileSizeInBytes)
            .withMetrics(metrics)
            .build();
    return Collections.singletonList(dataFileObj);
  }

  /**
   * Creates DataFile objects for no-copy scenarios with partitioning. This function extracts
   * partition keys from the InputFile and creates DataFile objects with partition metadata.
   */
  private static List<DataFile> createNoCopyPartitionedDataFiles(
      String file, PartitionSpec tableSpec, InputFile inputFile, Metrics metrics) {

    List<DataFile> dataFiles = new ArrayList<>();

    try {
      // Extract partition keys from the InputFile
      PartitionKey partitionKey = extractPartitionKeyFromFile(inputFile, tableSpec);

      // Validate that we have a valid partition key
      if (partitionKey != null && tableSpec.fields().size() > 0) {
        DataFile dataFile =
            DataFiles.builder(tableSpec)
                .withPartition(partitionKey)
                .withPath(file)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(inputFile.getLength())
                .withMetrics(metrics)
                .build();

        dataFiles.add(dataFile);
        logger.info("{}: created partitioned data file with partition key: {}", file, partitionKey);
      } else {
        throw new IOException("Invalid partition key extracted from file");
      }

    } catch (Exception e) {
      logger.warn(
          "{}: could not extract partition key from file, creating unpartitioned data file",
          file,
          e);

      // Fallback to unpartitioned data file
      DataFile dataFile =
          DataFiles.builder(tableSpec)
              .withPath(file)
              .withFormat(FileFormat.PARQUET)
              .withFileSizeInBytes(inputFile.getLength())
              .withMetrics(metrics)
              .build();

      dataFiles.add(dataFile);
    }

    return dataFiles;
  }

  /** Extracts partition key from InputFile by reading a sample of records. */
  private static PartitionKey extractPartitionKeyFromFile(
      InputFile inputFile, PartitionSpec tableSpec) throws IOException {
    // Read the first record to extract partition values
    Parquet.ReadBuilder readBuilder =
        Parquet.read(inputFile)
            .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSpec.schema(), s))
            .project(tableSpec.schema());

    try (CloseableIterable<Record> records = readBuilder.build()) {
      for (Record record : records) {
        // Create partition key from the first record
        PartitionKey partitionKey = new PartitionKey(tableSpec, tableSpec.schema());

        logger.debug(
            "Extracting partition key from record with {} partition fields",
            tableSpec.fields().size());

        for (PartitionField field : tableSpec.fields()) {
          try {
            String fieldName = tableSpec.schema().findField(field.sourceId()).name();
            Object value = record.getField(fieldName);

            logger.debug(
                "Field: {}, SourceId: {}, Value: {}", field.name(), field.sourceId(), value);

            if (value != null) {
              // Convert value based on partition transform
              Object convertedValue = convertValueForPartition(value, field);
              partitionKey.set(field.sourceId(), convertedValue);
              logger.debug("Set partition value for field {}: {}", field.name(), convertedValue);
            }
          } catch (Exception e) {
            logger.warn(
                "Could not extract partition value for field {}: {}", field.name(), e.getMessage());
            // Continue with other fields instead of failing completely
          }
        }

        return partitionKey;
      }
    }

    throw new IOException("No records found in file to extract partition key");
  }

  /** Converts a field value to the appropriate type for partition key based on transform. */
  private static Object convertValueForPartition(Object value, PartitionField field) {
    String transformName = field.transform().toString();

    switch (transformName) {
      case "identity":
        return value;
      case "day":
      case "month":
      case "hour":
        // For time-based partitions, convert to micros
        try {
          return toPartitionMicros(value);
        } catch (Exception e) {
          logger.warn("Could not convert value '{}' for field '{}' to micros", value, field.name());
          return value;
        }
      default:
        return value;
    }
  }

  private static List<DataFile> copyParquetWithPartition(
      String file,
      Schema tableSchema,
      PartitionSpec tableSpec,
      SortOrder tableOrderSpec,
      FileIO tableIO,
      InputFile inputFile,
      DataFileNamingStrategy dstDataFileSource)
      throws IOException {
    logger.info("{}: partitioning{}", file, tableOrderSpec.isSorted() ? "+sorting" : "");

    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(tableSchema, tableSpec);

    PartitionKey partitionKeyMold = new PartitionKey(tableSpec, tableSchema);
    Map<PartitionKey, List<Record>> partitionedRecords = new HashMap<>();

    Parquet.ReadBuilder readBuilder =
        Parquet.read(inputFile)
            .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
            .project(tableSchema);

    try (CloseableIterable<Record> records = readBuilder.build()) {
      for (Record record : records) {
        Record partitionRecord = GenericRecord.create(tableSchema);
        for (Types.NestedField field : tableSchema.columns()) {
          partitionRecord.setField(field.name(), record.getField(field.name()));
        }
        for (PartitionField field : tableSpec.fields()) {
          String fieldName = tableSchema.findField(field.sourceId()).name();
          Object value = partitionRecord.getField(fieldName);
          if (value != null) {
            String transformName = field.transform().toString();
            switch (transformName) {
              case "day", "month", "hour":
                long micros = toPartitionMicros(value);
                partitionRecord.setField(fieldName, micros);
                break;
              case "identity":
                break;
              default:
                throw new UnsupportedOperationException(
                    "unexpected transform value: " + transformName);
            }
          }
        }

        // Partition based on converted values
        partitionKeyMold.partition(partitionRecord);
        PartitionKey partitionKey = partitionKeyMold.copy();

        // Store the original record (without converted timestamp fields)
        partitionedRecords.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(record);
      }
    }

    List<DataFile> dataFiles = new ArrayList<>();

    // Create a comparator based on table.sortOrder()
    RecordComparator comparator =
        tableOrderSpec.isSorted() ? new RecordComparator(tableOrderSpec, tableSchema) : null;

    // Write sorted records for each partition
    for (Map.Entry<PartitionKey, List<Record>> entry : partitionedRecords.entrySet()) {
      PartitionKey partKey = entry.getKey();
      List<Record> records = entry.getValue();

      // Sort records within the partition
      if (comparator != null) {
        records.sort(comparator);
      }

      String dstDataFile = dstDataFileSource.get(tableSpec, partKey, file);
      OutputFile outFile =
          tableIO.newOutputFile(Strings.replacePrefix(dstDataFile, "s3://", "s3a://"));

      long fileSizeInBytes;
      Metrics metrics;
      try (FileAppender<Record> appender =
          appenderFactory.newAppender(outFile, FileFormat.PARQUET)) {
        for (Record rec : records) {
          appender.add(rec);
        }
        appender.close();
        fileSizeInBytes = appender.length();
        metrics = appender.metrics();
      }

      logger.info("{}: adding data file: {}", file, dstDataFile);
      dataFiles.add(
          DataFiles.builder(tableSpec)
              .withPath(outFile.location())
              .withFileSizeInBytes(fileSizeInBytes)
              .withPartition(partKey)
              .withFormat(FileFormat.PARQUET)
              .withMetrics(metrics)
              .build());
    }

    return dataFiles;
  }

  public static long toPartitionMicros(Object tsValue) {
    switch (tsValue) {
      case Long l -> {
        return l;
      }
      case String s -> {
        LocalDateTime ldt = LocalDateTime.parse(s);
        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
      }
      case LocalDate localDate -> {
        return localDate.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
      }
      case LocalDateTime localDateTime -> {
        return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
      }
      case OffsetDateTime offsetDateTime -> {
        return offsetDateTime.toInstant().toEpochMilli() * 1000L;
      }
      case Instant instant -> {
        return instant.toEpochMilli() * 1000L;
      }
      default ->
          throw new UnsupportedOperationException("unexpected value type: " + tsValue.getClass());
    }
  }

  private static DataFile copyParquetWithSortOrder(
      String file,
      String dstDataFile,
      Schema tableSchema,
      PartitionSpec tableSpec,
      SortOrder tableOrderSpec,
      FileIO tableIO,
      InputFile inputFile,
      DataFileNamingStrategy.Name dataFileNamingStrategy)
      throws IOException {
    logger.info("{}: copying (sorted) to {}", file, dstDataFile);

    long start = System.currentTimeMillis();

    OutputFile outputFile = tableIO.newOutputFile(dstDataFile);

    Parquet.ReadBuilder readBuilder =
        Parquet.read(inputFile)
            .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
            .project(tableSchema);

    // Read records into memory
    List<Record> records = new ArrayList<>();
    try (CloseableIterable<Record> iterable = readBuilder.build()) {
      for (Record r : iterable) {
        records.add(r);
      }
    }

    // Sort
    if (!tableOrderSpec.isUnsorted()) {
      records.sort(new RecordComparator(tableOrderSpec, tableSchema));
    }

    // Write sorted records to outputFile
    Parquet.WriteBuilder writeBuilder =
        Parquet.write(outputFile)
            .overwrite(
                dataFileNamingStrategy == DataFileNamingStrategy.Name.PRESERVE_ORIGINAL) // FIXME
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .schema(tableSchema);

    long fileSizeInBytes;
    Metrics metrics;
    try (FileAppender<Record> appender = writeBuilder.build()) {
      for (Record record : records) {
        appender.add(record);
      }
      appender.close();
      fileSizeInBytes = appender.length();
      metrics = appender.metrics();
    }

    logger.info(
        "{}: adding data file (copy (sorted) took {}s)",
        file,
        (System.currentTimeMillis() - start) / 1000);

    return new DataFiles.Builder(tableSpec)
        .withPath(dstDataFile)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(fileSizeInBytes)
        .withMetrics(metrics)
        .build();
  }

  private static boolean sameSchema(Table table, Schema fileSchema) {
    boolean sameSchema;
    Schema tableSchema = table.schema();
    String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    if (nameMapping != null && !nameMapping.isEmpty()) {
      NameMapping mapping = NameMappingParser.fromJson(nameMapping);
      Map<Integer, String> tableSchemaIdToName = tableSchema.idToName();
      var tableSchemaWithNameMappingApplied =
          TypeUtil.assignIds(
              Types.StructType.of(tableSchema.columns()),
              oldId -> {
                var fieldName = tableSchemaIdToName.get(oldId);
                MappedField mappedField = mapping.find(fieldName);
                return mappedField.id();
              });
      sameSchema = tableSchemaWithNameMappingApplied.asStructType().equals(fileSchema.asStruct());
    } else {
      sameSchema = tableSchema.sameSchema(fileSchema);
    }
    return sameSchema;
  }

  private static long copy(Parquet.ReadBuilder rb, Parquet.WriteBuilder wb) throws IOException {
    try (CloseableIterable<Record> parquetReader = rb.build()) {
      // not using try-with-resources because we need to close() for writer.length()
      FileAppender<Record> writer = null;
      try {
        writer = wb.build();
        writer.addAll(parquetReader);
      } finally {
        if (writer != null) {
          writer.close();
        }
      }
      return writer.length();
    }
  }

  public interface DataFileNamingStrategy {
    String get(String file);

    String get(PartitionSpec spec, StructLike partitionData, String file);

    enum Name {
      DEFAULT,
      PRESERVE_ORIGINAL;
    }

    static String defaultDataLocation(Table table) {
      return String.format("%s/%s", table.location().replaceAll("/+$", ""), "data");
    }

    record Default(String parent, String prefix) implements DataFileNamingStrategy {

      @Override
      public String get(String file) {
        String fileName = prefix + DigestUtils.sha256Hex(file) + ".parquet";
        return String.format("%s/%s", parent, fileName);
      }

      @Override
      public String get(PartitionSpec spec, StructLike partitionData, String file) {
        String fileName = prefix + DigestUtils.sha256Hex(file) + "-part.parquet";
        return String.format("%s/%s/%s", parent, spec.partitionToPath(partitionData), fileName);
      }
    }

    record PreserveOriginal(String parent) implements DataFileNamingStrategy {

      @Override
      public String get(String file) {
        if (!file.endsWith(".parquet")) {
          throw new UnsupportedOperationException(
              "expected " + file + " to have .parquet extension"); // FIXME: remove
        }
        String fileName = file.substring(file.lastIndexOf("/") + 1);
        return String.format("%s/%s", parent, fileName);
      }

      @Override
      public String get(PartitionSpec spec, StructLike partitionData, String file) {
        throw new UnsupportedOperationException();
      }
    }
  }

  public record Options(
      boolean skipDuplicates,
      boolean noCommit,
      boolean noCopy,
      boolean forceNoCopy,
      boolean forceTableAuth,
      boolean s3NoSignRequest,
      boolean s3CopyObject,
      int threadCount) {

    public static Builder builder() {
      return new Builder();
    }

    public Builder toBuilder() {
      return builder()
          .skipDuplicates(skipDuplicates)
          .noCommit(noCommit)
          .noCopy(noCopy)
          .forceNoCopy(forceNoCopy)
          .forceTableAuth(forceTableAuth)
          .s3NoSignRequest(s3NoSignRequest)
          .s3CopyObject(s3CopyObject)
          .threadCount(threadCount);
    }

    public static final class Builder {
      private boolean skipDuplicates;
      private boolean noCommit;
      private boolean noCopy;
      private boolean forceNoCopy;
      private boolean forceTableAuth;
      private boolean s3NoSignRequest;
      private boolean s3CopyObject;
      private int threadCount = Runtime.getRuntime().availableProcessors();

      private Builder() {}

      public Builder skipDuplicates(boolean skipDuplicates) {
        this.skipDuplicates = skipDuplicates;
        return this;
      }

      public Builder noCommit(boolean noCommit) {
        this.noCommit = noCommit;
        return this;
      }

      public Builder noCopy(boolean noCopy) {
        this.noCopy = noCopy;
        return this;
      }

      public Builder forceNoCopy(boolean forceNoCopy) {
        this.forceNoCopy = forceNoCopy;
        return this;
      }

      public Builder forceTableAuth(boolean forceTableAuth) {
        this.forceTableAuth = forceTableAuth;
        return this;
      }

      public Builder s3NoSignRequest(boolean s3NoSignRequest) {
        this.s3NoSignRequest = s3NoSignRequest;
        return this;
      }

      public Builder s3CopyObject(boolean s3CopyObject) {
        this.s3CopyObject = s3CopyObject;
        return this;
      }

      public Builder threadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
      }

      public Options build() {
        return new Options(
            skipDuplicates,
            noCommit,
            noCopy,
            forceNoCopy,
            forceTableAuth,
            s3NoSignRequest,
            s3CopyObject,
            threadCount);
      }
    }
  }
}
