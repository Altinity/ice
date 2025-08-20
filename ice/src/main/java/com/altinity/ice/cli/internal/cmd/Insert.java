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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
      RESTCatalog catalog, TableIdentifier nsTable, String[] files, Options options)
      throws IOException, InterruptedException {
    if (files.length == 0) {
      // no work to be done
      return;
    }

    Table table = catalog.loadTable(nsTable);

    // Create transaction and pass it to updatePartitionAndSortOrderMetadata
    Transaction txn = table.newTransaction();

    try (FileIO tableIO = table.io()) {
      final Supplier<S3Client> s3ClientSupplier;
      if (options.forceTableAuth()) {
        if (!(tableIO instanceof S3FileIO)) {
          throw new UnsupportedOperationException(
              "--force-table-auth is currently only supported for s3:// tables");
        }
        s3ClientSupplier = ((S3FileIO) tableIO)::client;
      } else {
        s3ClientSupplier = () -> S3.newClient(options.s3NoSignRequest());
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
            syncTablePartitionSpec(txn, table, tableSchema, tableEmpty, options.partitionList);
        var tableSortOrder =
            syncTableSortOrder(txn, table, tableSchema, tableEmpty, options.sortOrderList);
        if (tablePartitionSpec.isPartitioned() || tableSortOrder.isSorted()) {
          updateWriteDistributionModeIfNotSet(txn, table);
        }

        String dstPath = DataFileNamingStrategy.defaultDataLocation(table);
        DataFileNamingStrategy dstDataFileSource =
            switch (options.dataFileNamingStrategy) {
              case DEFAULT ->
                  new DataFileNamingStrategy.Default(dstPath, System.currentTimeMillis() + "-");
              case PRESERVE_ORIGINAL -> new DataFileNamingStrategy.PreserveOriginal(dstPath);
            };

        // appendOp to use the same transaction.
        AppendFiles appendOp = txn.newAppend();

        try (FileIO inputIO = Input.newIO(filesExpanded.getFirst(), table, s3ClientLazy);
            RetryLog retryLog =
                options.retryListFile != null && !options.retryListFile.isEmpty()
                    ? new RetryLog(options.retryListFile)
                    : null) {
          boolean atLeastOneFileAppended = false;

          int numThreads = Math.min(options.threadCount(), filesExpanded.size());
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
                              options,
                              s3ClientLazy,
                              dstDataFileSource,
                              tableSchemaCopy,
                              options.dataFileNamingStrategy,
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

          if (!options.noCommit()) {
            // TODO: log
            if (atLeastOneFileAppended) {
              appendOp.commit();
            } else {
              logger.warn("Table commit skipped (no files to append)");
            }
            if (retryLog != null) {
              retryLog.commit();
            }
            if (atLeastOneFileAppended) {
              // Commit transaction.
              txn.commitTransaction();
            }
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
      PartitionSpec partitionSpec,
      SortOrder sortOrder,
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

    boolean sorted = options.assumeSorted;
    if (!sorted && sortOrder.isSorted()) {
      sorted = Sorting.isSorted(inputFile, tableSchema, sortOrder);
      if (!sorted) {
        if (options.noCopy || options.s3CopyObject) {
          throw new BadRequestException(
              String.format("%s does not appear to be sorted", inputFile.location()));
        }
        logger.warn(
            "{} does not appear to be sorted. Falling back to full scan (slow)",
            inputFile.location());
      }
    }

    PartitionKey partitionKey = null;
    if (partitionSpec.isPartitioned()) {
      partitionKey = Partitioning.inferPartitionKey(metadata, partitionSpec);
      if (partitionKey == null) {
        if (options.noCopy || options.s3CopyObject) {
          throw new BadRequestException(
              String.format(
                  "Cannot infer partition key of %s from the metadata", inputFile.location()));
        }
        logger.warn(
            "{} does not appear to be partitioned. Falling back to full scan (slow)",
            inputFile.location());
      }
    }

    MessageType type = metadata.getFileMetaData().getSchema();
    Schema fileSchema = ParquetSchemaUtil.convert(type); // nameMapping applied (when present)
    if (!sameSchema(table, fileSchema)) {
      throw new BadRequestException(
          String.format("%s's schema doesn't match table's schema", file));
    }
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
    } else if (partitionSpec.isPartitioned() && partitionKey == null) {
      return copyPartitionedAndSorted(
          file, tableSchema, partitionSpec, sortOrder, tableIO, inputFile, dstDataFileSource);
    } else if (sortOrder.isSorted() && !sorted) {
      return Collections.singletonList(
          copySorted(
              file,
              dstDataFileSource.get(file),
              tableSchema,
              partitionSpec,
              sortOrder,
              tableIO,
              inputFile,
              dataFileNamingStrategy,
              partitionKey));
    } else {
      // Table isn't partitioned or sorted. Copy as is.
      String dstDataFile = dstDataFileSource.get(file);
      if (checkNotExists.apply(dstDataFile)) {
        return Collections.emptyList();
      }
      OutputFile outputFile =
          tableIO.newOutputFile(Strings.replacePrefix(dstDataFile, "s3://", "s3a://"));
      // TODO: support transferTo below (note that compression, etc. might be different)
      // try (var d = outputFile.create()) {
      //   try (var s = inputFile.newStream()) { s.transferTo(d); }
      // }
      Parquet.ReadBuilder readBuilder =
          Parquet.read(inputFile)
              .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
              .project(tableSchema)
              .reuseContainers();
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
        new DataFiles.Builder(partitionSpec)
            .withPath(dataFile)
            .withFormat("PARQUET")
            .withFileSizeInBytes(dataFileSizeInBytes)
            .withMetrics(metrics)
            .withPartition(partitionKey)
            .build();
    return Collections.singletonList(dataFileObj);
  }

  private static List<DataFile> copyPartitionedAndSorted(
      String file,
      Schema tableSchema,
      PartitionSpec partitionSpec,
      SortOrder sortOrder,
      FileIO tableIO,
      InputFile inputFile,
      DataFileNamingStrategy dstDataFileSource)
      throws IOException {
    logger.info("{}: partitioning{}", file, sortOrder.isSorted() ? "+sorting" : "");

    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(tableSchema, partitionSpec);

    // FIXME: stream to reduce memory usage
    Map<PartitionKey, List<Record>> partitionedRecords =
        Partitioning.partition(inputFile, tableSchema, partitionSpec);

    // Create a comparator based on table.sortOrder()
    RecordComparator comparator =
        sortOrder.isSorted() ? new RecordComparator(sortOrder, tableSchema) : null;

    List<DataFile> dataFiles = new ArrayList<>(partitionedRecords.size());

    // Write sorted records for each partition
    for (Map.Entry<PartitionKey, List<Record>> entry : partitionedRecords.entrySet()) {
      PartitionKey partKey = entry.getKey();
      List<Record> records = entry.getValue();
      entry.setValue(List.of()); // allow "records" to be gc-ed once w're done with them

      // Sort records within the partition
      if (comparator != null) {
        records.sort(comparator);
      }

      String dstDataFile = dstDataFileSource.get(partitionSpec, partKey, file);
      OutputFile outputFile =
          tableIO.newOutputFile(Strings.replacePrefix(dstDataFile, "s3://", "s3a://"));

      long fileSizeInBytes;
      Metrics metrics;
      try (FileAppender<Record> appender =
          appenderFactory.newAppender(outputFile, FileFormat.PARQUET)) {
        for (Record record : records) {
          appender.add(record);
        }
        appender.close();
        fileSizeInBytes = appender.length();
        metrics = appender.metrics();
      }

      logger.info("{}: adding data file: {}", file, dstDataFile);
      dataFiles.add(
          DataFiles.builder(partitionSpec)
              .withPath(outputFile.location())
              .withFileSizeInBytes(fileSizeInBytes)
              .withFormat(FileFormat.PARQUET)
              .withMetrics(metrics)
              .withPartition(partKey)
              .build());
    }

    return dataFiles;
  }

  private static DataFile copySorted(
      String file,
      String dstDataFile,
      Schema tableSchema,
      PartitionSpec partitionSpec,
      SortOrder sortOrder,
      FileIO tableIO,
      InputFile inputFile,
      DataFileNamingStrategy.Name dataFileNamingStrategy,
      PartitionKey partitionKey)
      throws IOException {
    logger.info("{}: copying (sorted) to {}", file, dstDataFile);

    long start = System.currentTimeMillis();

    OutputFile outputFile =
        tableIO.newOutputFile(Strings.replacePrefix(dstDataFile, "s3://", "s3a://"));

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
    if (!sortOrder.isUnsorted()) {
      records.sort(new RecordComparator(sortOrder, tableSchema));
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

    return new DataFiles.Builder(partitionSpec)
        .withPath(outputFile.location())
        .withFormat("PARQUET")
        .withFileSizeInBytes(fileSizeInBytes)
        .withMetrics(metrics)
        .withPartition(partitionKey)
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
      DataFileNamingStrategy.Name dataFileNamingStrategy,
      boolean skipDuplicates,
      boolean noCommit,
      boolean noCopy,
      boolean forceNoCopy,
      boolean forceTableAuth,
      boolean s3NoSignRequest,
      boolean s3CopyObject,
      boolean assumeSorted,
      @Nullable String retryListFile,
      @Nullable List<Main.IcePartition> partitionList,
      @Nullable List<Main.IceSortOrder> sortOrderList,
      int threadCount) {

    public static Builder builder() {
      return new Builder();
    }

    public static final class Builder {
      DataFileNamingStrategy.Name dataFileNamingStrategy;
      private boolean skipDuplicates;
      private boolean noCommit;
      private boolean noCopy;
      private boolean forceNoCopy;
      private boolean forceTableAuth;
      private boolean s3NoSignRequest;
      private boolean s3CopyObject;
      private boolean assumeSorted;
      String retryListFile;
      List<Main.IcePartition> partitionList = List.of();
      List<Main.IceSortOrder> sortOrderList = List.of();
      private int threadCount = Runtime.getRuntime().availableProcessors();

      private Builder() {}

      public Builder dataFileNamingStrategy(DataFileNamingStrategy.Name dataFileNamingStrategy) {
        this.dataFileNamingStrategy = dataFileNamingStrategy;
        return this;
      }

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

      public Builder assumeSorted(boolean assumeSorted) {
        this.assumeSorted = assumeSorted;
        return this;
      }

      public Builder retryListFile(String retryListFile) {
        this.retryListFile = retryListFile;
        return this;
      }

      public Builder partitionList(List<Main.IcePartition> partitionList) {
        this.partitionList = partitionList;
        return this;
      }

      public Builder sortOrderList(List<Main.IceSortOrder> sortOrderList) {
        this.sortOrderList = sortOrderList;
        return this;
      }

      public Builder threadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
      }

      public Options build() {
        return new Options(
            dataFileNamingStrategy,
            skipDuplicates,
            noCommit,
            forceNoCopy || noCopy,
            forceNoCopy,
            forceTableAuth,
            s3NoSignRequest,
            s3CopyObject,
            assumeSorted,
            retryListFile,
            partitionList,
            sortOrderList,
            threadCount);
      }
    }
  }
}
