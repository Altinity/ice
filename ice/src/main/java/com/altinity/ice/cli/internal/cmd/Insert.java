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
import com.altinity.ice.cli.internal.iceberg.io.Input;
import com.altinity.ice.cli.internal.iceberg.parquet.Metadata;
import com.altinity.ice.cli.internal.jvm.Stats;
import com.altinity.ice.cli.internal.retry.RetryLog;
import com.altinity.ice.cli.internal.s3.S3;
import com.altinity.ice.internal.strings.Strings;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.*;
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
      String retryListFile,
      List<Main.IcePartition> partitionColumns,
      List<Main.IceSortOrder> sortOrders,
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

    final Options finalOptions =
        options.forceNoCopy() ? options.toBuilder().noCopy(true).build() : options;
    Table table = catalog.loadTable(nsTable);

    updatePartitionAndSortOrderMetadata(table, partitionColumns, sortOrders);

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
                  .collect(Collectors.toSet());
        }

        String dstPath = DataFileNamingStrategy.defaultDataLocation(table);
        DataFileNamingStrategy dstDataFileSource =
            switch (dataFileNamingStrategy) {
              case DEFAULT ->
                  new DataFileNamingStrategy.Default(dstPath, System.currentTimeMillis() + "-");
              case PRESERVE_ORIGINAL -> new DataFileNamingStrategy.InputFilename(dstPath);
            };

        AppendFiles appendOp = table.newAppend();

        try (FileIO inputIO = Input.newIO(filesExpanded.getFirst(), table, s3ClientLazy);
            RetryLog retryLog =
                retryListFile != null && !retryListFile.isEmpty()
                    ? new RetryLog(retryListFile)
                    : null) {
          AtomicBoolean atLeastOneFileAppended = new AtomicBoolean(false);

          int numThreads = Math.min(finalOptions.threadCount(), filesExpanded.size());
          ExecutorService executor = Executors.newFixedThreadPool(numThreads);
          try {
            var futures = new ArrayList<Future<List<DataFile>>>();
            for (final String file : filesExpanded) {
              futures.add(
                  executor.submit(
                      () -> {
                        try {
                          List<DataFile> dataFiles =
                              processFile(
                                  table,
                                  catalog,
                                  tableIO,
                                  inputIO,
                                  tableDataFiles,
                                  finalOptions,
                                  s3ClientLazy,
                                  dstDataFileSource,
                                  tableSchema,
                                  dataFileNamingStrategy,
                                  file,
                                  partitionColumns);
                          if (dataFiles != null) {
                            for (DataFile df : dataFiles) {
                              atLeastOneFileAppended.set(true);
                              appendOp.appendFile(df);
                            }
                          }
                          return dataFiles;
                        } catch (Exception e) {
                          if (retryLog != null) {
                            logger.error(
                                "{}: error (adding to retry list and continuing)", file, e);
                            retryLog.add(file);
                            return null;
                          } else {
                            throw e;
                          }
                        }
                      }));
            }

            for (var future : futures) {
              try {
                List<DataFile> dataFiles = future.get();
                if (dataFiles != null) {
                  for (DataFile df : dataFiles) {
                    atLeastOneFileAppended.set(true);
                    appendOp.appendFile(df);
                  }
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while processing files", e);
              } catch (ExecutionException e) {
                if (retryLog == null) {
                  throw new IOException("Error processing files", e.getCause());
                }
              }
            }
          } finally {
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.MINUTES);
          }

          if (!finalOptions.noCommit()) {
            // TODO: log
            if (atLeastOneFileAppended.get()) {
              appendOp.commit();
            } else {
              logger.warn("Table commit skipped (no files to append)");
            }
            if (retryLog != null) {
              retryLog.commit();
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

  private static void updatePartitionAndSortOrderMetadata(
      Table table, List<Main.IcePartition> partitions, List<Main.IceSortOrder> sortOrders) {

    // Create a new transaction.
    Transaction txn = table.newTransaction();

    if (partitions != null && !partitions.isEmpty()) {
      var updateSpec = txn.updateSpec();
      for (Main.IcePartition partition : partitions) {
        String transform = partition.transform().toLowerCase();
        if (transform.startsWith("bucket[")) {
          int numBuckets = Integer.parseInt(transform.substring(7, transform.length() - 1));
          updateSpec.addField(Expressions.bucket(partition.column(), numBuckets));
        } else if (transform.startsWith("truncate[")) {
          int width = Integer.parseInt(transform.substring(9, transform.length() - 1));
          updateSpec.addField(Expressions.truncate(partition.column(), width));
        } else {
          switch (transform) {
            case "year":
              updateSpec.addField(Expressions.year(partition.column()));
              break;
            case "month":
              updateSpec.addField(Expressions.month(partition.column()));
              break;
            case "day":
              updateSpec.addField(Expressions.day(partition.column()));
              break;
            case "hour":
              updateSpec.addField(Expressions.hour(partition.column()));
              break;
            case "identity":
            default:
              updateSpec.addField(partition.column());
              break;
          }
        }
      }
      updateSpec.commit();
    }

    // Update sort order if provided
    if (sortOrders != null && !sortOrders.isEmpty()) {
      txn.updateProperties()
          .set(
              TableProperties.WRITE_DISTRIBUTION_MODE,
              TableProperties.WRITE_DISTRIBUTION_MODE_RANGE)
          .commit();

      ReplaceSortOrder replaceSortOrder = txn.replaceSortOrder();
      for (Main.IceSortOrder order : sortOrders) {
        SortDirection dir = order.desc() ? SortDirection.DESC : SortDirection.ASC;
        NullOrder nullOrd = order.nullFirst() ? NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST;
        if (dir == SortDirection.ASC) {
          replaceSortOrder.asc(order.column(), nullOrd);
        } else {
          replaceSortOrder.desc(order.column(), nullOrd);
        }
      }
      replaceSortOrder.commit();
    }

    // Commit transaction.
    txn.commitTransaction();
  }

  private static List<DataFile> processFile(
      Table table,
      RESTCatalog catalog,
      FileIO tableIO,
      FileIO inputIO,
      Set<String> tableDataFiles,
      Options options,
      Lazy<S3Client> s3ClientLazy,
      DataFileNamingStrategy dstDataFileSource,
      Schema tableSchema,
      DataFileNamingStrategy.Name dataFileNamingStrategy,
      String file,
      List<Main.IcePartition> partitionColumns)
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
    long dataFileSizeInBytes = 0;

    var start = System.currentTimeMillis();
    var dataFile = Strings.replacePrefix(file, "s3a://", "s3://");
    if (options.noCopy()) {
      if (checkNotExists.apply(dataFile)) {
        return null;
      }
      dataFileSizeInBytes = inputFile.getLength();
    } else if (options.s3CopyObject()) {
      if (!dataFile.startsWith("s3://") || !table.location().startsWith("s3://")) {
        throw new BadRequestException("--s3-copy-object is only supported between s3:// buckets");
      }
      String dstDataFile = dstDataFileSource.get(file);
      if (checkNotExists.apply(dstDataFile)) {
        return null;
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
      dataFile = dstDataFile;
    } else if (partitionColumns != null && !partitionColumns.isEmpty()) {
      String dstDataFile = dstDataFileSource.get(file);
      if (checkNotExists.apply(dstDataFile)) {
        return null;
      }
      return copyParquetWithPartition(
          file,
          Strings.replacePrefix(dstDataFileSource.get(file), "s3://", "s3a://"),
          tableSchema,
          table,
          inputFile,
          metadata);
    } else {
      String dstDataFile = dstDataFileSource.get(file);
      if (checkNotExists.apply(dstDataFile)) {
        return null;
      }

      OutputFile outputFile =
          tableIO.newOutputFile(Strings.replacePrefix(dstDataFile, "s3://", "s3a://"));

      Parquet.ReadBuilder readBuilder =
          Parquet.read(inputFile)
              .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
              .project(tableSchema);

      logger.info("{}: copying to {}", file, dstDataFile);

      // Read records into memory
      List<Record> records = new ArrayList<>();
      try (CloseableIterable<Record> iterable = readBuilder.build()) {
        for (Record r : iterable) {
          records.add(r);
        }
      }

      // Sort records if sort order is defined and non-empty
      SortOrder sortOrder = table.sortOrder();
      if (sortOrder != null && !sortOrder.isUnsorted()) {
        records.sort(new RecordSortComparator(sortOrder, tableSchema));
      }

      // Write sorted records out
      Parquet.WriteBuilder writeBuilder =
          Parquet.write(outputFile)
              .overwrite(dataFileNamingStrategy == DataFileNamingStrategy.Name.PRESERVE_ORIGINAL)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .schema(tableSchema);

      try (FileAppender<Record> appender = writeBuilder.build()) {
        for (Record record : records) {
          appender.add(record);
        }

        // dataFileSizeInBytes = appender.length();
      }

      InputFile inFile = outputFile.toInputFile();
      dataFileSizeInBytes = inFile.getLength();
      dataFile = dstDataFile;
    }
    logger.info(
        "{}: adding data file (copy took {}s)", file, (System.currentTimeMillis() - start) / 1000);
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    Metrics metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), metricsConfig);

    // dataFileSizeInBytes = inputFile.getLength();
    DataFile dataFileObj =
        new DataFiles.Builder(table.spec())
            .withPath(dataFile)
            .withFormat("PARQUET")
            // .withRecordCount(recordCount)
            .withFileSizeInBytes(dataFileSizeInBytes)
            .withMetrics(metrics)
            .build();
    return Collections.singletonList(dataFileObj);
  }

  private static List<DataFile> copyParquetWithPartition(
      String file,
      String dstDataFile,
      Schema tableSchema,
      Table table,
      InputFile inputFile,
      ParquetMetadata metadata)
      throws IOException {

    logger.info("{}: copying to partitions under {}", file, dstDataFile);
    var start = System.currentTimeMillis();

    // Partition writer setup
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 0).format(FileFormat.PARQUET).build();

    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(tableSchema, table.spec());

    PartitionKey partitionKey = new PartitionKey(table.spec(), tableSchema);
    Map<PartitionKey, List<Record>> partitionedRecords = new HashMap<>();
    Map<PartitionKey, OutputFile> writtenFiles = new HashMap<>();

    Parquet.ReadBuilder readBuilder =
        Parquet.read(inputFile)
            .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
            .project(tableSchema)
            .reuseContainers();

    // Read and group records by partition
    try (CloseableIterable<Record> records = readBuilder.build()) {
      for (Record record : records) {
        partitionKey.partition(record);
        PartitionKey keyCopy = partitionKey.copy();

        partitionedRecords.computeIfAbsent(keyCopy, k -> new ArrayList<>()).add(record);
      }
    }

    List<DataFile> dataFiles = new ArrayList<>();

    logger.info("Sort order: " + table.sortOrder().toString());
    // Create a comparator based on table.sortOrder()
    RecordSortComparator comparator = new RecordSortComparator(table.sortOrder(), tableSchema);

    // Write sorted records for each partition
    for (Map.Entry<PartitionKey, List<Record>> entry : partitionedRecords.entrySet()) {
      PartitionKey partKey = entry.getKey();
      List<Record> records = entry.getValue();

      // Sort records within the partition
      records.sort(comparator);

      OutputFile outFile = fileFactory.newOutputFile(partKey).encryptingOutputFile();
      writtenFiles.put(partKey, outFile);

      try (FileAppender<Record> appender =
          appenderFactory.newAppender(outFile, FileFormat.PARQUET)) {
        for (Record rec : records) {
          appender.add(rec);
        }
        appender.close();

        logger.info(
            "{}: adding data file (copy took {}s)",
            file,
            (System.currentTimeMillis() - start) / 1000);
        InputFile inFile = outFile.toInputFile();
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        Metrics metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), metricsConfig);
        dataFiles.add(
            DataFiles.builder(table.spec())
                .withPath(outFile.location())
                .withFileSizeInBytes(inFile.getLength())
                .withPartition(partKey)
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(records.size())
                .withMetrics(metrics)
                .build());
      }
    }

    return dataFiles;
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
    }

    record InputFilename(String parent) implements DataFileNamingStrategy {

      @Override
      public String get(String file) {
        if (!file.endsWith(".parquet")) {
          throw new UnsupportedOperationException(
              "expected " + file + " to have .parquet extension"); // FIXME: remove
        }
        String fileName = file.substring(file.lastIndexOf("/") + 1);
        return String.format("%s/%s", parent, fileName);
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
