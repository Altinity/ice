package com.altinity.ice.internal.cmd;

import com.altinity.ice.internal.aws.S3;
import com.altinity.ice.internal.iceberg.DataFileNamingStrategy;
import com.altinity.ice.internal.io.Input;
import com.altinity.ice.internal.io.RetryLog;
import com.altinity.ice.internal.jvm.Stats;
import com.altinity.ice.internal.parquet.Metadata;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
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
import org.apache.parquet.hadoop.metadata.BlockMetaData;
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
      List<String> partitionColumns,
      List<String> sortColumns,
      int threadCount)
      throws IOException, InterruptedException {
    if (files.length == 0) {
      // no work to be done
      return;
    }

    InsertOptions options =
        InsertOptions.builder()
            .skipDuplicates(skipDuplicates)
            .noCommit(noCommit)
            .noCopy(noCopy)
            .forceNoCopy(forceNoCopy)
            .forceTableAuth(forceTableAuth)
            .s3NoSignRequest(s3NoSignRequest)
            .s3CopyObject(s3CopyObject)
            .threadCount(threadCount)
            .build();

    final InsertOptions finalOptions =
        options.forceNoCopy() ? options.toBuilder().noCopy(true).build() : options;
    Table table = catalog.loadTable(nsTable);

    // Update partition spec if provided
    if (partitionColumns != null && !partitionColumns.isEmpty()) {
      var updateSpec = table.updateSpec();
      for (String column : partitionColumns) {
        updateSpec.addField(column);
      }
      updateSpec.commit();
    }

    // Update sort order if provided
    if (sortColumns != null && !sortColumns.isEmpty()) {
      table
          .updateProperties()
          .set(
              TableProperties.WRITE_DISTRIBUTION_MODE,
              TableProperties.WRITE_DISTRIBUTION_MODE_RANGE)
          .commit();
      var updatedSortOrder = table.replaceSortOrder();
      for (String column : sortColumns) {
        updatedSortOrder.asc(column);
      }
      updatedSortOrder.commit();
    }

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
              case INPUT_FILENAME -> new DataFileNamingStrategy.InputFilename(dstPath);
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
            executor.awaitTermination(1, TimeUnit.MINUTES);
            executor.shutdownNow();
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

  private static List<DataFile> processFile(
      Table table,
      RESTCatalog catalog,
      FileIO tableIO,
      FileIO inputIO,
      Set<String> tableDataFiles,
      InsertOptions options,
      Lazy<S3Client> s3ClientLazy,
      DataFileNamingStrategy dstDataFileSource,
      Schema tableSchema,
      DataFileNamingStrategy.Name dataFileNamingStrategy,
      String file,
      List<String> partitionColumns)
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
    var dataFile = replacePrefix(file, "s3a://", "s3://");
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
      dataFileSizeInBytes = inputFile.getLength();
      dataFile = dstDataFile;
    } else if (partitionColumns != null && !partitionColumns.isEmpty()) {
      String dstDataFile = dstDataFileSource.get(file);
      if (checkNotExists.apply(dstDataFile)) {
        return null;
      }
      return copyParquetWithPartition(file, dstDataFile, tableSchema, table, inputFile);
    } else {
      String dstDataFile = dstDataFileSource.get(file);
      if (checkNotExists.apply(dstDataFile)) {
        return null;
      }
      OutputFile outputFile = tableIO.newOutputFile(replacePrefix(dstDataFile, "s3://", "s3a://"));
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
              .overwrite(dataFileNamingStrategy == DataFileNamingStrategy.Name.INPUT_FILENAME)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .schema(tableSchema);
      logger.info("{}: copying to {}", file, dstDataFile);
      // file size may have changed due to different compression, etc.
      dataFileSizeInBytes = copy(readBuilder, writeBuilder);
      dataFile = dstDataFile;
    }
    logger.info("{}: adding data file", file);
    long recordCount = metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    Metrics metrics = ParquetUtil.fileMetrics(inputFile, metricsConfig);
    DataFile dataFileObj =
        new DataFiles.Builder(table.spec())
            .withPath(dataFile)
            .withFormat("PARQUET")
            .withRecordCount(recordCount)
            .withFileSizeInBytes(dataFileSizeInBytes)
            .withMetrics(metrics)
            .build();
    return Collections.singletonList(dataFileObj);
  }

  private static List<DataFile> copyParquetWithPartition(
      String file, String dstDataFile, Schema tableSchema, Table table, InputFile inputFile)
      throws IOException {

    logger.info("{}: copying to partitions under {}", file, dstDataFile);

    // Partition writer setup
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 0).format(FileFormat.PARQUET).build();

    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(tableSchema, table.spec());

    PartitionKey partitionKey = new PartitionKey(table.spec(), tableSchema);
    Map<PartitionKey, FileAppender<Record>> openAppenders = new HashMap<>();
    Map<PartitionKey, OutputFile> writtenFiles = new HashMap<>();

    Parquet.ReadBuilder readBuilder =
        Parquet.read(inputFile)
            .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
            .project(tableSchema)
            .reuseContainers();
    Map<PartitionKey, Long> recordCounts = new HashMap<>();

    try (CloseableIterable<Record> records = readBuilder.build()) {
      for (Record record : records) {
        partitionKey.partition(record);
        PartitionKey keyCopy = partitionKey.copy();

        FileAppender<Record> appender = openAppenders.get(keyCopy);
        if (appender == null) {
          OutputFile outFile = fileFactory.newOutputFile(keyCopy).encryptingOutputFile();
          appender = appenderFactory.newAppender(outFile, FileFormat.PARQUET);
          openAppenders.put(keyCopy, appender);
          writtenFiles.put(keyCopy, outFile);
          recordCounts.put(keyCopy, 0L);
        }

        appender.add(record);
        recordCounts.put(keyCopy, recordCounts.get(keyCopy) + 1);
      }
    }

    List<DataFile> dataFiles = new ArrayList<>();

    for (Map.Entry<PartitionKey, FileAppender<Record>> entry : openAppenders.entrySet()) {
      PartitionKey partKey = entry.getKey();
      FileAppender<Record> appender = entry.getValue();
      appender.close();

      OutputFile outFile = writtenFiles.get(partKey);
      InputFile inFile = outFile.toInputFile();

      MetricsConfig metricsConfig = MetricsConfig.forTable(table);
      Metrics metrics = ParquetUtil.fileMetrics(inFile, metricsConfig);

      dataFiles.add(
          DataFiles.builder(table.spec())
              .withPath(outFile.location())
              .withFileSizeInBytes(inFile.getLength())
              .withPartition(partKey)
              .withFormat(FileFormat.PARQUET)
              .withRecordCount(recordCounts.get(partKey))
              .withMetrics(metrics)
              .build());
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

  private static String replacePrefix(String text, String from, String to) {
    if (text.startsWith(from)) {
      return to + text.substring(from.length());
    }
    return text;
  }
}
