package com.altinity.ice.internal.cmd;

import com.altinity.ice.internal.crypto.Hash;
import com.altinity.ice.internal.io.Input;
import com.altinity.ice.internal.parquet.Metadata;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
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
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Insert {

  private static final Logger logger = LoggerFactory.getLogger(Insert.class);

  private Insert() {}

  // TODO: refactor
  public static void run(
      RESTCatalog catalog,
      TableIdentifier nsTable,
      String[] dataFiles,
      boolean noCopy,
      boolean dryRun)
      throws IOException {
    if (dataFiles.length == 0) {
      // no work to be done
      return;
    }
    Table table = catalog.loadTable(nsTable);
    if (table.location() == null || table.location().isEmpty()) {
      // TODO: how do we even end up in this situation? pyiceberg?
      throw new UnsupportedOperationException(
          "Adding files to tables without location set is not currently supported");
    }
    Transaction tx = table.newTransaction();
    AppendFiles appendOp = tx.newAppend();
    Set<String> dataFilesSet = null;
    try (FileIO inputIO = Input.newIO(dataFiles[0], table);
        FileIO tableIO = table.io()) {
      // TODO: parallel
      var prefix = System.currentTimeMillis() + "-";
      for (String file : dataFiles) {
        InputFile inputFile = Input.newFile(file, catalog, inputIO == null ? tableIO : inputIO);
        ParquetMetadata metadata = Metadata.read(inputFile);

        Schema tableSchema = table.schema();

        MessageType type = metadata.getFileMetaData().getSchema();
        Schema fileSchema = ParquetSchemaUtil.convert(type); // nameMapping applied (when present)

        if (!sameSchema(table, fileSchema)) {
          throw new BadRequestException(
              String.format("%s's schema doesn't match table's schema", file));
        }

        // assuming datafiles can be anywhere when table.location() is empty
        var noCopyPossible = file.startsWith(table.location());
        // TODO: check before uploading anything
        if (noCopy && !noCopyPossible) {
          throw new IllegalArgumentException(
              file + " cannot be added to catalog without copy"); // TODO: explain
        }
        long dataFileSizeInBytes;
        var dataFile = replacePrefix(file, "s3a://", "s3://");
        if (!noCopy) {
          String name = Hash.sha256(file);
          // TODO: support custom format
          dataFile =
              String.format(
                  "%s/%s/%s",
                  table.location().replaceAll("/+$", ""), "data", prefix + name + ".parquet");
          OutputFile outputFile = tableIO.newOutputFile(replacePrefix(dataFile, "s3://", "s3a://"));
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
                  .createWriterFunc(GenericParquetWriter::buildWriter)
                  .schema(tableSchema);

          // file size may have changed due to different compression, etc.
          dataFileSizeInBytes = copy(readBuilder, writeBuilder);
        } else {
          if (dataFilesSet == null) {
            Snapshot snapshot = table.currentSnapshot();
            if (snapshot != null) {
              dataFilesSet =
                  StreamSupport.stream(snapshot.addedDataFiles(tableIO).spliterator(), false)
                      .map(ContentFile::location)
                      .collect(Collectors.toSet());
            } else {
              dataFilesSet = Set.of();
            }
          }
          if (dataFilesSet.contains(dataFile)) {
            throw new BadRequestException(
                String.format("%s is already part of the table", dataFile));
          }
          dataFileSizeInBytes = inputFile.getLength();
        }
        long recordCount =
            metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
        DataFile df =
            new DataFiles.Builder(table.spec())
                .withPath(dataFile)
                .withFormat("PARQUET")
                .withRecordCount(recordCount)
                .withFileSizeInBytes(dataFileSizeInBytes)
                // TODO: metrics
                .build();
        appendOp.appendFile(df);
      }
      appendOp.commit();
      if (!dryRun) {
        // TODO: log
        tx.commitTransaction();
      } else {
        logger.warn("Table.Transaction commit skipped (--dry-run)");
      }
    }
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
