package com.altinity.ice.cmd;

import com.altinity.ice.crypto.Hash;
import com.altinity.ice.io.InputFiles;
import com.altinity.ice.parquet.Metadata;
import com.altinity.ice.parquet.ParquetAvroWriterWithINT96AsFIXEDSupport;
import java.io.IOException;
import java.util.*;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.*;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.Type;

public final class Insert {

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
      throw new UnsupportedOperationException(
          "adding files to tables without location set is not currently supported");
    }
    Transaction transaction = table.newTransaction();
    AppendFiles appendFiles = transaction.newAppend();
    FileIO inputIO = null;
    for (String file : dataFiles) {
      if (file.startsWith("s3://")) {
        // FIXME
        inputIO = new S3FileIO();
        inputIO.initialize(
            Map.of(
                "client.credentials-provider",
                "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider")); // TODO:
        // remove
        break;
      }
    }
    try (FileIO io = table.io()) {
      // TODO: parallel
      var prefix = System.currentTimeMillis() + "-";
      for (String file : dataFiles) {
        // TODO: InputFiles.get
        InputFile inputFile =
            InputFiles.get(file, catalog.properties().get("ice.http.cache"), inputIO);
        ParquetMetadata metadata = Metadata.read(inputFile);
        long fileSizeInBytes = inputFile.getLength();
        var noCopyPossible = file.startsWith(table.location());
        // TODO: check before uploading anything
        if (noCopy && !noCopyPossible) {
          throw new IllegalArgumentException(file + " cannot be added to catalog without copy");
        }
        if (!noCopy) {
          // warehouseLocation + "/" + namespace + "/" + tableName
          String name = Hash.sha256(file);
          // TODO: support custom format
          var pathInWarehouse =
              String.join(
                  "/", table.location().replaceAll("/+$", ""), "data", prefix + name + ".parquet");

          // TODO: check schemas match
          //                    System.out.println(table.location());
          //                    System.out.println(replacePrefix(pathInWarehouse, "s3://",
          // "s3a://"));
          OutputFile outputFile =
              io.newOutputFile(replacePrefix(pathInWarehouse, "s3://", "s3a://"));
          // TODO: support below with force flag
          /*
                                  // TODO: compression, etc. might be different
                                  try (var d = outputFile.create()) {
                                      try (var s = inputFile.newStream()) {
                                          s.transferTo(d);
                                      }
                                  }
          */

          //          var avro =
          //
          // metadata.getFileMetaData().getKeyValueMetaData().containsKey("parquet.avro.schema");
          //          System.out.println(avro);
          //          if (true) {
          //            throw new RuntimeException("yeey");
          //          }
          // FIXME: project to the schema of the table?
          Parquet.ReadBuilder readBuilder =
              Parquet.read(inputFile)
                  // https://github.com/apache/parquet-java/tree/master/parquet-avro
                  .project(table.schema());
          // TODO: reuseContainers?

          if (true) {
            readBuilder.createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema));
          } else {
            readBuilder.set(
                AvroReadSupport.READ_INT96_AS_FIXED, "true"); // "parquet.avro.readInt96AsFixed"
          }

          Set<String> int96fields = new HashSet<>();
          traverseSchema(metadata.getFileMetaData().getSchema(), "", int96fields);
          if (int96fields.isEmpty()) {
            int96fields = null;
          }
          try (CloseableIterable<Record> parquetReader = readBuilder.build()) {
            Parquet.WriteBuilder writeBuilder =
                Parquet.write(outputFile)
                    //                  .createWriterFunc(GenericParquetWriter::buildWriter)
                    .schema(table.schema()); // TODO: forTable?
            if (true) {
              writeBuilder.createWriterFunc(GenericParquetWriter::buildWriter);
            } else {
              writeBuilder.createWriterFunc(
                  new ParquetAvroWriterWithINT96AsFIXEDSupport(int96fields)
                      ::buildWriter); // TODO: check reused
            }
            FileAppender<Record> writer = null;
            try {
              writer = writeBuilder.build();
              writer.addAll(parquetReader);
            } finally {
              if (writer != null) {
                writer.close();
              }
            }
            fileSizeInBytes = writer.length();
          }

          file = pathInWarehouse; // TODO: refactor
        }
        long recordCount =
            metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
        DataFile dataFile =
            new DataFiles.Builder(table.spec())
                /*
                * Received exception:
                Code: 36. DB::Exception: Expected to find 'test/nyc/taxis' in data path: 's3a://sasquatch/yellow_tripdata_2024-12.parquet'. (BAD_ARGUMENTS)

                :) select * from iceberg_demo.`nyc.taxis` limit 10 FORMAT CSVWithNamesAndTypes
                */
                .withPath(
                    replacePrefix(file, "s3a://", "s3://")) // TODO: replace s3a:// with s3://?
                .withFormat("PARQUET")
                .withRecordCount(recordCount)
                .withFileSizeInBytes(fileSizeInBytes)
                .build();
        appendFiles.appendFile(dataFile);
      }
      appendFiles.commit();
      if (!dryRun) {
        transaction.commitTransaction();
      }
    } finally {
      if (inputIO != null) {
        inputIO.close();
      }
    }
  }

  private static void traverseSchema(
      org.apache.parquet.schema.GroupType scheme, String path, Set<String> aggr) {
    for (Type field : scheme.getFields()) {
      if (field.isPrimitive()) {
        if ("INT96".equals(field.asPrimitiveType().getPrimitiveTypeName().name())) {
          aggr.add(path + field.getName());
        }
      } else {
        traverseSchema(field.asGroupType(), field.getName() + ".", aggr);
      }
    }
  }

  private static String replacePrefix(String text, String from, String to) {
    if (text.startsWith(from)) {
      return to + text.substring(from.length());
    }
    return text;
  }
}
