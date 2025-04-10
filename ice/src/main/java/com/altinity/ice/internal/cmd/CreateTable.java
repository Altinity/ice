package com.altinity.ice.internal.cmd;

import com.altinity.ice.internal.io.InputFiles;
import com.altinity.ice.internal.parquet.Metadata;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.parquet.schema.MessageType;

public final class CreateTable {

  private CreateTable() {}

  public static void run(
      RESTCatalog catalog, TableIdentifier nsTable, String schemaFile, boolean ignoreAlreadyExists)
      throws IOException {
    FileIO io = null;
    if (schemaFile.startsWith("s3://")) {
      io = new S3FileIO();
      // FIXME: this may not be what user wants
      io.initialize(
          Map.of(
              "client.credentials-provider",
              "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider"));
    }
    try {
      InputFile inputFile =
          InputFiles.get(schemaFile, catalog.properties().get("ice.http.cache"), io); // TODO: move
      MessageType type = Metadata.read(inputFile).getFileMetaData().getSchema();
      Schema fileSchema = ParquetSchemaUtil.convert(type);
      // TODO: location
      // if we don't set location, it's automatically set to $warehouse/$namespace/$table
      try {
        catalog.createTable(nsTable, fileSchema);
      } catch (AlreadyExistsException e) {
        if (ignoreAlreadyExists) {
          return;
        }
        throw e;
      }
    } finally {
      if (io != null) {
        io.close();
      }
    }
  }
}
