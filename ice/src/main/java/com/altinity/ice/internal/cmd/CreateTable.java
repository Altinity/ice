package com.altinity.ice.internal.cmd;

import com.altinity.ice.internal.io.Input;
import com.altinity.ice.internal.parquet.Metadata;
import java.io.IOException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.parquet.schema.MessageType;

public final class CreateTable {

  private CreateTable() {}

  public static void run(
      RESTCatalog catalog,
      TableIdentifier nsTable,
      String schemaFile,
      String location,
      boolean ignoreAlreadyExists)
      throws IOException {
    try (var inputIO = Input.newIO(schemaFile, null)) {
      InputFile inputFile = Input.newFile(schemaFile, catalog, inputIO);
      MessageType type = Metadata.read(inputFile).getFileMetaData().getSchema();
      Schema fileSchema = ParquetSchemaUtil.convert(type);
      try {
        // if we don't set location, it's automatically set to $warehouse/$namespace/$table
        catalog.createTable(nsTable, fileSchema, PartitionSpec.unpartitioned(), location, null);
      } catch (AlreadyExistsException e) {
        if (ignoreAlreadyExists) {
          return;
        }
        throw e;
      }
    }
  }
}
