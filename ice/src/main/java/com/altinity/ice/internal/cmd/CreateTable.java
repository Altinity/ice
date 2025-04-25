package com.altinity.ice.internal.cmd;

import com.altinity.ice.internal.aws.S3;
import com.altinity.ice.internal.io.Input;
import com.altinity.ice.internal.parquet.Metadata;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.parquet.schema.MessageType;
import software.amazon.awssdk.services.s3.S3Client;
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
      List<String> partitionColumns,
      List<String> sortColumns)
      throws IOException {
    Lazy<S3Client> s3ClientLazy = new Lazy<>(() -> S3.newClient(s3NoSignRequest));

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
      MessageType type = Metadata.read(inputFile).getFileMetaData().getSchema();
      Schema fileSchema = ParquetSchemaUtil.convert(type);
      try {
        Map<String, String> props = null;
        if (!ParquetSchemaUtil.hasIds(type)) {
          // force name-based resolution instead of position-based resolution
          NameMapping mapping = MappingUtil.create(fileSchema);
          String mappingJson = NameMappingParser.toJson(mapping);
          props = Map.of(TableProperties.DEFAULT_NAME_MAPPING, mappingJson);
        }

        // Create partition spec based on provided partition columns
        final PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(fileSchema);
        if (partitionColumns != null && !partitionColumns.isEmpty()) {
          for (String column : partitionColumns) {
            partitionSpecBuilder.identity(column);
          }
        }
        final PartitionSpec partitionSpec = partitionSpecBuilder.build();

        // Create sort order based on provided sort columns (z-order)
        SortOrder sortOrder = null;
        if (sortColumns != null && !sortColumns.isEmpty()) {
          SortOrder.Builder sortOrderBuilder = SortOrder.builderFor(fileSchema);
          for (String column : sortColumns) {
            sortOrderBuilder.asc(column);
          }
          sortOrder = sortOrderBuilder.build();
        }

        // if we don't set location, it's automatically set to $warehouse/$namespace/$table
        var table = catalog.createTable(nsTable, fileSchema, partitionSpec, location, props);
        // Apply the sort order to the table
        if (sortOrder != null) {
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
      } catch (AlreadyExistsException e) {
        if (ignoreAlreadyExists) {
          return;
        }
        throw e;
      }
    }
  }
}
