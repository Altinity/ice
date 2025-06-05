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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

public final class Scan {

  private Scan() {}

  public static void run(RESTCatalog catalog, TableIdentifier tableId, int limit, boolean json)
      throws IOException {

    org.apache.iceberg.Table table = catalog.loadTable(tableId);
    Schema tableSchema = table.schema();
    List<Map<String, Object>> records = new ArrayList<>();
    TableScan tableScan = table.newScan();
    try (FileIO tableIO = table.io();
        CloseableIterable<FileScanTask> scanTasks = tableScan.planFiles()) {
      scan:
      for (FileScanTask task : scanTasks) {
        InputFile input = tableIO.newInputFile(task.file().location());
        try (CloseableIterable<org.apache.iceberg.data.Record> parquetReader =
            Parquet.read(input)
                .createReaderFunc(s -> GenericParquetReaders.buildReader(tableSchema, s))
                .project(tableSchema)
                .build()) {
          for (org.apache.iceberg.data.Record record : parquetReader) {
            if (limit-- <= 0) break scan;
            records.add(convertRecord(record, record.struct()));
          }
        }
      }
    }
    ObjectMapper mapper = json ? new ObjectMapper() : new ObjectMapper(new YAMLFactory());
    String output = mapper.writeValueAsString(records);
    System.out.println(output);
  }

  private static Map<String, Object> convertRecord(Record record, Types.StructType structType) {
    Map<String, Object> result = new LinkedHashMap<>();
    List<Types.NestedField> fields = structType.fields();
    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField field = fields.get(i);
      Object value = record.get(i);
      result.put(field.name(), convertValue(value, field.type()));
    }
    return result;
  }

  private static Object convertValue(Object value, org.apache.iceberg.types.Type type) {
    if (value == null) {
      return null;
    }
    switch (type.typeId()) {
      case STRUCT:
        return convertRecord((Record) value, type.asStructType());
      case LIST:
        var list = (List<?>) value;
        return list.stream()
            .map(item -> convertValue(item, type.asListType().elementType()))
            .toList();
      case MAP:
        var map = (Map<?, ?>) value;
        Map<Object, Object> convertedMap = new LinkedHashMap<>();
        type.asMapType().keyType();
        for (var entry : map.entrySet()) {
          Object k = convertValue(entry.getKey(), type.asMapType().keyType());
          Object v = convertValue(entry.getValue(), type.asMapType().valueType());
          convertedMap.put(k, v);
        }
        return convertedMap;
      default:
        return value; // primitive
    }
  }
}
