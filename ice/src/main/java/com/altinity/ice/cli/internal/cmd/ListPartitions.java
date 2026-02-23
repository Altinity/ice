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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;

public final class ListPartitions {

  private ListPartitions() {}

  public static void run(RESTCatalog catalog, TableIdentifier tableId, boolean json)
    throws IOException {
    org.apache.iceberg.Table table = catalog.loadTable(tableId);
    PartitionSpec spec = table.spec();

    if (!spec.isPartitioned()) {
      var result = new Result(tableId.toString(), List.of(), List.of());
      output(result, json);
      return;
    }

    List<PartitionFieldInfo> partitionSpec = new ArrayList<>();
    for (PartitionField field : spec.fields()) {
      String sourceColumn = table.schema().findField(field.sourceId()).name();
      partitionSpec.add(
        new PartitionFieldInfo(sourceColumn, field.name(), field.transform().toString()));
    }

    TreeSet<String> partitionPaths = new TreeSet<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String path = spec.partitionToPath(task.file().partition());
        partitionPaths.add(path);
      }
    }

    var result =
      new Result(tableId.toString(), partitionSpec, new ArrayList<>(partitionPaths));
    output(result, json);
  }

  private static void output(Result result, boolean json) throws IOException {
    ObjectMapper mapper =
      json
        ? new ObjectMapper()
        : new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    System.out.println(mapper.writeValueAsString(result));
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  record Result(String table, List<PartitionFieldInfo> partitionSpec, List<String> partitions) {}

  record PartitionFieldInfo(String sourceColumn, String name, String transform) {}

}
