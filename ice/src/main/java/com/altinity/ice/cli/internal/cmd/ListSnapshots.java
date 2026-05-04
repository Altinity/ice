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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;

public final class ListSnapshots {

  private ListSnapshots() {}

  public static void run(RESTCatalog catalog, TableIdentifier tableId, boolean json, int limit)
      throws IOException {
    Table table = catalog.loadTable(tableId);
    Long currentSnapshotId =
        table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null;

    List<SnapshotInfo> rows = new ArrayList<>();
    for (Snapshot snapshot : table.snapshots()) {
      Long parentId = snapshot.parentId();
      rows.add(
          new SnapshotInfo(
              snapshot.snapshotId(),
              parentId,
              snapshot.sequenceNumber(),
              snapshot.timestampMillis(),
              Instant.ofEpochMilli(snapshot.timestampMillis()).toString(),
              snapshot.operation(),
              currentSnapshotId != null && snapshot.snapshotId() == currentSnapshotId,
              snapshot.summary(),
              snapshot.manifestListLocation()));
    }

    rows.sort(Comparator.comparingLong(SnapshotInfo::timestampMillis));

    if (limit > 0 && rows.size() > limit) {
      rows = new ArrayList<>(rows.subList(rows.size() - limit, rows.size()));
    }

    var result = new Result(tableId.toString(), currentSnapshotId, rows);
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
  record Result(String table, Long currentSnapshotId, List<SnapshotInfo> snapshots) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  record SnapshotInfo(
      long snapshotId,
      Long parentId,
      long sequenceNumber,
      long timestampMillis,
      String timestamp,
      String operation,
      boolean current,
      Map<String, String> summary,
      String manifestListLocation) {}
}
