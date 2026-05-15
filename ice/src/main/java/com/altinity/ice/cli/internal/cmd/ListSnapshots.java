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

import com.altinity.ice.cli.internal.util.TreePrinter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    List<DescribeMetadata.SnapshotInfo> rows =
        DescribeMetadata.extractSnapshots(table.snapshots(), currentSnapshotId);

    rows.sort(Comparator.comparingLong(DescribeMetadata.SnapshotInfo::timestampMillis));

    if (limit > 0 && rows.size() > limit) {
      rows = new ArrayList<>(rows.subList(rows.size() - limit, rows.size()));
    }

    if (json) {
      var result = new Result(tableId.toString(), currentSnapshotId, rows);
      ObjectMapper mapper = new ObjectMapper();
      mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      System.out.println(mapper.writeValueAsString(result));
      return;
    }

    printTree(tableId.toString(), currentSnapshotId, rows);
  }

  private static void printTree(
      String tableName, Long currentSnapshotId, List<DescribeMetadata.SnapshotInfo> rows)
      throws IOException {
    StringBuilder rootLabel = new StringBuilder();
    rootLabel.append("table: ").append(tableName);
    if (currentSnapshotId != null) {
      rootLabel.append("\ncurrentSnapshotId: ").append(currentSnapshotId);
    }

    if (rows.isEmpty()) {
      TreePrinter.print(new TreePrinter.Node(rootLabel.toString(), List.of()));
      System.out.println("(no snapshots)");
      return;
    }

    ObjectMapper yamlMapper =
        new ObjectMapper(
            new YAMLFactory()
                .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
    yamlMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

    Set<Long> presentIds = new HashSet<>(rows.size());
    for (DescribeMetadata.SnapshotInfo info : rows) {
      presentIds.add(info.snapshotId());
    }

    Map<Long, List<DescribeMetadata.SnapshotInfo>> childrenByParent = new HashMap<>();
    List<DescribeMetadata.SnapshotInfo> roots = new ArrayList<>();
    for (DescribeMetadata.SnapshotInfo info : rows) {
      Long parentId = info.parentId();
      if (parentId == null || !presentIds.contains(parentId)) {
        roots.add(info);
      } else {
        childrenByParent.computeIfAbsent(parentId, k -> new ArrayList<>()).add(info);
      }
    }

    List<TreePrinter.Node> rootChildren = new ArrayList<>(roots.size());
    for (DescribeMetadata.SnapshotInfo root : roots) {
      rootChildren.add(buildNode(root, childrenByParent, yamlMapper));
    }

    TreePrinter.print(new TreePrinter.Node(rootLabel.toString(), rootChildren));
  }

  private static TreePrinter.Node buildNode(
      DescribeMetadata.SnapshotInfo info,
      Map<Long, List<DescribeMetadata.SnapshotInfo>> childrenByParent,
      ObjectMapper yamlMapper)
      throws IOException {
    List<DescribeMetadata.SnapshotInfo> children =
        childrenByParent.getOrDefault(info.snapshotId(), List.of());
    List<TreePrinter.Node> childNodes = new ArrayList<>(children.size());

    for (DescribeMetadata.SnapshotInfo child : children) {
      childNodes.add(buildNode(child, childrenByParent, yamlMapper));
    }
    return new TreePrinter.Node(yamlMapper.writeValueAsString(info), childNodes);
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  record Result(
      String table, Long currentSnapshotId, List<DescribeMetadata.SnapshotInfo> snapshots) {}
}
