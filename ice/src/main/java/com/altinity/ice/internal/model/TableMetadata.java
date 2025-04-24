package com.altinity.ice.internal.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TableMetadata(String kind, Metadata metadata, TableData data) {
  public TableMetadata {
    if (kind == null) {
      kind = "Table";
    }
  }

  public record Metadata(String id) {}

  public record TableData(
      String schema_raw,
      String partition_spec_raw,
      String sort_order_raw,
      Map<String, String> properties,
      String location,
      SnapshotInfo current_snapshot,
      List<MetricsInfo> metrics) {}

  public record SnapshotInfo(
      long sequence_number,
      long id,
      Long parent_id,
      long timestamp,
      String timestamp_iso,
      String timestamp_iso_local,
      String operation,
      Map<String, String> summary,
      String location) {}

  public record MetricsInfo(String file, long record_count, List<ColumnMetrics> columns) {}

  public record ColumnMetrics(
      String name, Long value_count, Long null_count, String lower_bound, String upper_bound) {}
}
