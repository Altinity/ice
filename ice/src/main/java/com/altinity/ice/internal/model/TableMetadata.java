package com.altinity.ice.internal.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;
import lombok.Data;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class TableMetadata {
  private String kind = "Table";
  private Metadata metadata;
  private TableData data;

  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public void setData(TableData data) {
    this.data = data;
  }

  @Data
  public static class Metadata {
    private String id;
  }

  @Data
  public static class TableData {
    private String schema_raw;
    private String partition_spec_raw;
    private String sort_order_raw;
    private Map<String, String> properties;
    private String location;
    private SnapshotInfo current_snapshot;
    private List<MetricsInfo> metrics;
  }

  @Data
  public static class SnapshotInfo {
    private long sequence_number;
    private long id;
    private Long parent_id;
    private long timestamp;
    private String timestamp_iso;
    private String timestamp_iso_local;
    private String operation;
    private Map<String, String> summary;
    private String location;
  }

  @Data
  public static class MetricsInfo {
    private String file;
    private long record_count;
    private List<ColumnMetrics> columns;
  }

  @Data
  public static class ColumnMetrics {
    private String name;
    private Long value_count;
    private Long null_count;
    private String lower_bound;
    private String upper_bound;
  }
}
