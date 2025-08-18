/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.config;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.apache.iceberg.TableProperties;

public record MaintenanceConfig(
    @JsonPropertyDescription(
            "MANIFEST_COMPACTION, DATA_COMPACTION, SNAPSHOT_CLEANUP, ORPHAN_CLEANUP")
        Job[] jobs,
    @JsonPropertyDescription("Max snapshot age in hours (5 days by default).")
        int maxSnapshotAgeHours,
    @JsonPropertyDescription("Minimum number of snapshots to keep (1 by default).")
        int minSnapshotsToKeep,
    @JsonPropertyDescription("The target file size for the table in MB (64 min; 512 by default).")
        int targetFileSizeMB,
    @JsonPropertyDescription(
            "The minimum number of files to be compacted if a table partition size is smaller than the target file size (5 by default).")
        int minInputFiles,
    @JsonPropertyDescription(
            "How long to wait before considering file for data compaction (3 hours by default; -1 to disable).")
        int dataCompactionCandidateMinAgeInHours,
    @JsonPropertyDescription(
            "The number of days to retain orphan files before deleting them (3 by default; -1 to disable).")
        int orphanFileRetentionPeriodInDays,
    @JsonPropertyDescription("Orphan whitelist (defaults to */metadata/*, */data/*)")
        String[] orphanWhitelist,
    @JsonPropertyDescription("Print changes without applying them") boolean dryRun) {

  public enum Job {
    MANIFEST_COMPACTION,
    DATA_COMPACTION,
    SNAPSHOT_CLEANUP,
    ORPHAN_CLEANUP;
  }

  public MaintenanceConfig(
      Job[] jobs,
      int maxSnapshotAgeHours,
      int minSnapshotsToKeep,
      int targetFileSizeMB,
      int minInputFiles,
      int dataCompactionCandidateMinAgeInHours,
      int orphanFileRetentionPeriodInDays,
      String[] orphanWhitelist,
      boolean dryRun) {
    this.jobs = jobs;
    this.maxSnapshotAgeHours =
        maxSnapshotAgeHours > 0
            ? maxSnapshotAgeHours
            : (int) (TableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT / 1000 / 60 / 60); // 5 days
    this.minSnapshotsToKeep =
        minSnapshotsToKeep > 0
            ? minSnapshotsToKeep
            : TableProperties.MIN_SNAPSHOTS_TO_KEEP_DEFAULT; // 1
    this.targetFileSizeMB = targetFileSizeMB >= 64 ? targetFileSizeMB : 512;
    this.minInputFiles = minInputFiles > 0 ? minInputFiles : 5;
    this.dataCompactionCandidateMinAgeInHours =
        dataCompactionCandidateMinAgeInHours > 0
            ? dataCompactionCandidateMinAgeInHours
            : dataCompactionCandidateMinAgeInHours == -1 ? 0 : 3;
    this.orphanFileRetentionPeriodInDays =
        orphanFileRetentionPeriodInDays > 0
            ? orphanFileRetentionPeriodInDays
            : orphanFileRetentionPeriodInDays == -1 ? 0 : 3;
    this.orphanWhitelist =
        orphanWhitelist == null ? new String[] {"*/metadata/*", "*/data/*"} : orphanWhitelist;
    this.dryRun = dryRun;
  }
}
