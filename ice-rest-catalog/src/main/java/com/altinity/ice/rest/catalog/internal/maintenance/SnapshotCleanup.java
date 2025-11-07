/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.maintenance;

import java.util.concurrent.TimeUnit;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record SnapshotCleanup(long maxSnapshotAgeHours, int minSnapshotsToKeep, boolean dryRun)
    implements MaintenanceJob {

  private static final Logger logger = LoggerFactory.getLogger(SnapshotCleanup.class);

  @Override
  public void perform(Table table) {
    if (table.currentSnapshot() == null) {
      logger.warn("Table {} has no snapshots, skipping maintenance", table.name());
      return;
    }

    ExpireSnapshots op = table.expireSnapshots();

    long ttlInMs = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(maxSnapshotAgeHours);
    if (ttlInMs > 0) {
      op.expireOlderThan(ttlInMs);
    }
    if (minSnapshotsToKeep > 0) {
      op.retainLast(minSnapshotsToKeep);
    }
    if (dryRun) {
      op.apply();
    } else {
      op.commit();
    }
  }
}
