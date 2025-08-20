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

import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Table;

public record ManifestCompaction(boolean dryRun) implements MaintenanceJob {

  @Override
  public void perform(Table table) {
    // FIXME: generates new files even where there are no changes
    RewriteManifests op = table.rewriteManifests();
    if (dryRun) {
      op.apply();
    } else {
      op.commit();
    }
  }
}
