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

import com.github.shyiko.skedule.Schedule;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceScheduler {
  private static final Logger logger = LoggerFactory.getLogger(MaintenanceScheduler.class);

  private final Catalog catalog;
  private final AtomicBoolean isMaintenanceMode = new AtomicBoolean(false);
  private final ScheduledExecutorService executor;
  private final Schedule schedule;
  private final Object taskLock = new Object();

  private ScheduledFuture<?> currentTask;
  private final Integer snapshotExpirationDays;
  private final Integer orphanFileExpirationDays;

  public MaintenanceScheduler(
      Catalog catalog, String schedule, int snapshotExpirationDays, int orphanFileExpirationDays) {
    this.catalog = catalog;
    this.executor = new ScheduledThreadPoolExecutor(1);
    ((ScheduledThreadPoolExecutor) executor).setRemoveOnCancelPolicy(true);
    this.schedule = Schedule.parse(schedule);
    this.snapshotExpirationDays = snapshotExpirationDays;
    this.orphanFileExpirationDays = orphanFileExpirationDays;
  }

  public void startScheduledMaintenance() {
    scheduleNextMaintenance();
  }

  public void stopScheduledMaintenance() {
    synchronized (taskLock) {
      if (currentTask != null) {
        currentTask.cancel(false);
      }
      executor.shutdown();
    }
  }

  private void scheduleNextMaintenance() {
    synchronized (taskLock) {
      if (currentTask != null) {
        currentTask.cancel(false);
      }

      ZonedDateTime now = ZonedDateTime.now();
      ZonedDateTime next = schedule.next(now);

      long delay = next.toEpochSecond() - now.toEpochSecond();
      currentTask =
          executor.schedule(
              () -> {
                performMaintenance();
                scheduleNextMaintenance(); // Schedule next run
              },
              delay,
              TimeUnit.SECONDS);

      logger.info("Next maintenance scheduled for: {}", next);
    }
  }

  public void performMaintenance() {
    if (isMaintenanceMode.get()) {
      logger.info("Skipping maintenance task as system is already in maintenance mode");
      return;
    }

    try {
      logger.info("Starting scheduled maintenance task");
      setMaintenanceMode(true);

      if (catalog != null) {
        logger.info("Performing maintenance on catalog: {}", catalog.name());
        List<Namespace> namespaces;
        if (catalog instanceof SupportsNamespaces nsCatalog) {
          namespaces = nsCatalog.listNamespaces();
          for (Namespace ns : namespaces) {
            logger.debug("Namespace: " + ns);
          }
        } else {
          logger.error("Catalog does not support namespace operations.");
          return;
        }

        for (Namespace namespace : namespaces) {
          List<TableIdentifier> tables = catalog.listTables(namespace);
          for (TableIdentifier tableIdent : tables) {
            long olderThanMillis =
                System.currentTimeMillis() - TimeUnit.DAYS.toMillis(snapshotExpirationDays);

            // This throws a Location does not exist error.
            Table table = null;
            try {
              table = catalog.loadTable(tableIdent);
            } catch (NotFoundException ne) {
              logger.warn("Table {} location not found, skipping maintenance", tableIdent, ne);
              continue;
            }
            // Check if table has any snapshots before performing maintenance
            if (table.currentSnapshot() == null) {
              logger.warn("Table {} has no snapshots, skipping maintenance", tableIdent);
              continue;
            }

            table.rewriteManifests().rewriteIf(manifest -> true).commit();
            table.expireSnapshots().expireOlderThan(olderThanMillis).commit();

            if (orphanFileExpirationDays == 0) {
              logger.info("Skipping orphan file removal for table {}", tableIdent);
              continue;
            }
            long orphanCutOffMillis =
                System.currentTimeMillis() - TimeUnit.DAYS.toMillis(orphanFileExpirationDays);
            // Remove orphans only for S3-based tables
            String tableLocation = table.location();
            if (tableLocation != null && tableLocation.startsWith("s3://")) {
              OrphanFileScanner orphanFileScanner = new OrphanFileScanner(table);
              try {
                orphanFileScanner.removeOrphanedFiles(orphanCutOffMillis, false);
              } catch (Exception e) {
                logger.warn("Failed to remove orphan files for table {}", tableIdent, e);
              }
            } else {
              logger.debug(
                  "Skipping orphan file removal for non-S3 table: {} (location: {})",
                  tableIdent,
                  tableLocation);
            }
          }
        }
        logger.info("Maintenance operations completed for catalog: {}", catalog.name());
      } else {
        logger.warn("No catalog available for maintenance operations");
      }

      logger.info("Scheduled maintenance task completed successfully");
    } catch (Exception e) {
      logger.error("Error during scheduled maintenance task", e);
    } finally {
      setMaintenanceMode(false);
    }
  }

  private void setMaintenanceMode(boolean enabled) {
    isMaintenanceMode.set(enabled);
    logger.info("Maintenance mode {}", enabled ? "enabled" : "disabled");
  }
}
