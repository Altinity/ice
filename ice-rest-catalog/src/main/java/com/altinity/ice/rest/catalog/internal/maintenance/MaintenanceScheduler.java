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

import com.altinity.ice.rest.catalog.internal.metrics.MaintenanceMetrics;
import com.github.shyiko.skedule.Schedule;
import java.time.ZonedDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceScheduler {
  private static final Logger logger = LoggerFactory.getLogger(MaintenanceScheduler.class);

  private final MaintenanceRunner maintenanceRunner;
  private final AtomicBoolean isMaintenanceMode = new AtomicBoolean(false);
  private final ScheduledExecutorService executor;
  private final Schedule schedule;
  private final Object taskLock = new Object();

  private ScheduledFuture<?> currentTask;

  public MaintenanceScheduler(String schedule, MaintenanceRunner maintenanceRunner) {
    this.maintenanceRunner = maintenanceRunner;
    this.executor = new ScheduledThreadPoolExecutor(1);
    ((ScheduledThreadPoolExecutor) executor).setRemoveOnCancelPolicy(true);
    this.schedule = Schedule.parse(schedule);
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
    MaintenanceMetrics metrics = MaintenanceMetrics.getInstance();

    if (isMaintenanceMode.get()) {
      logger.info("Skipping maintenance task as system is already in maintenance mode");
      metrics.recordMaintenanceSkipped();
      return;
    }

    long startTime = System.nanoTime();
    boolean success = false;

    try {
      logger.info("Starting scheduled maintenance task");
      setMaintenanceMode(true);
      metrics.recordMaintenanceStarted();

      maintenanceRunner.run();

      logger.info("Scheduled maintenance task completed successfully");
      success = true;
    } catch (Exception e) {
      logger.error("Error during scheduled maintenance task", e);
    } finally {
      setMaintenanceMode(false);
      double durationSecs = (System.nanoTime() - startTime) / 1_000_000_000.0;
      metrics.recordMaintenanceCompleted(success, durationSecs);
    }
  }

  private void setMaintenanceMode(boolean enabled) {
    isMaintenanceMode.set(enabled);
    logger.info("Maintenance mode {}", enabled ? "enabled" : "disabled");
  }
}
