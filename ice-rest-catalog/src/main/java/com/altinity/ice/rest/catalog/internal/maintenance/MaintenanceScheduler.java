package com.altinity.ice.rest.catalog.internal.maintenance;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceScheduler {
  private static final Logger logger = LoggerFactory.getLogger(MaintenanceScheduler.class);

  private final ScheduledExecutorService scheduler;
  private final AtomicBoolean isMaintenanceMode = new AtomicBoolean(false);
  private long maintenanceInterval = 24; // Default 24 hours
  private TimeUnit maintenanceTimeUnit = TimeUnit.HOURS;

  public MaintenanceScheduler() {
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  public void startScheduledMaintenance() {
    logger.info(
        "Starting scheduled maintenance with interval: {} {}",
        maintenanceInterval,
        maintenanceTimeUnit);
    scheduler.scheduleAtFixedRate(
        this::performMaintenance, maintenanceInterval, maintenanceInterval, maintenanceTimeUnit);
  }

  public void stopScheduledMaintenance() {
    logger.info("Stopping scheduled maintenance");
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public void setMaintenanceSchedule(long interval, TimeUnit timeUnit) {
    this.maintenanceInterval = interval;
    this.maintenanceTimeUnit = timeUnit;
    logger.info("Maintenance schedule updated to: {} {}", interval, timeUnit);

    // Restart the scheduler with new settings
    stopScheduledMaintenance();
    startScheduledMaintenance();
  }

  public Map<String, Object> getMaintenanceSchedule() {
    Map<String, Object> schedule = new HashMap<>();
    schedule.put("interval", maintenanceInterval);
    schedule.put("timeUnit", maintenanceTimeUnit.name());
    return schedule;
  }

  public boolean isInMaintenanceMode() {
    return isMaintenanceMode.get();
  }

  public void setMaintenanceMode(boolean enabled) {
    isMaintenanceMode.set(enabled);
    logger.info("Maintenance mode {}", enabled ? "enabled" : "disabled");
  }

  private void performMaintenance() {
    if (isMaintenanceMode.get()) {
      logger.info("Skipping maintenance task as system is already in maintenance mode");
      return;
    }

    try {
      logger.info("Starting scheduled maintenance task");
      setMaintenanceMode(true);

      // Perform maintenance tasks here
      // For example: cleanup old files, optimize tables, etc.

      logger.info("Scheduled maintenance task completed successfully");
    } catch (Exception e) {
      logger.error("Error during scheduled maintenance task", e);
    } finally {
      setMaintenanceMode(false);
    }
  }
}
