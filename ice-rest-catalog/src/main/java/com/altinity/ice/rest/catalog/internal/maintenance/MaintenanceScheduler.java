package com.altinity.ice.rest.catalog.internal.maintenance;

import com.altinity.ice.rest.catalog.internal.config.Config;
import com.github.shyiko.skedule.Schedule;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceScheduler {
  private static final Logger logger = LoggerFactory.getLogger(MaintenanceScheduler.class);
  private static final int DEFAULT_EXPIRATION_DAYS = 30;

  private final Catalog catalog;
  private final AtomicBoolean isMaintenanceMode = new AtomicBoolean(false);
  private final ScheduledExecutorService executor;
  private final Map<String, String> config;
  private ScheduledFuture<?> currentTask;
  private Schedule schedule;

  public MaintenanceScheduler(Catalog catalog, Map<String, String> config) {
    this.catalog = catalog;
    this.config = config;
    this.executor = new ScheduledThreadPoolExecutor(1);
    ((ScheduledThreadPoolExecutor) executor).setRemoveOnCancelPolicy(true);
    // Default schedule: every day at midnight
    this.schedule = Schedule.at(LocalTime.MIDNIGHT).everyDay();
  }

  public void startScheduledMaintenance() {
    scheduleNextMaintenance();
  }

  public void stopScheduledMaintenance() {
    if (currentTask != null) {
      currentTask.cancel(false);
    }
    executor.shutdown();
  }

  private void scheduleNextMaintenance() {
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

  public void setMaintenanceSchedule(String scheduleExpression) {
    this.schedule = Schedule.parse(scheduleExpression);
    scheduleNextMaintenance();
  }

  public void setMaintenanceMode(boolean enabled) {
    isMaintenanceMode.set(enabled);
    logger.info("Maintenance mode {}", enabled ? "enabled" : "disabled");
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
        if (catalog instanceof SupportsNamespaces) {
          SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
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
            int expirationDays = DEFAULT_EXPIRATION_DAYS;
            String configuredDays = config.get(Config.OPTION_SNAPSHOT_EXPIRATION_DAYS);
            if (configuredDays != null) {
              try {
                expirationDays = Integer.parseInt(configuredDays);
                logger.debug("Using configured snapshot expiration days: {}", expirationDays);
              } catch (NumberFormatException e) {
                logger.warn(
                    "Invalid value for {}: {}. Using default of {} days",
                    Config.OPTION_SNAPSHOT_EXPIRATION_DAYS,
                    configuredDays,
                    DEFAULT_EXPIRATION_DAYS);
              }
            }
            long olderThanMillis =
                System.currentTimeMillis() - TimeUnit.DAYS.toMillis(expirationDays);
            Table table = catalog.loadTable(tableIdent);
            table.expireSnapshots().expireOlderThan(olderThanMillis).commit();
            table.rewriteManifests().rewriteIf(manifest -> true).commit();
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
}
