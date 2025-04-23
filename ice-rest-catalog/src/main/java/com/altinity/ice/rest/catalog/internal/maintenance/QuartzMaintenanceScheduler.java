package com.altinity.ice.rest.catalog.internal.maintenance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuartzMaintenanceScheduler {
  private static final Logger logger = LoggerFactory.getLogger(QuartzMaintenanceScheduler.class);
  private static final String MAINTENANCE_JOB_NAME = "MaintenanceJob";
  private static final String MAINTENANCE_GROUP_NAME = "MaintenanceGroup";
  private static final String MAINTENANCE_TRIGGER_NAME = "MaintenanceTrigger";

  private final Scheduler scheduler;
  private final AtomicBoolean isMaintenanceMode = new AtomicBoolean(false);
  private String cronExpression = "0 0 0 * * ?"; // Default: Run at midnight every day
  private final Catalog catalog;

  public QuartzMaintenanceScheduler(Catalog catalog) throws SchedulerException {
    this.catalog = catalog;
    SchedulerFactory schedulerFactory = new StdSchedulerFactory();
    this.scheduler = schedulerFactory.getScheduler();
  }

  public QuartzMaintenanceScheduler(Catalog catalog, Properties quartzProperties)
      throws SchedulerException {
    this.catalog = catalog;
    SchedulerFactory schedulerFactory = new StdSchedulerFactory(quartzProperties);
    this.scheduler = schedulerFactory.getScheduler();
  }

  public void startScheduledMaintenance() throws SchedulerException {
    logger.info("Starting scheduled maintenance with cron expression: {}", cronExpression);

    // Create the job
    JobDetail job =
        JobBuilder.newJob(MaintenanceJob.class)
            .withIdentity(MAINTENANCE_JOB_NAME, MAINTENANCE_GROUP_NAME)
            .build();

    // Add scheduler to job data map
    job.getJobDataMap().put("scheduler", this);

    // Create the trigger
    Trigger trigger =
        TriggerBuilder.newTrigger()
            .withIdentity(MAINTENANCE_TRIGGER_NAME, MAINTENANCE_GROUP_NAME)
            .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
            .build();

    // Schedule the job
    scheduler.scheduleJob(job, trigger);

    // Start the scheduler
    scheduler.start();
  }

  public void stopScheduledMaintenance() throws SchedulerException {
    logger.info("Stopping scheduled maintenance");
    scheduler.shutdown(true);
  }

  public void setMaintenanceSchedule(String cronExpression) throws SchedulerException {
    this.cronExpression = cronExpression;
    logger.info("Maintenance schedule updated to cron expression: {}", cronExpression);

    // Update the trigger
    JobKey jobKey = JobKey.jobKey(MAINTENANCE_JOB_NAME, MAINTENANCE_GROUP_NAME);
    if (scheduler.checkExists(jobKey)) {
      Trigger newTrigger =
          TriggerBuilder.newTrigger()
              .withIdentity(MAINTENANCE_TRIGGER_NAME, MAINTENANCE_GROUP_NAME)
              .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
              .build();

      scheduler.rescheduleJob(
          TriggerBuilder.newTrigger()
              .withIdentity(MAINTENANCE_TRIGGER_NAME, MAINTENANCE_GROUP_NAME)
              .build()
              .getKey(),
          newTrigger);
    } else {
      // If job doesn't exist, create it
      JobDetail job =
          JobBuilder.newJob(MaintenanceJob.class)
              .withIdentity(MAINTENANCE_JOB_NAME, MAINTENANCE_GROUP_NAME)
              .build();

      // Add scheduler to job data map
      job.getJobDataMap().put("scheduler", this);

      Trigger trigger =
          TriggerBuilder.newTrigger()
              .withIdentity(MAINTENANCE_TRIGGER_NAME, MAINTENANCE_GROUP_NAME)
              .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
              .build();

      scheduler.scheduleJob(job, trigger);
    }
  }

  public void setMaintenanceSchedule(long interval, TimeUnit timeUnit) throws SchedulerException {
    // Convert interval and timeUnit to cron expression
    String cronExpression = convertToCronExpression(interval, timeUnit);
    setMaintenanceSchedule(cronExpression);
  }

  private String convertToCronExpression(long interval, TimeUnit timeUnit) {
    // Default to daily at midnight
    if (interval <= 0) {
      return "0 0 0 * * ?";
    }

    switch (timeUnit) {
      case MINUTES:
        return "0 */" + interval + " * * * ?";
      case HOURS:
        return "0 0 */" + interval + " * * ?";
      case DAYS:
        return "0 0 0 */" + interval + " * ?";
      default:
        return "0 0 0 * * ?"; // Default to daily at midnight
    }
  }

  public Map<String, Object> getMaintenanceSchedule() {
    Map<String, Object> schedule = new HashMap<>();
    schedule.put("cronExpression", cronExpression);
    return schedule;
  }

  public boolean isInMaintenanceMode() {
    return isMaintenanceMode.get();
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

      // Perform maintenance tasks on the catalog
      if (catalog != null) {
        logger.info("Performing maintenance on catalog: {}", catalog.name());
        // Example maintenance operations:
        // - Clean up old metadata files
        // - Optimize table layouts
        // - Vacuum old snapshots
        // - Compact small files

        // For now, just log that we're performing maintenance
        // get the list of namespaces.
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
        } // Iterate through namespace

        for (Namespace namespace : namespaces) {
          // Get the table
          List<TableIdentifier> tables = catalog.listTables(namespace);
          for (TableIdentifier tableIdent : tables) {
            long olderThanMillis = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30);
            // Get the table
            Table table = catalog.loadTable(tableIdent);
            // Expire snapshots older than 30 days
            table.expireSnapshots().expireOlderThan(olderThanMillis).commit();
            // Remove Orphan Files

            // Rewrite Manifests
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
