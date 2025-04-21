package com.altinity.ice.rest.catalog.internal.maintenance;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceJob implements Job {
  private static final Logger logger = LoggerFactory.getLogger(MaintenanceJob.class);

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    logger.info("Maintenance job triggered by Quartz scheduler");

    // Get the scheduler from the context
    QuartzMaintenanceScheduler scheduler =
        (QuartzMaintenanceScheduler) context.getMergedJobDataMap().get("scheduler");
    if (scheduler != null) {
      scheduler.performMaintenance();
    } else {
      logger.error("Maintenance scheduler not found in job context");
    }
  }
}
