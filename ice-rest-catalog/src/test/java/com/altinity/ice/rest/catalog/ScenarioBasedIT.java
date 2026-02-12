/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Scenario-based integration tests for ICE REST Catalog.
 *
 * <p>This test class automatically discovers and executes all test scenarios from the
 * test/resources/scenarios directory. Each scenario is run as a separate test case.
 */
public class ScenarioBasedIT extends RESTCatalogTestBase {

  @Override
  protected ScenarioTestRunner createScenarioRunner(String scenarioName) throws Exception {
    Path scenariosDir = getScenariosDirectory();

    // Create CLI config file
    File cliConfig = createTempCliConfig();

    // Build template variables
    Map<String, String> templateVars = new HashMap<>();
    templateVars.put("CLI_CONFIG", cliConfig.getAbsolutePath());
    templateVars.put("MINIO_ENDPOINT", getMinioEndpoint());
    templateVars.put("CATALOG_URI", getCatalogUri());

    // Try to find ice-jar in the build
    String projectRoot = Paths.get("").toAbsolutePath().getParent().toString();
    String iceJar = projectRoot + "/ice/target/ice-jar";
    File iceJarFile = new File(iceJar);

    if (iceJarFile.exists() && iceJarFile.canExecute()) {
      // Use pre-built ice-jar if available
      templateVars.put("ICE_CLI", iceJar);
      logger.info("Using ice-jar from: {}", iceJar);
    } else {
      // Fall back to using local-ice wrapper script
      String localIce = projectRoot + "/.bin/local-ice";
      templateVars.put("ICE_CLI", localIce);
      logger.info("Using local-ice script from: {}", localIce);
    }

    return new ScenarioTestRunner(scenariosDir, templateVars);
  }
}
