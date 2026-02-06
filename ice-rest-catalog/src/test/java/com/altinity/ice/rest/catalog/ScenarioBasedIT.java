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
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Scenario-based integration tests for ICE REST Catalog.
 *
 * <p>This test class automatically discovers and executes all test scenarios from the
 * test/resources/scenarios directory. Each scenario is run as a separate test case.
 */
public class ScenarioBasedIT extends RESTCatalogTestBase {

  /**
   * Data provider that discovers all test scenarios.
   *
   * @return Array of scenario names to be used as test parameters
   * @throws Exception If there's an error discovering scenarios
   */
  @DataProvider(name = "scenarios")
  public Object[][] scenarioProvider() throws Exception {
    Path scenariosDir = getScenariosDirectory();
    ScenarioTestRunner runner = createScenarioRunner();

    List<String> scenarios = runner.discoverScenarios();

    if (scenarios.isEmpty()) {
      logger.warn("No test scenarios found in: {}", scenariosDir);
      return new Object[0][0];
    }

    logger.info("Discovered {} test scenario(s): {}", scenarios.size(), scenarios);

    // Convert to Object[][] for TestNG data provider
    Object[][] data = new Object[scenarios.size()][1];
    for (int i = 0; i < scenarios.size(); i++) {
      data[i][0] = scenarios.get(i);
    }
    return data;
  }

  /**
   * Parameterized test that executes a single scenario.
   *
   * @param scenarioName Name of the scenario to execute
   * @throws Exception If the scenario execution fails
   */
  @Test(dataProvider = "scenarios")
  public void testScenario(String scenarioName) throws Exception {
    logger.info("====== Starting scenario test: {} ======", scenarioName);

    ScenarioTestRunner runner = createScenarioRunner();
    ScenarioTestRunner.ScenarioResult result = runner.executeScenario(scenarioName);

    // Log results
    if (result.runScriptResult() != null) {
      logger.info("Run script exit code: {}", result.runScriptResult().exitCode());
    }

    if (result.verifyScriptResult() != null) {
      logger.info("Verify script exit code: {}", result.verifyScriptResult().exitCode());
    }

    // Assert success
    if (!result.isSuccess()) {
      StringBuilder errorMessage = new StringBuilder();
      errorMessage.append("Scenario '").append(scenarioName).append("' failed:\n");

      if (result.runScriptResult() != null && result.runScriptResult().exitCode() != 0) {
        errorMessage.append("\nRun script failed with exit code: ");
        errorMessage.append(result.runScriptResult().exitCode());
        errorMessage.append("\nStdout:\n");
        errorMessage.append(result.runScriptResult().stdout());
        errorMessage.append("\nStderr:\n");
        errorMessage.append(result.runScriptResult().stderr());
      }

      if (result.verifyScriptResult() != null && result.verifyScriptResult().exitCode() != 0) {
        errorMessage.append("\nVerify script failed with exit code: ");
        errorMessage.append(result.verifyScriptResult().exitCode());
        errorMessage.append("\nStdout:\n");
        errorMessage.append(result.verifyScriptResult().stdout());
        errorMessage.append("\nStderr:\n");
        errorMessage.append(result.verifyScriptResult().stderr());
      }

      throw new AssertionError(errorMessage.toString());
    }

    logger.info("====== Scenario test passed: {} ======", scenarioName);
  }

  /**
   * Create a ScenarioTestRunner with the appropriate template variables.
   *
   * @return Configured ScenarioTestRunner
   * @throws Exception If there's an error creating the runner
   */
  private ScenarioTestRunner createScenarioRunner() throws Exception {
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

  /**
   * Get the path to the scenarios directory.
   *
   * @return Path to scenarios directory
   * @throws URISyntaxException If the resource URL cannot be converted to a path
   */
  private Path getScenariosDirectory() throws URISyntaxException {
    // Get the scenarios directory from test resources
    URL scenariosUrl = getClass().getClassLoader().getResource("scenarios");

    if (scenariosUrl == null) {
      // If not found in resources, try relative to project
      return Paths.get("src/test/resources/scenarios");
    }

    return Paths.get(scenariosUrl.toURI());
  }
}
