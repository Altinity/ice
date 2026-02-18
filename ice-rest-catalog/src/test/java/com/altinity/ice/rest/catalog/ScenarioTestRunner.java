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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test runner for scenario-based integration tests.
 *
 * <p>This class discovers scenario directories, loads their configuration, processes script
 * templates, and executes them in a controlled test environment.
 */
public class ScenarioTestRunner {

  private static final Logger logger = LoggerFactory.getLogger(ScenarioTestRunner.class);
  private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

  private final Path scenariosDir;
  private final Map<String, String> globalTemplateVars;

  /**
   * Create a new scenario test runner.
   *
   * @param scenariosDir Path to the scenarios directory
   * @param globalTemplateVars Global template variables available to all scenarios
   */
  public ScenarioTestRunner(Path scenariosDir, Map<String, String> globalTemplateVars) {
    this.scenariosDir = scenariosDir;
    this.globalTemplateVars = new HashMap<>(globalTemplateVars);
  }

  /**
   * Discover all scenario directories.
   *
   * @return List of scenario names (directory names)
   * @throws IOException If there's an error reading the scenarios directory
   */
  public List<String> discoverScenarios() throws IOException {
    if (!Files.exists(scenariosDir) || !Files.isDirectory(scenariosDir)) {
      logger.warn("Scenarios directory does not exist: {}", scenariosDir);
      return new ArrayList<>();
    }

    try (Stream<Path> stream = Files.list(scenariosDir)) {
      return stream
          .filter(Files::isDirectory)
          .map(path -> path.getFileName().toString())
          .filter(name -> !name.startsWith(".")) // Ignore hidden directories
          .sorted()
          .collect(Collectors.toList());
    }
  }

  /**
   * Load a scenario configuration from its directory.
   *
   * @param scenarioName Name of the scenario (directory name)
   * @return ScenarioConfig object
   * @throws IOException If there's an error reading the scenario configuration
   */
  public ScenarioConfig loadScenarioConfig(String scenarioName) throws IOException {
    Path scenarioDir = scenariosDir.resolve(scenarioName);
    Path configPath = scenarioDir.resolve("scenario.yaml");

    if (!Files.exists(configPath)) {
      throw new IOException("scenario.yaml not found in " + scenarioDir);
    }

    return yamlMapper.readValue(configPath.toFile(), ScenarioConfig.class);
  }

  /**
   * Execute a scenario's scripts.
   *
   * @param scenarioName Name of the scenario to execute
   * @return ScenarioResult containing execution results
   * @throws Exception If there's an error executing the scenario
   */
  public ScenarioResult executeScenario(String scenarioName) throws Exception {
    logger.info("Executing scenario: {}", scenarioName);

    Path scenarioDir = scenariosDir.resolve(scenarioName);
    ScenarioConfig config = loadScenarioConfig(scenarioName);

    // Build template variables map
    Map<String, String> templateVars = new HashMap<>(globalTemplateVars);
    if (!templateVars.containsKey("SCENARIO_DIR")) {
      templateVars.put("SCENARIO_DIR", scenarioDir.toAbsolutePath().toString());
    }

    // Add environment variables from scenario config
    if (config.env() != null) {
      templateVars.putAll(config.env());
    }

    ScriptExecutionResult runScriptResult = null;
    ScriptExecutionResult verifyScriptResult = null;

    // Execute run.sh.tmpl
    Path runScriptTemplate = scenarioDir.resolve("run.sh.tmpl");
    if (Files.exists(runScriptTemplate)) {
      logger.info("Executing run script for scenario: {}", scenarioName);
      runScriptResult = executeScript(runScriptTemplate, templateVars);

      if (runScriptResult.exitCode() != 0) {
        logger.error("Run script failed for scenario: {}", scenarioName);
        logger.error("Exit code: {}", runScriptResult.exitCode());
        logger.error("stdout:\n{}", runScriptResult.stdout());
        logger.error("stderr:\n{}", runScriptResult.stderr());
        return new ScenarioResult(scenarioName, runScriptResult, verifyScriptResult);
      }
    } else {
      logger.warn("No run.sh.tmpl found for scenario: {}", scenarioName);
    }

    // Execute verify.sh.tmpl (optional)
    Path verifyScriptTemplate = scenarioDir.resolve("verify.sh.tmpl");
    if (Files.exists(verifyScriptTemplate)) {
      logger.info("Executing verify script for scenario: {}", scenarioName);
      verifyScriptResult = executeScript(verifyScriptTemplate, templateVars);

      if (verifyScriptResult.exitCode() != 0) {
        logger.error("Verify script failed for scenario: {}", scenarioName);
        logger.error("Exit code: {}", verifyScriptResult.exitCode());
        logger.error("stdout:\n{}", verifyScriptResult.stdout());
        logger.error("stderr:\n{}", verifyScriptResult.stderr());
      }
    }

    return new ScenarioResult(scenarioName, runScriptResult, verifyScriptResult);
  }

  /**
   * Execute a script template with the given template variables.
   *
   * @param scriptTemplatePath Path to the script template
   * @param templateVars Map of template variables to substitute
   * @return ScriptExecutionResult containing execution results
   * @throws IOException If there's an error reading or executing the script
   */
  private ScriptExecutionResult executeScript(
      Path scriptTemplatePath, Map<String, String> templateVars) throws IOException {
    // Read the script template
    String scriptContent = Files.readString(scriptTemplatePath);

    // Process template variables
    String processedScript = processTemplate(scriptContent, templateVars);

    // Create a temporary executable script file
    Path tempScript = Files.createTempFile("scenario-script-", ".sh");
    try {
      Files.writeString(tempScript, processedScript);
      if (!tempScript.toFile().setExecutable(true)) {
        logger.warn("Could not set script as executable: {}", tempScript);
      }

      // Execute the script
      ProcessBuilder processBuilder = new ProcessBuilder("/bin/bash", tempScript.toString());

      // Set environment variables from template vars
      Map<String, String> env = processBuilder.environment();
      env.putAll(templateVars);

      Process process = processBuilder.start();

      // Capture output
      StringBuilder stdout = new StringBuilder();
      StringBuilder stderr = new StringBuilder();

      Thread stdoutReader =
          new Thread(
              () -> {
                try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                  String line;
                  while ((line = reader.readLine()) != null) {
                    stdout.append(line).append("\n");
                    logger.info("[script] {}", line);
                  }
                } catch (IOException e) {
                  logger.error("Error reading stdout", e);
                }
              });

      Thread stderrReader =
          new Thread(
              () -> {
                try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                  String line;
                  while ((line = reader.readLine()) != null) {
                    stderr.append(line).append("\n");
                    logger.warn("[script] {}", line);
                  }
                } catch (IOException e) {
                  logger.error("Error reading stderr", e);
                }
              });

      stdoutReader.start();
      stderrReader.start();

      // Wait for the process to complete
      int exitCode = process.waitFor();

      // Wait for output readers to finish
      stdoutReader.join();
      stderrReader.join();

      return new ScriptExecutionResult(exitCode, stdout.toString(), stderr.toString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Script execution interrupted", e);
    } finally {
      // Clean up temporary script file
      try {
        Files.deleteIfExists(tempScript);
      } catch (IOException e) {
        logger.warn("Failed to delete temporary script file: {}", tempScript, e);
      }
    }
  }

  /**
   * Process template variables in a script.
   *
   * <p>Replaces {{VARIABLE_NAME}} with the corresponding value from templateVars.
   *
   * @param template Template string
   * @param templateVars Map of variable names to values
   * @return Processed string with variables substituted
   */
  private String processTemplate(String template, Map<String, String> templateVars) {
    String result = template;
    for (Map.Entry<String, String> entry : templateVars.entrySet()) {
      String placeholder = "{{" + entry.getKey() + "}}";
      result = result.replace(placeholder, entry.getValue());
    }
    return result;
  }

  /** Result of executing a scenario. */
  public record ScenarioResult(
      String scenarioName,
      ScriptExecutionResult runScriptResult,
      ScriptExecutionResult verifyScriptResult) {

    public boolean isSuccess() {
      boolean runSuccess = runScriptResult == null || runScriptResult.exitCode() == 0;
      boolean verifySuccess = verifyScriptResult == null || verifyScriptResult.exitCode() == 0;
      return runSuccess && verifySuccess;
    }
  }

  /** Result of executing a single script. */
  public record ScriptExecutionResult(int exitCode, String stdout, String stderr) {}
}
