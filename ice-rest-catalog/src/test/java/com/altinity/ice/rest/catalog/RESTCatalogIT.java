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
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.testng.annotations.Test;
import picocli.CommandLine;

/**
 * Basic REST catalog integration tests. Tests fundamental catalog operations like namespace and
 * table management.
 */
public class RESTCatalogIT extends RESTCatalogTestBase {

  @Test
  public void testCatalogBasicOperations() throws Exception {
    // Test catalog operations using ICE CLI commands

    // Create CLI config file
    File tempConfigFile = createTempCliConfig();

    String namespaceName = "test_ns";

    // Create namespace via CLI
    int createExitCode =
        new CommandLine(com.altinity.ice.cli.Main.class)
            .execute(
                "--config", tempConfigFile.getAbsolutePath(), "create-namespace", namespaceName);

    // Verify create namespace command succeeded
    assert createExitCode == 0 : "Create namespace command should succeed";

    // Delete the namespace via CLI
    int deleteExitCode =
        new CommandLine(com.altinity.ice.cli.Main.class)
            .execute(
                "--config", tempConfigFile.getAbsolutePath(), "delete-namespace", namespaceName);

    // Verify delete namespace command succeeded
    assert deleteExitCode == 0 : "Delete namespace command should succeed";

    logger.info("Basic catalog operations (create and delete namespace) successful with ICE CLI");
  }

  @Test
  public void testScanCommand() throws Exception {
    // Create CLI config file
    File tempConfigFile = createTempCliConfig();

    String namespaceName = "test_scan";
    String tableName = "test_scan.users";

    // Create namespace via CLI
    int createNsExitCode =
        new CommandLine(com.altinity.ice.cli.Main.class)
            .execute(
                "--config", tempConfigFile.getAbsolutePath(), "create-namespace", namespaceName);

    assert createNsExitCode == 0 : "Create namespace command should succeed";

    // Create table first using insert command with --create-table flag
    // Use existing iris parquet file
    String testParquetPath = "examples/localfileio/iris.parquet";
    File testParquetFile = new File(testParquetPath);
    if (!testParquetFile.exists()) {
      // Try alternative path
      testParquetFile = new File("../examples/localfileio/iris.parquet");
    }
    assert testParquetFile.exists()
        : "Test parquet file should exist at " + testParquetFile.getAbsolutePath();

    // Create table and insert data
    int insertExitCode =
        new CommandLine(com.altinity.ice.cli.Main.class)
            .execute(
                "--config",
                tempConfigFile.getAbsolutePath(),
                "insert",
                "--create-table",
                tableName,
                testParquetFile.getAbsolutePath());

    assert insertExitCode == 0 : "Insert command should succeed to create table with data";

    // Test CLI scan command on the table with data and capture output
    // Save original System.out and System.err
    PrintStream originalOut = System.out;
    PrintStream originalErr = System.err;
    
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
    PrintStream captureOut = new PrintStream(outputStream);
    PrintStream captureErr = new PrintStream(errorStream);
    
    try {
      // Redirect System.out and System.err to capture streams
      System.setOut(captureOut);
      System.setErr(captureErr);
      
      int scanExitCode = new CommandLine(com.altinity.ice.cli.Main.class)
          .execute("--config", tempConfigFile.getAbsolutePath(), "scan", tableName);
      
      captureOut.flush();
      captureErr.flush();
      
      String scanOutput = outputStream.toString();
      String scanError = errorStream.toString();
      
      // Combine stdout and stderr for analysis
      String combinedOutput = scanOutput + scanError;
    
      // Verify scan command succeeded
      assert scanExitCode == 0 : "Scan command should succeed on existing table";
      
      // Validate scan output contains expected data from iris dataset
      assert combinedOutput.length() > 0 : "Scan should produce some output";
      
      // Check for iris dataset columns (be flexible with column name formats)
      boolean hasSepalLength = combinedOutput.contains("sepal.length") || combinedOutput.contains("sepal_length") || combinedOutput.contains("sepal-length");
      boolean hasVariety = combinedOutput.contains("variety");
      
      assert hasSepalLength : "Scan output should contain sepal length column data. Output: " + combinedOutput.substring(0, Math.min(500, combinedOutput.length()));
      assert hasVariety : "Scan output should contain variety column data. Output: " + combinedOutput.substring(0, Math.min(500, combinedOutput.length()));
      
      logger.info("ICE CLI scan command test successful - validated output contains iris data");
      logger.info("Scan output length: {} characters", combinedOutput.length());
      logger.debug("Scan output preview: {}", combinedOutput.substring(0, Math.min(500, combinedOutput.length())));
      
    } finally {
      // Restore original System.out and System.err
      System.setOut(originalOut);
      System.setErr(originalErr);
    }

    // Cleanup - delete table first, then namespace
    int deleteTableExitCode =
        new CommandLine(com.altinity.ice.cli.Main.class)
            .execute("--config", tempConfigFile.getAbsolutePath(), "delete-table", tableName);

    int deleteNsExitCode =
        new CommandLine(com.altinity.ice.cli.Main.class)
            .execute(
                "--config", tempConfigFile.getAbsolutePath(), "delete-namespace", namespaceName);

    assert deleteNsExitCode == 0 : "Delete namespace command should succeed";

    logger.info("ICE CLI scan command test completed");
  }
}
