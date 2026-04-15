/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.CatalogProperties;
import org.testng.annotations.Test;

public class DescribeMetadataTest {

  private static String testMetadataPath() {
    String path =
        Objects.requireNonNull(
                DescribeMetadataTest.class
                    .getClassLoader()
                    .getResource("com/altinity/ice/cli/internal/cmd/test-v1.metadata.json"))
            .getPath();
    return "file://" + path;
  }

  private static Map<String, String> testConfig() {
    String filePath = testMetadataPath();
    String dir = filePath.substring(0, filePath.lastIndexOf('/'));
    return Map.of(CatalogProperties.WAREHOUSE_LOCATION, dir);
  }

  @Test
  public void testDescribeMetadataSummary() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));

    try {
      DescribeMetadata.run(
          testConfig(), testMetadataPath(), false, DescribeMetadata.Option.SUMMARY);

      String output = outContent.toString();

      assertThat(output).contains("uuid:");
      assertThat(output).contains("fb3de834-aa02-4154-b3e2-a1f528e8e7c4");
      assertThat(output).contains("formatVersion: 2");
      assertThat(output).contains("location:");
      assertThat(output).contains("currentSnapshotId:");
      assertThat(output).contains("numSnapshots: 1");
      assertThat(output).contains("partitionSpec:");
      assertThat(output).contains("sortOrder:");
    } finally {
      System.setOut(originalOut);
    }
  }

  @Test
  public void testDescribeMetadataSchema() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));

    try {
      DescribeMetadata.run(testConfig(), testMetadataPath(), false, DescribeMetadata.Option.SCHEMA);

      String output = outContent.toString();

      assertThat(output).contains("schema:");
      assertThat(output).contains("schemaId: 0");
      assertThat(output).contains("fields:");
      assertThat(output).contains("name: \"id\"");
      assertThat(output).contains("name: \"name\"");
      assertThat(output).contains("name: \"ts\"");
      assertThat(output).contains("type: \"long\"");
      assertThat(output).contains("type: \"string\"");
    } finally {
      System.setOut(originalOut);
    }
  }

  @Test
  public void testDescribeMetadataSnapshots() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));

    try {
      DescribeMetadata.run(
          testConfig(), testMetadataPath(), false, DescribeMetadata.Option.SNAPSHOTS);

      String output = outContent.toString();

      assertThat(output).contains("snapshots:");
      assertThat(output).contains("snapshotId:");
      assertThat(output).contains("timestamp:");
      assertThat(output).contains("operation: \"append\"");
      assertThat(output).contains("manifestListLocation:");
    } finally {
      System.setOut(originalOut);
    }
  }

  @Test
  public void testDescribeMetadataHistory() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));

    try {
      DescribeMetadata.run(
          testConfig(), testMetadataPath(), false, DescribeMetadata.Option.HISTORY);

      String output = outContent.toString();

      assertThat(output).contains("history:");
      assertThat(output).contains("snapshotLog:");
      assertThat(output).contains("snapshotId:");
      assertThat(output).contains("timestamp:");
    } finally {
      System.setOut(originalOut);
    }
  }

  @Test
  public void testDescribeMetadataJson() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));

    try {
      DescribeMetadata.run(testConfig(), testMetadataPath(), true, DescribeMetadata.Option.SUMMARY);

      String output = outContent.toString();

      assertThat(output).contains("{");
      assertThat(output).contains("}");
      assertThat(output).contains("\"summary\"");
      assertThat(output).contains("\"uuid\"");
      assertThat(output).contains("\"fb3de834-aa02-4154-b3e2-a1f528e8e7c4\"");
    } finally {
      System.setOut(originalOut);
    }
  }

  @Test
  public void testDescribeMetadataDefaultIsSummary() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));

    try {
      DescribeMetadata.run(
          testConfig(), testMetadataPath(), false, DescribeMetadata.Option.SUMMARY);

      String output = outContent.toString();

      assertThat(output).contains("summary:");
      assertThat(output).doesNotContain("schema:");
      assertThat(output).doesNotContain("snapshots:");
      assertThat(output).doesNotContain("history:");
      assertThat(output).doesNotContain("manifests:");
    } finally {
      System.setOut(originalOut);
    }
  }
}
