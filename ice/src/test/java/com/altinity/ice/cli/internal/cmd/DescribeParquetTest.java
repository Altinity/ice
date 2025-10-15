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

import com.altinity.ice.test.Resource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.testng.annotations.Test;

public class DescribeParquetTest {

  @Test
  public void testDescribeParquetSummary() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));
    
    try {
      InMemoryCatalog catalog = new InMemoryCatalog();
      catalog.initialize("test", java.util.Map.of());
      
      var sampleFile = Resource.asInputFile("com/altinity/ice/cli/internal/iceberg/parquet/sample-001.parquet");
      
      DescribeParquet.run(sampleFile, false, DescribeParquet.Option.SUMMARY);
      
      String output = outContent.toString();
      
      assertThat(output).contains("rows:");
      assertThat(output).contains("rowGroups:");
      assertThat(output).contains("compressedSize:");
      assertThat(output).contains("uncompressedSize:");
    } finally {
      System.setOut(originalOut);
    }
  }

  @Test
  public void testDescribeParquetColumns() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));
    
    try {
      var sampleFile = Resource.asInputFile("com/altinity/ice/cli/internal/iceberg/parquet/sample-001.parquet");
      
      DescribeParquet.run(sampleFile, false, DescribeParquet.Option.COLUMNS);
      
      String output = outContent.toString();
      
      assertThat(output).contains("columns:");
      assertThat(output).contains("name:");
      assertThat(output).contains("type:");
    } finally {
      System.setOut(originalOut);
    }
  }
  
  @Test 
  public void testDescribeParquetJson() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));
    
    try {
      var sampleFile = Resource.asInputFile("com/altinity/ice/cli/internal/iceberg/parquet/sample-001.parquet");
      
      DescribeParquet.run(sampleFile, true, DescribeParquet.Option.SUMMARY);
      
      String output = outContent.toString();
      
      assertThat(output).contains("{");
      assertThat(output).contains("}");
      assertThat(output).contains("\"summary\"");
    } finally {
      System.setOut(originalOut);
    }
  }
}