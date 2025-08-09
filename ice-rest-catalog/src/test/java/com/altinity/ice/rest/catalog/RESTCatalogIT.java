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

import org.testng.annotations.Test;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.PartitionSpec;
import picocli.CommandLine;

/**
 * Basic REST catalog integration tests.
 * Tests fundamental catalog operations like namespace and table management.
 */
public class RESTCatalogIT extends RESTCatalogTestBase {

  @Test
  public void testCatalogBasicOperations() {
    // Test REST catalog with minio backend for warehouse storage
    
    // Create a namespace via REST API
    var namespace = org.apache.iceberg.catalog.Namespace.of("test_ns");
    restCatalog.createNamespace(namespace);

    // Verify namespace exists via REST API
    var namespaces = restCatalog.listNamespaces();
    assert namespaces.contains(namespace) : "Namespace should exist";
    
    // Delete the namespace via REST API
    restCatalog.dropNamespace(namespace);
    
    // Verify namespace no longer exists via REST API
    var namespacesAfterDrop = restCatalog.listNamespaces();
    assert !namespacesAfterDrop.contains(namespace) : "Namespace should not exist after deletion";

    logger.info("Basic REST catalog operations (create and delete namespace) successful with SQLite + Minio backend");
  }
  
  @Test
  public void testScanCommand() throws Exception {
    // Create a namespace and empty table for scan test
    var namespace = org.apache.iceberg.catalog.Namespace.of("test_scan");
    restCatalog.createNamespace(namespace);
    
    // Create a simple schema
    Schema schema = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "age", Types.IntegerType.get())
    );
    
    // Create table (empty table is fine for scan command test)
    TableIdentifier tableId = TableIdentifier.of(namespace, "users");
    restCatalog.createTable(tableId, schema, PartitionSpec.unpartitioned());
    
    // Create CLI config file
    File tempConfigFile = createTempCliConfig();
    
    // Test CLI scan command on empty table (should succeed with no output)
    int exitCode = new CommandLine(com.altinity.ice.cli.Main.class)
        .execute("--config", tempConfigFile.getAbsolutePath(), "scan", "test_scan.users");
    
    // Verify scan command succeeded
    assert exitCode == 0 : "Scan command should succeed even on empty table";
    
    logger.info("ICE CLI scan command test successful on empty table");
    
    // Cleanup
    restCatalog.dropTable(tableId);
    restCatalog.dropNamespace(namespace);
  }
}