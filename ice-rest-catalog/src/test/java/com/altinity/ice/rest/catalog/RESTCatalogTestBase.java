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
import java.util.HashMap;
import java.util.Map;

import com.altinity.ice.rest.catalog.internal.config.Config;
import org.apache.iceberg.CatalogProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.testcontainers.containers.GenericContainer;
import org.eclipse.jetty.server.Server;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import java.net.URI;
import java.nio.file.Files;

import static com.altinity.ice.rest.catalog.Main.createServer;

/**
 * Base class for REST catalog integration tests.
 * Provides common setup and teardown for minio, REST catalog server, and REST client.
 */
public abstract class RESTCatalogTestBase {

  protected static final Logger logger = LoggerFactory.getLogger(RESTCatalogTestBase.class);
  protected RESTCatalog restCatalog;
  protected Server server;
  
  @SuppressWarnings("rawtypes")
  protected final GenericContainer minio =
    new GenericContainer("minio/minio:latest")
      .withExposedPorts(9000)
      .withEnv("MINIO_ACCESS_KEY", "minioadmin")
      .withEnv("MINIO_SECRET_KEY", "minioadmin")
      .withCommand("server", "/data");

  @BeforeClass
  public void setUp() throws Exception {
    // Start minio container
    minio.start();
    
    // Configure S3 properties for minio
    String minioEndpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
    
    // Create S3 client to create bucket
    S3Client s3Client = S3Client.builder()
        .endpointOverride(URI.create(minioEndpoint))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("minioadmin", "minioadmin")))
        .forcePathStyle(true)
        .build();
    
    // Create the test bucket
    try {
      s3Client.createBucket(CreateBucketRequest.builder()
          .bucket("test-bucket")
          .build());
      logger.info("Created test-bucket in minio");
    } catch (Exception e) {
      logger.warn("Bucket may already exist: {}", e.getMessage());
    } finally {
      s3Client.close();
    }
    
    // Create ICE REST catalog server configuration
    Config config = new Config(
      "localhost:8080", // addr
      "localhost:8081", // debugAddr 
      null, // adminAddr
      "test-catalog", // name
      "jdbc:sqlite::memory:", // uri
      "s3://test-bucket/warehouse", // warehouse
      null, // localFileIOBaseDir
      new Config.S3(minioEndpoint, true, "minioadmin", "minioadmin", "us-east-1"), // s3
      null, // bearerTokens
      new Config.AnonymousAccess(true, new Config.AccessConfig(false, null)), // anonymousAccess - enable with read-write for testing
      null, // maintenanceSchedule
      0, // snapshotTTLInDays
      null, // loadTableProperties
      null // icebergProperties
    );
    
    // Create backend catalog from config
    Map<String, String> icebergConfig = config.toIcebergConfig();
    Catalog backendCatalog = org.apache.iceberg.CatalogUtil.buildIcebergCatalog("backend", icebergConfig, null);
    
    // Start ICE REST catalog server
    server = createServer("localhost", 8080, backendCatalog, config, icebergConfig);
    server.start();
    
    // Wait for server to be ready
    while (!server.isStarted()) {
      Thread.sleep(100);
    }
    
    // Create REST client to test the server - use base URL without /v1/config
    Map<String, String> restClientProps = new HashMap<>();
    restClientProps.put(CatalogProperties.URI, "http://localhost:8080");
    
    restCatalog = new RESTCatalog();
    restCatalog.initialize("rest-catalog", restClientProps);
  }

  @AfterClass
  public void tearDown() {
    // Close the REST catalog client
    if (restCatalog != null) {
      try {
        restCatalog.close();
      } catch (Exception e) {
        logger.error("Error closing REST catalog: {}", e.getMessage(), e);
      }
    }
    
    // Stop the REST catalog server
    if (server != null) {
      try {
        server.stop();
      } catch (Exception e) {
        logger.error("Error stopping server: {}", e.getMessage(), e);
      }
    }
    
    // Stop minio container
    if (minio != null && minio.isRunning()) {
      minio.stop();
    }
  }
  
  /**
   * Helper method to create a temporary CLI config file
   */
  protected File createTempCliConfig() throws Exception {
    File tempConfigFile = File.createTempFile("ice-rest-cli-", ".yaml");
    tempConfigFile.deleteOnExit();
    
    String configContent = "uri: http://localhost:8080\n";
    Files.write(tempConfigFile.toPath(), configContent.getBytes());
    
    return tempConfigFile;
  }
}