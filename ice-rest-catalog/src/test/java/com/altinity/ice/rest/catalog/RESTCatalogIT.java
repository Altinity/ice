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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import com.altinity.ice.internal.strings.Strings;
import com.altinity.ice.rest.catalog.internal.config.Config;
import com.altinity.ice.rest.catalog.internal.etcd.EtcdCatalog;
import com.google.common.net.HostAndPort;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Txn;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.eclipse.jetty.server.Server;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import picocli.CommandLine;

import static com.altinity.ice.rest.catalog.Main.*;

public class RESTCatalogIT {

  private Thread serverThread;

  private EtcdCatalog catalog;
  private Consumer<KV> preKvtx;
  private Server httpServer;
  private Server adminServer;

  @SuppressWarnings("rawtypes")
  private final GenericContainer etcd =
      new GenericContainer("bitnami/etcd:3.5.21")
          .withExposedPorts(2379, 2380)
          .withEnv("ALLOW_NONE_AUTHENTICATION", "yes");

  @BeforeClass
  public void setUp() {
    etcd.start();
    String uri = "http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379);
    catalog =
        new EtcdCatalog("default", uri, "/foo", new InMemoryFileIO()) {

          @Override
          protected Txn kvtx() {
            if (preKvtx != null) {
              var x = preKvtx;
              preKvtx = null;
              x.accept(this.kv);
            }
            return super.kvtx();
          }
        };
  }

  @Test()
  public void testMainCommandWithConfig() throws Exception {
    // Provide your CLI arguments here — replace with an actual test config file path

    // Start server in a separate thread
    serverThread =
        new Thread(
            () -> {
              try {
                var config = Config.load("src/test/resources/ice-rest-catalog.yaml");

                // Use RESTCatalog directly
                Map<String, String> icebergConfig = new HashMap<>();
                icebergConfig.put("uri", "http://localhost:5000");
                icebergConfig.put("warehouse", "s3://my-bucket/warehouse");
                // Revert back to rest catalog
                icebergConfig.put("catalog-impl", "org.apache.iceberg.rest.RESTCatalog");
                icebergConfig.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                // Catalog catalog =  newEctdCatalog(icebergConfig);
                // need to implement custom org.apache.iceberg.rest.RESTClient)
                if (!Strings.isNullOrEmpty(config.adminAddr())) {
                  HostAndPort adminHostAndPort = HostAndPort.fromString(config.adminAddr());
                  adminServer =
                      createAdminServer(
                          adminHostAndPort.getHost(),
                          adminHostAndPort.getPort(),
                          catalog,
                          config,
                          icebergConfig);
                  adminServer.start();
                }

                HostAndPort hostAndPort = HostAndPort.fromString(config.addr());
                httpServer =
                    createServer(
                        hostAndPort.getHost(),
                        hostAndPort.getPort(),
                        catalog,
                        config,
                        icebergConfig);
                httpServer.start();
                httpServer.join();
              } catch (Exception e) {
                System.err.println("Error in server thread: " + e.getMessage());
                e.printStackTrace();
              }
            });

    serverThread.start();

    // Give the server time to start up
    Thread.sleep(5000);

    // Create namespace and table using Ice CLI
    String table = "nyc.taxis";

    // insert data
    new CommandLine(new com.altinity.ice.cli.Main())
        .execute("--config", "src/test/resources/ice-rest-cli.yaml", "insert", table, 
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet", "--partition='[{\"column\":\"tpep_pickup_datetime\",\"transform\":\"day\"}]'");

    // Scan command
    new CommandLine(new com.altinity.ice.cli.Main())
        .execute("--config", "src/test/resources/ice-rest-cli.yaml", "scan", table);

    // Delete table
    new CommandLine(new com.altinity.ice.cli.Main())
        .execute("--config", "src/test/resources/ice-rest-cli.yaml", "delete-table", table);

    // Delete namespace
    new CommandLine(new com.altinity.ice.cli.Main())
        .execute("--config", "src/test/resources/ice-rest-cli.yaml", "delete-namespace", "nyc");


  }

  @AfterClass
  public void tearDown() {
    try {
      // Stop the servers first
      if (httpServer != null && httpServer.isRunning()) {
        try {
          httpServer.stop();
        } catch (Exception e) {
          System.err.println("Error stopping http server: " + e.getMessage());
        }
      }

      if (adminServer != null && adminServer.isRunning()) {
        try {
          adminServer.stop();
        } catch (Exception e) {
          System.err.println("Error stopping admin server: " + e.getMessage());
        }
      }

      // Wait for the server thread to finish
      if (serverThread != null && serverThread.isAlive()) {
        serverThread.join(5000); // Wait up to 5 seconds for graceful shutdown
        if (serverThread.isAlive()) {
          serverThread.interrupt(); // Force interrupt if still running
        }
      }

      // Stop the etcd container
      if (etcd != null && etcd.isRunning()) {
        etcd.stop();
      }
    } catch (Exception e) {
      System.err.println("Error during server shutdown: " + e.getMessage());
    }
  }
}
