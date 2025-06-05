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

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import picocli.CommandLine;

public class RESTCatalogIT {

  private Thread serverThread;
  private Main mainCommand;

  @Test
  public void testMainCommandWithConfig() throws InterruptedException {
    // Provide your CLI arguments here — replace with an actual test config file path
    String[] args = {"--config", "src/test/resources/ice-rest-catalog.yaml"};

    // Create a new instance of the command class
    mainCommand = new Main();

    // Start the server in a separate thread
    serverThread =
        new Thread(
            () -> {
              try {
                new CommandLine(mainCommand).execute(args);
              } catch (Exception e) {
                System.err.println("Error running server: " + e.getMessage());
              }
            });
    serverThread.start();

    // Give the server time to start up
    Thread.sleep(5000);
  }

  @AfterClass
  public void tearDown() {
    try {
      // Get the admin address from the config
      String adminAddr = System.getProperty("ice.rest.catalog.admin.addr", "localhost:5000");

      // Get the shutdown token from the config
      String shutdownToken = "foo";

      // Create HTTP client and send shutdown request
      java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
      java.net.http.HttpRequest request =
          java.net.http.HttpRequest.newBuilder()
              .uri(java.net.URI.create("http://" + adminAddr + "/shutdown?token=" + shutdownToken))
              // .header("Authorization", "bearer " + shutdownToken)
              .POST(java.net.http.HttpRequest.BodyPublishers.noBody())
              .build();

      java.net.http.HttpResponse<String> response =
          client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        System.err.println("Failed to shutdown server. Status code: " + response.statusCode());
      }

      // Wait for the server thread to finish
      if (serverThread != null && serverThread.isAlive()) {
        serverThread.join(5000); // Wait up to 5 seconds for graceful shutdown
        if (serverThread.isAlive()) {
          serverThread.interrupt(); // Force interrupt if still running
        }
      }
    } catch (Exception e) {
      System.err.println("Error during server shutdown: " + e.getMessage());
    }
  }
}
