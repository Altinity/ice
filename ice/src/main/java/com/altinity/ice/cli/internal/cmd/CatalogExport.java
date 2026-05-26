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

import com.altinity.ice.cli.internal.catalog.CatalogAdminClient;
import com.altinity.ice.internal.strings.Strings;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CatalogExport {

  private static final Logger logger = LoggerFactory.getLogger(CatalogExport.class);

  private CatalogExport() {}

  public static void run(CatalogAdminClient client, String namespace, String output)
      throws IOException {
    var snapshot = client.catalogExport(namespace);
    String json = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(snapshot);
    if (Strings.isNullOrEmpty(output) || "-".equals(output)) {
      System.out.print(json);
      if (!json.endsWith("\n")) {
        System.out.println();
      }
    } else {
      Files.writeString(Path.of(output), json);
      logger.info("Wrote catalog snapshot to {}", output);
    }
  }
}
