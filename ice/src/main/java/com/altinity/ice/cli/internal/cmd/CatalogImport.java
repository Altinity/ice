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

import com.altinity.ice.cli.internal.catalog.AdminCatalogSnapshot;
import com.altinity.ice.cli.internal.catalog.CatalogAdminClient;
import com.altinity.ice.internal.strings.Strings;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class CatalogImport {

  private CatalogImport() {}

  public static void run(CatalogAdminClient client, String input, boolean dryRun, boolean overwrite)
      throws IOException {
    String snapshotJson = readCatalogSnapshotInput(input);
    ObjectMapper mapper = new ObjectMapper();
    var snapshot = mapper.readValue(snapshotJson, AdminCatalogSnapshot.class);
    var result = client.catalogImport(snapshot, dryRun, overwrite);
    System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
  }

  private static String readCatalogSnapshotInput(String inputPath) throws IOException {
    if (Strings.isNullOrEmpty(inputPath) || "-".equals(inputPath)) {
      return new String(System.in.readAllBytes(), StandardCharsets.UTF_8);
    }
    return Files.readString(Path.of(inputPath));
  }
}
