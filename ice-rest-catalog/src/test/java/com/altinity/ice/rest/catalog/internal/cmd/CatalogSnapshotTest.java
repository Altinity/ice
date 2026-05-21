/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import com.altinity.ice.rest.catalog.internal.rest.RESTObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class CatalogSnapshotTest {

  @Test
  public void roundTripSerialization() throws JsonProcessingException {
    CatalogSnapshot original =
        new CatalogSnapshot(
            CatalogSnapshot.CURRENT_VERSION,
            "default",
            "2026-05-20T19:45:00Z",
            List.of(new CatalogSnapshot.NamespaceEntry("n/flowers", Map.of())),
            List.of(
                new CatalogSnapshot.TableEntry(
                    "t/flowers/iris",
                    Map.of(
                        "table_type",
                        "ICEBERG",
                        "metadata_location",
                        "s3://bucket1/flowers/iris/metadata/00002-uuid.metadata.json",
                        "previous_metadata_location",
                        "s3://bucket1/flowers/iris/metadata/00001-uuid.metadata.json"))));

    String json = RESTObjectMapper.mapper().writeValueAsString(original);
    assertThat(json).contains("\"catalog_name\":\"default\"");
    assertThat(json).contains("\"exported_at\":\"2026-05-20T19:45:00Z\"");
    assertThat(json).contains("\"metadata_location\"");

    CatalogSnapshot restored = RESTObjectMapper.mapper().readValue(json, CatalogSnapshot.class);
    assertThat(restored).isEqualTo(original);
  }
}
