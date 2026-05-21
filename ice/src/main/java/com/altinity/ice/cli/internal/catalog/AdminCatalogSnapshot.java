/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.catalog;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;

/** Client-side DTO for the catalog export snapshot, used with {@link HTTPClient}. */
public record AdminCatalogSnapshot(
    int version,
    @JsonProperty("catalog_name") String catalogName,
    @JsonProperty("exported_at") String exportedAt,
    List<NamespaceEntry> namespaces,
    List<TableEntry> tables)
    implements RESTRequest, RESTResponse {

  @Override
  public void validate() {}

  public record NamespaceEntry(String key, Map<String, String> value) {}

  public record TableEntry(String key, Map<String, String> value) {}
}
