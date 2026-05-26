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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/** Portable JSON snapshot of etcd catalog registry keys ({@code n/} and {@code t/}). */
public record CatalogSnapshot(
    int version,
    @JsonProperty("catalog_name") String catalogName,
    @JsonProperty("exported_at") String exportedAt,
    List<NamespaceEntry> namespaces,
    List<TableEntry> tables) {

  public static final int CURRENT_VERSION = 1;

  public record NamespaceEntry(String key, Map<String, String> value) {}

  public record TableEntry(String key, Map<String, String> value) {}
}
