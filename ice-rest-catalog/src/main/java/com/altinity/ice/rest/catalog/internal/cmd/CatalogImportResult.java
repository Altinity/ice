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

/** Summary returned by catalog import. */
public record CatalogImportResult(
    int created,
    int skipped,
    int overwritten,
    @JsonProperty("catalog_name") String catalogName,
    @JsonProperty("exported_at") String exportedAt) {}
