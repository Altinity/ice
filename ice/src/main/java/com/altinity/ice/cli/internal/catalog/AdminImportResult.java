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
import org.apache.iceberg.rest.RESTResponse;

/** Client-side DTO for the catalog import result, used with {@link HTTPClient}. */
public record AdminImportResult(
    int created,
    int skipped,
    int overwritten,
    @JsonProperty("catalog_name") String catalogName,
    @JsonProperty("exported_at") String exportedAt)
    implements RESTResponse {

  @Override
  public void validate() {}
}
