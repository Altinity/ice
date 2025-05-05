/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.rest;

import com.altinity.ice.rest.catalog.internal.auth.Session;
import java.util.Map;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

public class RESTCatalogMiddlewareTableConfig extends RESTCatalogMiddleware {

  private final Map<String, String> defaults;

  public RESTCatalogMiddlewareTableConfig(RESTCatalogHandler next, Map<String, String> defaults) {
    super(next);
    this.defaults = defaults;
  }

  @Override
  public <T extends RESTResponse> T handle(
      Session session,
      Route route,
      Map<String, String> vars,
      Object requestBody,
      Class<T> responseType) {
    T restResponse = this.next.handle(session, route, vars, requestBody, responseType);
    if (restResponse instanceof LoadTableResponse loadTableResponse) {
      Map<String, String> tableConfig = loadTableResponse.config();
      for (var e : defaults.entrySet()) {
        tableConfig.putIfAbsent(e.getKey(), e.getValue());
      }
    }
    return restResponse;
  }
}
