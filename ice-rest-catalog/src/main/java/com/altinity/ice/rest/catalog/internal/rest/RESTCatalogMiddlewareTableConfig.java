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
