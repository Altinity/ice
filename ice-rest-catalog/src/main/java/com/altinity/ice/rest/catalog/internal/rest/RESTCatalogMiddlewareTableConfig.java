package com.altinity.ice.rest.catalog.internal.rest;

import com.altinity.ice.rest.catalog.internal.auth.Session;
import com.altinity.ice.rest.catalog.internal.config.Config;
import java.util.Map;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

public class RESTCatalogMiddlewareTableConfig extends RESTCatalogMiddleware {

  private final Map<String, String> defaults;

  public RESTCatalogMiddlewareTableConfig(RESTCatalogHandler next, Map<String, String> config) {
    super(next);
    this.defaults = Config.parseQuery(config.getOrDefault(Config.OPTION_TABLE_CONFIG, ""));
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
      tableConfig.putAll(defaults); // TODO: putIfAbsent
    }
    return restResponse;
  }
}
