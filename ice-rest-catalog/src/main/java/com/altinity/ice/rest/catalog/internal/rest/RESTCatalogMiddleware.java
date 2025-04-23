package com.altinity.ice.rest.catalog.internal.rest;

public abstract class RESTCatalogMiddleware implements RESTCatalogHandler {

  protected final RESTCatalogHandler next;

  public RESTCatalogMiddleware(RESTCatalogHandler next) {
    this.next = next;
  }
}
