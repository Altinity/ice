package com.altinity.ice.cmd;

import java.io.IOException;
import org.apache.iceberg.rest.RESTCatalog;

public final class Check {

  private Check() {}

  public static void run(RESTCatalog catalog) throws IOException {
    catalog.listNamespaces();
  }
}
