/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.cmd;

import java.io.IOException;
import org.apache.iceberg.rest.RESTCatalog;

public final class Check {

  private Check() {}

  public static void run(RESTCatalog catalog) throws IOException {
    catalog.listNamespaces();
  }
}
