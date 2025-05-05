/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.jetty;

import java.util.stream.Stream;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;

public final class ServerConfig {

  public static void setQuiet(Server s) {
    Stream.of(s.getConnectors())
        .flatMap(c -> c.getConnectionFactories().stream())
        .filter(cf -> cf instanceof HttpConnectionFactory)
        .forEach(
            cf -> {
              HttpConfiguration hc = ((HttpConnectionFactory) cf).getHttpConfiguration();
              hc.setSendServerVersion(false);
              hc.setSendDateHeader(false);
            });
  }
}
