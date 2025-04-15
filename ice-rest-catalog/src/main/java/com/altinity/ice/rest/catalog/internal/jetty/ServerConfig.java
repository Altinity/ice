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
