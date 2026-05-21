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

import com.altinity.ice.cli.internal.iceberg.rest.RESTCatalogFactory;
import com.altinity.ice.internal.strings.Strings;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.client5.http.ssl.TlsSocketStrategy;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.auth.AuthSession;

/** HTTP client for ice-rest-catalog admin catalog export/import endpoints. */
public final class CatalogAdminClient implements AutoCloseable {

  private final HTTPClient httpClient;

  private CatalogAdminClient(HTTPClient httpClient) {
    this.httpClient = httpClient;
  }

  public static CatalogAdminClient create(
      String adminBaseUri, String bearerToken, byte[] caCrt, boolean sslVerify) throws IOException {
    TlsSocketStrategy tlsSocketStrategy = null;
    if (caCrt != null || !sslVerify) {
      try {
        SSLContext sslContext =
            !sslVerify
                ? RESTCatalogFactory.createInsecureSSLContext()
                : RESTCatalogFactory.loadCABundle(caCrt);
        HostnameVerifier hostnameVerifier =
            sslVerify ? HttpsSupport.getDefaultHostnameVerifier() : (hostname, session) -> true;
        tlsSocketStrategy = new DefaultClientTlsStrategy(sslContext, hostnameVerifier);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    HTTPClient.Builder builder =
        HTTPClient.builder(Map.of()).uri(adminBaseUri).withAuthSession(AuthSession.EMPTY);
    if (tlsSocketStrategy != null) {
      builder.withTlsSocketStrategy(tlsSocketStrategy);
    }
    if (!Strings.isNullOrEmpty(bearerToken)) {
      String auth =
          bearerToken.toLowerCase().startsWith("bearer ") ? bearerToken : "Bearer " + bearerToken;
      builder.withHeader("Authorization", auth);
    }
    return new CatalogAdminClient(builder.build());
  }

  public AdminCatalogSnapshot catalogExport(String namespace) {
    Map<String, String> query = new LinkedHashMap<>();
    if (!Strings.isNullOrEmpty(namespace)) {
      query.put("namespace", namespace);
    }
    return httpClient.get(
        "admin/v1/catalog-export", query, AdminCatalogSnapshot.class, Map.of(), e -> {});
  }

  public AdminImportResult catalogImport(
      AdminCatalogSnapshot snapshot, boolean dryRun, boolean overwrite) {
    StringBuilder path = new StringBuilder("admin/v1/catalog-import");
    String sep = "?";
    if (dryRun) {
      path.append(sep).append("dry-run=true");
      sep = "&";
    }
    if (overwrite) {
      path.append(sep).append("overwrite=true");
    }
    return httpClient.post(
        path.toString(), snapshot, AdminImportResult.class, Map.of(), e -> {});
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }
}
