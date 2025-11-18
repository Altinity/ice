/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.iceberg.rest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.client5.http.ssl.TlsSocketStrategy;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;

public class RESTCatalogFactory {

  public static RESTCatalog create(byte[] caCrt, boolean sslVerify) {
    if (caCrt == null && sslVerify) {
      return new RESTCatalog();
    }
    SSLContext sslContext;
    try {
      if (!sslVerify) {
        sslContext = createInsecureSSLContext();
      } else {
        sslContext = loadCABundle(caCrt);
      }
    } catch (CertificateException
        | KeyStoreException
        | IOException
        | NoSuchAlgorithmException
        | KeyManagementException e) {
      throw new RuntimeException(e);
    }
    HostnameVerifier hostnameVerifier =
        sslVerify ? HttpsSupport.getDefaultHostnameVerifier() : (hostname, session) -> true;
    TlsSocketStrategy tlsSocketStrategy =
        new DefaultClientTlsStrategy(sslContext, hostnameVerifier);
    return new RESTCatalog(
        SessionCatalog.SessionContext.createEmpty(),
        x ->
            HTTPClient.builder(x)
                .uri(x.get(CatalogProperties.URI))
                .withTlsSocketStrategy(tlsSocketStrategy)
                .build());
  }

  private static SSLContext loadCABundle(byte[] caCrt)
      throws CertificateException,
          KeyStoreException,
          IOException,
          NoSuchAlgorithmException,
          KeyManagementException {
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    Collection<? extends Certificate> certs =
        cf.generateCertificates(new ByteArrayInputStream(caCrt));
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null, null);
    int i = 0;
    for (java.security.cert.Certificate c : certs) {
      trustStore.setCertificateEntry("custom-" + i++, c);
    }
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());
    return sslContext;
  }

  private static SSLContext createInsecureSSLContext()
      throws NoSuchAlgorithmException, KeyManagementException {
    TrustManager[] trustAllCerts =
        new TrustManager[] {
          new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
              return new X509Certificate[0];
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {}

            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
          }
        };
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, trustAllCerts, new SecureRandom());
    return sslContext;
  }
}
