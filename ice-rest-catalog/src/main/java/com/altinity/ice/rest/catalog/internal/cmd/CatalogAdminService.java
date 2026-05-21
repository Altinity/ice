/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.cmd;

import com.altinity.ice.internal.strings.Strings;
import com.altinity.ice.rest.catalog.internal.etcd.EtcdCatalog;
import com.altinity.ice.rest.catalog.internal.rest.RESTObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.exceptions.RuntimeIOException;

/** Catalog registry export/import for etcd-backed catalogs. */
public final class CatalogAdminService {

  private CatalogAdminService() {}

  public static CatalogSnapshot export(
      Catalog catalog, String catalogName, String namespaceFilter) {
    EtcdCatalog etcdCatalog = requireEtcdCatalog(catalog);
    List<CatalogSnapshot.NamespaceEntry> namespaces = new ArrayList<>();
    for (EtcdCatalog.CatalogKv kv : etcdCatalog.listAllNamespaceKvs()) {
      if (!matchesNamespaceFilter(kv.key(), namespaceFilter, etcdCatalog)) {
        continue;
      }
      namespaces.add(new CatalogSnapshot.NamespaceEntry(kv.key(), parseJsonMap(kv.value())));
    }

    List<CatalogSnapshot.TableEntry> tables = new ArrayList<>();
    for (EtcdCatalog.CatalogKv kv : etcdCatalog.listAllTableKvs(namespaceFilter)) {
      tables.add(new CatalogSnapshot.TableEntry(kv.key(), parseJsonMap(kv.value())));
    }

    return new CatalogSnapshot(
        CatalogSnapshot.CURRENT_VERSION, catalogName, Instant.now().toString(), namespaces, tables);
  }

  public static CatalogImportResult importSnapshot(
      Catalog catalog, CatalogSnapshot snapshot, boolean dryRun, boolean overwrite) {
    EtcdCatalog etcdCatalog = requireEtcdCatalog(catalog);

    if (snapshot.version() != CatalogSnapshot.CURRENT_VERSION) {
      throw new IllegalArgumentException(
          "Unsupported snapshot version: "
              + snapshot.version()
              + " (expected "
              + CatalogSnapshot.CURRENT_VERSION
              + ")");
    }

    int created = 0;
    int skipped = 0;
    int overwritten = 0;

    if (snapshot.namespaces() != null) {
      for (CatalogSnapshot.NamespaceEntry entry : snapshot.namespaces()) {
        EtcdCatalog.PutCatalogKvResult result =
            etcdCatalog.putCatalogKv(entry.key(), marshal(entry.value()), overwrite, dryRun);
        switch (result) {
          case CREATED -> created++;
          case SKIPPED -> skipped++;
          case OVERWRITTEN -> overwritten++;
        }
      }
    }
    if (snapshot.tables() != null) {
      for (CatalogSnapshot.TableEntry entry : snapshot.tables()) {
        EtcdCatalog.PutCatalogKvResult result =
            etcdCatalog.putCatalogKv(entry.key(), marshal(entry.value()), overwrite, dryRun);
        switch (result) {
          case CREATED -> created++;
          case SKIPPED -> skipped++;
          case OVERWRITTEN -> overwritten++;
        }
      }
    }

    return new CatalogImportResult(
        created, skipped, overwritten, snapshot.catalogName(), snapshot.exportedAt());
  }

  private static EtcdCatalog requireEtcdCatalog(Catalog catalog) {
    if (!(catalog instanceof EtcdCatalog etcdCatalog)) {
      throw new IllegalArgumentException(
          "Catalog export/import requires an etcd-backed catalog (uri: etcd:... in config)");
    }
    return etcdCatalog;
  }

  private static boolean matchesNamespaceFilter(
      String namespaceKey, String namespaceFilter, EtcdCatalog catalog) {
    if (Strings.isNullOrEmpty(namespaceFilter)) {
      return true;
    }
    String prefix = catalogPrefixForFilter(catalog) + "n/";
    if (!namespaceKey.startsWith(prefix)) {
      return false;
    }
    String path = namespaceKey.substring(prefix.length());
    return path.equals(namespaceFilter) || path.startsWith(namespaceFilter + "/");
  }

  private static String catalogPrefixForFilter(EtcdCatalog catalog) {
    if ("default".equals(catalog.name())) {
      return "";
    }
    return catalog.name() + "/";
  }

  private static Map<String, String> parseJsonMap(String json) {
    try {
      return RESTObjectMapper.mapper().readValue(json, new TypeReference<>() {});
    } catch (JsonProcessingException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static String marshal(Map<String, String> value) {
    try {
      return RESTObjectMapper.mapper().writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new RuntimeIOException(e);
    }
  }
}
