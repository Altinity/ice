/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.altinity.ice.rest.catalog.internal.auth.Session;
import com.altinity.ice.rest.catalog.internal.config.Config;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Covers staged table create (REST {@code stageCreate} + commit) which must use {@link
 * org.apache.iceberg.rest.CatalogHandlers#updateTable} create path instead of {@link
 * org.apache.iceberg.catalog.Catalog#loadTable(TableIdentifier)} first. Uses an in-memory {@code
 * jdbc:sqlite} metastore (same pattern as {@link
 * com.altinity.ice.rest.catalog.RESTCatalogTestBase}).
 */
public class RESTCatalogAdapterCreateIT {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private Path warehouseDir;
  private Catalog catalog;
  private RESTCatalogAdapter adapter;
  private Session session;

  @BeforeClass
  public void beforeClass() throws IOException {
    warehouseDir = Files.createTempDirectory("restcat-create-it-");
    String warehouseUri = warehouseDir.toUri().toString();
    if (!warehouseUri.endsWith("/")) {
      warehouseUri = warehouseUri + "/";
    }
    Config config =
        new Config(
            "localhost:8080",
            "localhost:8081",
            null,
            "it",
            "jdbc:sqlite::memory:",
            warehouseUri,
            null,
            null,
            null,
            new Config.AnonymousAccess(true, new Config.AccessConfig(false, null)),
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    catalog = CatalogUtil.buildIcebergCatalog("it", config.toIcebergConfig(), null);
    adapter = new RESTCatalogAdapter(catalog);
    session = new Session("it", false, null);
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (catalog instanceof Closeable c) {
      c.close();
    }
    if (warehouseDir != null) {
      try (var walk = Files.walk(warehouseDir)) {
        walk.sorted((a, b) -> -a.compareTo(b))
            .forEach(
                p -> {
                  try {
                    Files.deleteIfExists(p);
                  } catch (IOException ignored) {
                    // best-effort cleanup
                  }
                });
      }
    }
  }

  private static String shortId() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }

  @Test
  public void stagedCreateCommitRegistersTable() {
    String nsName = "ns_" + shortId();
    Namespace ns = Namespace.of(nsName);
    ((SupportsNamespaces) catalog).createNamespace(ns);
    String tableName = "t_" + shortId();
    TableIdentifier ident = TableIdentifier.of(ns, tableName);

    CreateTableRequest stageReq =
        CreateTableRequest.builder().withName(tableName).withSchema(SCHEMA).stageCreate().build();
    stageReq.validate();

    Map<String, String> createVars = Map.of("namespace", RESTUtil.encodeNamespace(ns));
    LoadTableResponse staged =
        adapter.handle(session, Route.CREATE_TABLE, createVars, stageReq, LoadTableResponse.class);
    assertThat(staged.tableMetadata()).isNotNull();

    List<MetadataUpdate> updates = staged.tableMetadata().changes();
    UpdateTableRequest commitReq =
        UpdateTableRequest.create(ident, UpdateRequirements.forCreateTable(updates), updates);
    commitReq.validate();

    Map<String, String> updateVars =
        Map.of(
            "namespace", RESTUtil.encodeNamespace(ns),
            "table", RESTUtil.encodeString(tableName));
    LoadTableResponse committed =
        adapter.handle(session, Route.UPDATE_TABLE, updateVars, commitReq, LoadTableResponse.class);
    assertThat(committed.tableMetadata()).isNotNull();
    assertThat(catalog.tableExists(ident)).isTrue();
    assertThat(catalog.loadTable(ident).schema().sameSchema(SCHEMA)).isTrue();
  }

  @Test
  public void stagedCreateCommitDuplicateFails() {
    String nsName = "ns_" + shortId();
    Namespace ns = Namespace.of(nsName);
    ((SupportsNamespaces) catalog).createNamespace(ns);
    String tableName = "t_" + shortId();
    TableIdentifier ident = TableIdentifier.of(ns, tableName);

    CreateTableRequest stageReq =
        CreateTableRequest.builder().withName(tableName).withSchema(SCHEMA).stageCreate().build();
    stageReq.validate();

    Map<String, String> createVars = Map.of("namespace", RESTUtil.encodeNamespace(ns));
    LoadTableResponse staged =
        adapter.handle(session, Route.CREATE_TABLE, createVars, stageReq, LoadTableResponse.class);

    List<MetadataUpdate> updates = staged.tableMetadata().changes();
    UpdateTableRequest commitReq =
        UpdateTableRequest.create(ident, UpdateRequirements.forCreateTable(updates), updates);
    commitReq.validate();

    Map<String, String> updateVars =
        Map.of(
            "namespace", RESTUtil.encodeNamespace(ns),
            "table", RESTUtil.encodeString(tableName));

    adapter.handle(session, Route.UPDATE_TABLE, updateVars, commitReq, LoadTableResponse.class);
    assertThat(catalog.tableExists(ident)).isTrue();

    assertThatThrownBy(
            () ->
                adapter.handle(
                    session, Route.UPDATE_TABLE, updateVars, commitReq, LoadTableResponse.class))
        .isInstanceOfAny(AlreadyExistsException.class, CommitFailedException.class);
  }

  @Test
  public void routeFromMatchesStagedCreatePaths() {
    assertThat(Route.from(HTTPRequest.HTTPMethod.POST, "v1/namespaces/ns1/tables").first())
        .isEqualTo(Route.CREATE_TABLE);
    assertThat(Route.from(HTTPRequest.HTTPMethod.POST, "v1/namespaces/ns1/tables/t1").first())
        .isEqualTo(Route.UPDATE_TABLE);
  }
}
