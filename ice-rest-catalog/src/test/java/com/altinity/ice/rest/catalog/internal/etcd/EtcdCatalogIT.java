/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.etcd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.etcd.jetcd.KV;
import io.etcd.jetcd.Txn;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class EtcdCatalogIT {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));

  @SuppressWarnings("rawtypes")
  private final GenericContainer etcd =
      new GenericContainer("milvusdb/etcd:3.5.21-r2")
          .withExposedPorts(2379, 2380)
          .withEnv("ALLOW_NONE_AUTHENTICATION", "yes");

  private EtcdCatalog catalog;
  private Consumer<KV> preKvtx;

  @BeforeClass
  public void setUp() {
    etcd.start();
    String uri = "http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379);
    catalog =
        new EtcdCatalog("default", uri, "/foo", new InMemoryFileIO()) {

          @Override
          protected Txn kvtx() {
            if (preKvtx != null) {
              var x = preKvtx;
              preKvtx = null;
              x.accept(this.kv);
            }
            return super.kvtx();
          }
        };
  }

  @AfterClass
  public void tearDown() throws Exception {
    catalog.close();
    etcd.stop();
  }

  @Test
  public void testNamespaceCreateDelete() {
    String ns = rand();
    Namespace parent = Namespace.of(ns);
    List<Namespace> namespaceList =
        IntStream.range(0, 2).mapToObj(i -> Namespace.of(parent.toString(), "ns" + i)).toList();
    catalog.createNamespace(parent);
    assertThatThrownBy(() -> catalog.createNamespace(parent))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");
    namespaceList.forEach(namespace -> catalog.createNamespace(namespace));
    assertThat(catalog.listNamespaces(parent)).hasSize(2);
    assertThat(catalog.listNamespaces(parent))
        .isEqualTo(List.of(Namespace.of(ns, "ns0"), Namespace.of(ns, "ns1")));
    catalog.listNamespaces(parent).forEach(namespace -> catalog.dropNamespace(namespace));
    assertThat(catalog.listNamespaces(parent)).isEmpty();
  }

  @Test
  public void testNamespaceCreateConcurrent() {
    String ns = rand();
    preKvtx =
        kv -> {
          catalog.createNamespace(Namespace.of(ns));
        };
    assertThatThrownBy(() -> catalog.createNamespace(Namespace.of(ns)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("commit failed");
  }

  @Test
  public void testNamespaceDeleteWithChildNamespace() {
    String ns = rand();
    catalog.createNamespace(Namespace.of(ns));
    catalog.createNamespace(Namespace.of(ns, "a"));
    assertThatThrownBy(() -> catalog.dropNamespace(Namespace.of(ns)))
        .isInstanceOf(NamespaceNotEmptyException.class);
  }

  @Test
  public void testNamespaceDeleteWithTable() {
    String ns = rand();
    catalog.createNamespace(Namespace.of(ns));
    catalog.createTable(TableIdentifier.of(ns, "a"), SCHEMA);
    assertThatThrownBy(() -> catalog.dropNamespace(Namespace.of(ns)))
        .isInstanceOf(NamespaceNotEmptyException.class);
  }

  @Test
  public void testNoSuchNamespace() {
    assertThatThrownBy(() -> catalog.createNamespace(Namespace.of("a", "b")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Parent namespace does not exist");
    assertThatThrownBy(() -> catalog.listNamespaces(Namespace.of("a")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist");
    assertThatThrownBy(() -> catalog.loadNamespaceMetadata(Namespace.of("a")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist");
    assertThatThrownBy(() -> catalog.dropNamespace(Namespace.of("a")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist");
    assertThatThrownBy(() -> catalog.setProperties(Namespace.of("a"), Map.of()))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist");
    assertThatThrownBy(() -> catalog.removeProperties(Namespace.of("a"), Set.of()))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist");
    assertThatThrownBy(() -> catalog.listTables(Namespace.of("a")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist");
  }

  @Test
  public void testNamespaceInvalid() {
    assertThatThrownBy(() -> catalog.createNamespace(Namespace.of("a", "", "b")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must not be empty");
    assertThatThrownBy(() -> catalog.createNamespace(Namespace.of("a", "b.c")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must not contain dot");
  }

  @Test
  public void testNamespaceMetadata() {
    String ns = rand();
    Namespace namespace = Namespace.of(ns);

    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    catalog.createNamespace(namespace, properties);
    assertThat(catalog.loadNamespaceMetadata(namespace)).isEqualTo(properties);

    properties.put("key3", "val3");
    properties.put("key2", "updated_val2");
    assertThat(catalog.setProperties(namespace, properties)).isTrue();
    assertThat(catalog.setProperties(namespace, properties)).isFalse();
    assertThat(catalog.loadNamespaceMetadata(namespace)).isEqualTo(properties);

    properties.remove("key3");
    assertThat(catalog.removeProperties(namespace, Sets.newHashSet("key3"))).isTrue();
    assertThat(catalog.removeProperties(namespace, Sets.newHashSet("key3"))).isFalse();
    assertThat(catalog.loadNamespaceMetadata(namespace)).isEqualTo(properties);
  }

  @Test
  public void testTableCreateDelete() {
    String ns = rand();
    Namespace namespace = Namespace.of(ns);
    catalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "t1");
    catalog.createTable(tableIdentifier, SCHEMA);
    assertThatThrownBy(() -> catalog.createTable(tableIdentifier, SCHEMA))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");
    catalog.createTable(TableIdentifier.of(namespace, "t2"), SCHEMA);
    assertThat(catalog.listTables(namespace)).hasSize(2);
    assertThat(catalog.listTables(namespace))
        .isEqualTo(List.of(TableIdentifier.of(ns, "t1"), TableIdentifier.of(ns, "t2")));
    catalog.listTables(namespace).forEach(table -> catalog.dropTable(table));
    assertThat(catalog.listTables(namespace)).isEmpty();
  }

  /*
  @Test
  public void testTablePurge() {
    String ns = rand();
    Namespace namespace = Namespace.of(ns);
    catalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "t1");
    catalog.createTable(tableIdentifier, SCHEMA);
    catalog.dropTable(tableIdentifier, true);
  }
  */

  @Test
  public void testNoSuchTable() {
    String ns = rand();
    assertThatThrownBy(() -> catalog.createTable(TableIdentifier.of(ns, "b"), SCHEMA))
        .isInstanceOf(NoSuchNamespaceException.class);
    assertThatThrownBy(() -> catalog.dropTable(TableIdentifier.of("a", "b")))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Table does not exist");
  }

  @Test
  public void testTableInvalid() {
    String ns = rand();
    Namespace namespace = Namespace.of(ns);
    catalog.createNamespace(namespace);
    assertThatThrownBy(
            () -> catalog.createTable(TableIdentifier.of(Namespace.empty(), "a"), SCHEMA))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must not be empty");
    assertThatThrownBy(() -> catalog.createTable(TableIdentifier.of(namespace, "a.b"), SCHEMA))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must not contain dot");
  }

  @Test
  public void testTableRename() {
    String ns = rand();
    Namespace namespace = Namespace.of(ns);
    catalog.createNamespace(namespace);
    TableIdentifier from = TableIdentifier.of(namespace, "bar");
    catalog.createTable(from, SCHEMA);
    Table registeringTable = catalog.loadTable(from); // FIXME

    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((BaseMetastoreTableOperations) ops).currentMetadataLocation();

    assertThatThrownBy(() -> catalog.renameTable(TableIdentifier.of(namespace, "a"), from))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("does not exist");

    assertThatThrownBy(() -> catalog.renameTable(from, from))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");

    TableIdentifier to = TableIdentifier.of(namespace, "baz");
    catalog.renameTable(from, to);

    assertThatThrownBy(() -> catalog.loadTable(from))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("does not exist");

    Table renamedTable = catalog.loadTable(to);
    TableOperations renamedTableOps = ((HasTableOperations) renamedTable).operations();
    String renamedTableMetadataLocation =
        ((BaseMetastoreTableOperations) renamedTableOps).currentMetadataLocation();

    assertThat(renamedTableMetadataLocation)
        .as("metadata location should be copied to new table entry")
        .isEqualTo(metadataLocation);
  }

  @Test
  public void testTableUpdate() {
    String ns = rand();
    Namespace namespace = Namespace.of(ns);
    catalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "bar");
    catalog.createTable(tableIdentifier, SCHEMA);
    Table table = catalog.loadTable(tableIdentifier);
    table.updateSchema().addColumn("data", Types.StringType.get()).commit();
    table.refresh();
    assertThat(table.schema().columns()).hasSize(2);
  }

  @Test
  public void testTableRegister() {
    String ns = rand();
    Namespace namespace = Namespace.of(ns);
    catalog.createNamespace(namespace);
    TableIdentifier identifier = TableIdentifier.of(namespace, "bar");
    catalog.createTable(identifier, SCHEMA);
    Table registeringTable = catalog.loadTable(identifier); // FIXME
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((BaseMetastoreTableOperations) ops).currentMetadataLocation();
    assertThatThrownBy(() -> catalog.registerTable(identifier, metadataLocation))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");
    assertThat(catalog.dropTable(identifier, false)).isTrue();
    Table registeredTable = catalog.registerTable(identifier, metadataLocation);
    assertThat(registeredTable).isNotNull();
    String expectedMetadataLocation =
        ((HasTableOperations) registeredTable).operations().current().metadataFileLocation();
    assertThat(metadataLocation).isEqualTo(expectedMetadataLocation);
    assertThat(catalog.loadTable(identifier)).isNotNull();
    assertThat(catalog.dropTable(identifier)).isTrue();
    assertThat(catalog.dropNamespace(namespace)).isTrue();
  }

  private static String rand() {
    return UUID.randomUUID().toString();
  }
}
