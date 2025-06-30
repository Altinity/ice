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

import com.altinity.ice.internal.strings.Strings;
import com.altinity.ice.rest.catalog.internal.rest.RESTObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EtcdCatalog extends BaseMetastoreCatalog implements SupportsNamespaces {

  private static final Logger logger = LoggerFactory.getLogger(EtcdCatalog.class);

  private static final String NAMESPACE_PREFIX = "n/";
  private static final String TABLE_PREFIX = "t/";

  private final String catalogName;
  private final String warehouseLocation;
  private final Client client;
  protected final KV kv;
  private final FileIO io;

  public EtcdCatalog(String name, String uri, String warehouseLocation, FileIO io) {
    this.catalogName = name;
    Preconditions.checkArgument(
        name != null && !name.isBlank() && !name.contains("/"), "Invalid catalog name");
    this.warehouseLocation = LocationUtil.stripTrailingSlash(warehouseLocation);
    var etcdClient =
        Client.builder().endpoints(uri.split(",")).keepaliveWithoutCalls(false).build();
    this.client = etcdClient;
    this.kv = etcdClient.getKVClient();
    this.io = io;
  }

  // Used by EtcdCatalogTest to test concurrent modifications.
  protected Txn kvtx() {
    return kv.txn();
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    validateNamespace(namespace);
    if (namespaceExists(namespace)) {
      throw new AlreadyExistsException("Namespace already exists: %s", namespace);
    }

    String[] levels = namespace.levels();
    if (levels.length > 1) {
      Namespace parent = Namespace.of(Arrays.copyOf(levels, levels.length - 1));
      if (!namespaceExists(parent)) {
        throw new NoSuchNamespaceException("Parent namespace does not exist: %s", parent);
      }
    }

    ByteSequence key = byteSeq(namespaceKey(namespace));
    var value = byteSeq(marshal(metadata));

    TxnResponse txRes =
        unwrapCommit(
            kvtx()
                .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0)))
                .Then(Op.put(key, value, PutOption.DEFAULT))
                .commit());
    if (!txRes.isSucceeded()) {
      throw new CommitFailedException("commit failed");
    }
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    ByteSequence key = byteSeq(namespaceKey(namespace));
    return unwrap(kv.get(key, GetOption.builder().withCountOnly(true).build())).getCount() > 0;
  }

  private String namespaceKey(Namespace namespace) {
    return namespacePrefix() + namespaceToPath(namespace);
  }

  private String namespacePrefix() {
    if ("default".equals(catalogName)) { // for backward-compatibility
      return NAMESPACE_PREFIX;
    }
    return catalogName + "/" + NAMESPACE_PREFIX;
  }

  private static <T> T unwrapCommit(java.util.concurrent.CompletableFuture<T> x) {
    try {
      return x.get();
    } catch (InterruptedException | ExecutionException e) {
      // TODO: Thread.currentThread().interrupt();?
      throw new CommitStateUnknownException(e);
    }
  }

  private static <T> T unwrap(java.util.concurrent.CompletableFuture<T> x) {
    try {
      return x.get();
    } catch (InterruptedException | ExecutionException e) {
      // TODO: Thread.currentThread().interrupt();?
      throw new RuntimeIOException(new IOException(e));
    }
  }

  private static ByteSequence byteSeq(String value) {
    return ByteSequence.from(value, StandardCharsets.UTF_8);
  }

  private static String namespaceToPath(Namespace namespace) {
    return String.join("/", namespace.levels());
  }

  protected String marshal(Map<String, String> obj) {
    try {
      return RESTObjectMapper.mapper().writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeIOException(e);
    }
  }

  protected Map<String, String> unmarshal(String v) {
    try {
      return RESTObjectMapper.mapper().readValue(v, new TypeReference<>() {});
    } catch (JsonProcessingException e) {
      throw new RuntimeIOException(e);
    }
  }

  private void validateNamespace(Namespace namespace) {
    for (String level : namespace.levels()) {
      ValidationException.check(
          level != null && !level.isEmpty(), "Namespace level must not be empty: %s", namespace);
      ValidationException.check(
          !level.contains("."),
          "Namespace level must not contain dot, but found %s in %s",
          level,
          namespace);
    }
  }

  private void validateTableIdentifier(TableIdentifier identifier) {
    validateNamespace(identifier.namespace());
    ValidationException.check(
        identifier.hasNamespace(), "Table namespace must not be empty: %s", identifier);
    String tableName = identifier.name();
    ValidationException.check(
        !tableName.contains("."), "Table name must not contain dot: %s", tableName);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    validateNamespace(namespace);
    String prefix = namespaceKey(namespace);
    prefix = prefix.endsWith("/") ? prefix : prefix + "/";
    GetResponse res = unwrap(kv.get(byteSeq(prefix), GetOption.builder().isPrefix(true).build()));
    if (res.getKvs().isEmpty() && !namespace.isEmpty()) {
      if (!namespaceExists(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
    }
    return res.getKvs().stream()
        .map(
            k ->
                Namespace.of(
                    Strings.removePrefix(
                            k.getKey().toString(StandardCharsets.UTF_8), namespacePrefix())
                        .split("/")))
        .toList();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    return unmarshal(
        loadNamespaceMetadataRaw(namespace).getValue().toString(StandardCharsets.UTF_8));
  }

  private KeyValue loadNamespaceMetadataRaw(Namespace namespace) throws NoSuchNamespaceException {
    validateNamespace(namespace);
    ByteSequence key = byteSeq(namespaceKey(namespace));
    GetResponse res = unwrap(kv.get(key));
    if (res.getCount() == 0) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    return res.getKvs().getFirst();
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    validateNamespace(namespace);
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    if (!listNamespaces(namespace).isEmpty() || !listTables(namespace).isEmpty()) {
      throw new NamespaceNotEmptyException("Cannot drop non-empty namespace: " + namespace);
    }
    // FIXME: child ns/table might have been created since check ^
    unwrapCommit(kv.delete(byteSeq(namespaceKey(namespace))));
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    KeyValue entry = loadNamespaceMetadataRaw(namespace);
    Map<String, String> metadata = unmarshal(entry.getValue().toString(StandardCharsets.UTF_8));

    boolean changed = false;
    for (var p : properties.entrySet()) {
      if (!p.getValue().equals(metadata.get(p.getKey()))) {
        changed = true;
        break;
      }
    }
    if (!changed) {
      return false;
    }

    metadata.putAll(properties);

    ByteSequence key = byteSeq(namespaceKey(namespace));
    ByteSequence value = byteSeq(marshal(metadata));

    TxnResponse txRes =
        unwrapCommit(
            kvtx()
                .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(entry.getVersion())))
                .Then(Op.put(key, value, PutOption.DEFAULT))
                .commit());
    if (!txRes.isSucceeded()) {
      throw new CommitFailedException("commit failed");
    }
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    KeyValue entry = loadNamespaceMetadataRaw(namespace);
    Map<String, String> metadata = unmarshal(entry.getValue().toString(StandardCharsets.UTF_8));

    boolean changed = false;
    for (String property : properties) {
      if (metadata.containsKey(property)) {
        metadata.remove(property);
        changed = true;
      }
    }
    if (!changed) {
      return false;
    }

    ByteSequence key = byteSeq(namespaceKey(namespace));
    ByteSequence value = byteSeq(marshal(metadata));

    TxnResponse txRes =
        unwrapCommit(
            kvtx()
                .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(entry.getVersion())))
                .Then(Op.put(key, value, PutOption.DEFAULT))
                .commit());
    if (!txRes.isSucceeded()) {
      throw new CommitFailedException("commit failed");
    }
    return true;
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return String.format(
        "%s/%s/%s",
        warehouseLocation,
        String.join("/", tableIdentifier.namespace().levels()),
        tableIdentifier.name());
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    validateTableIdentifier(tableIdentifier);
    return new EtcdCatalogTableOperations(tableIdentifier);
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    validateNamespace(namespace);
    String prefix = tableKey(namespace);
    prefix = prefix.endsWith("/") ? prefix : prefix + "/";
    GetResponse res = unwrap(kv.get(byteSeq(prefix), GetOption.builder().isPrefix(true).build()));
    if (res.getKvs().isEmpty() && !namespace.isEmpty()) {
      if (!namespaceExists(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
    }
    return res.getKvs().stream()
        .map(
            k ->
                TableIdentifier.of(
                    Strings.removePrefix(k.getKey().toString(StandardCharsets.UTF_8), tablePrefix())
                        .split("/")))
        .toList();
  }

  private String tableKey(Namespace namespace) {
    return tablePrefix() + namespaceToPath(namespace);
  }

  private String tableKey(TableIdentifier identifier) {
    return tablePrefix() + tableIdentifierToPath(identifier);
  }

  private String tablePrefix() {
    if ("default".equals(catalogName)) { // for backward-compatibility
      return TABLE_PREFIX;
    }
    return catalogName + "/" + TABLE_PREFIX;
  }

  private static String tableIdentifierToPath(TableIdentifier identifier) {
    return String.join("/", identifier.namespace().levels()) + "/" + identifier.name();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    validateTableIdentifier(identifier);
    if (!tableExists(identifier)) {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    }

    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata = null;
    if (purge) {
      try {
        lastMetadata = ops.current();
      } catch (NotFoundException e) {
        logger.warn(
            "Failed to load table metadata for table: {}, continuing drop without purge",
            identifier,
            e);
      }
    }

    unwrapCommit(kv.delete(byteSeq(tableKey(identifier))));

    if (purge && lastMetadata != null) {
      CatalogUtil.dropTableData(ops.io(), lastMetadata);
      logger.info("Table {} data purged", identifier);
    }
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (!tableExists(from)) {
      throw new NoSuchTableException(
          "Cannot rename table %s to %s: %s does not exist", from, to, from);
    }
    if (tableExists(to)) {
      throw new AlreadyExistsException(
          "Cannot rename table %s to %s: %s already exists", from, to, to);
    }

    ByteSequence fromKey = byteSeq(tableKey(from));
    GetResponse fromObjRes = unwrap(kv.get(fromKey));
    KeyValue fromObj = fromObjRes.getKvs().getFirst();
    var data = unmarshal(fromObj.getValue().toString(StandardCharsets.UTF_8));

    var value = byteSeq(marshal(data));

    ByteSequence key = byteSeq(tableKey(to));

    TxnResponse txRes =
        unwrapCommit(
            kvtx()
                .If(
                    new Cmp(fromKey, Cmp.Op.EQUAL, CmpTarget.version(fromObj.getVersion())),
                    new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0)))
                .Then(
                    Op.delete(fromKey, DeleteOption.DEFAULT), Op.put(key, value, PutOption.DEFAULT))
                .commit());
    if (!txRes.isSucceeded()) {
      throw new CommitFailedException("commit failed");
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    client.close();
  }

  private class EtcdCatalogTableOperations extends BaseMetastoreTableOperations {

    private static final String TABLE_TYPE_VALUE =
        ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH);

    private final TableIdentifier tableIdentifier;
    private final String tableName;

    public EtcdCatalogTableOperations(TableIdentifier tableIdentifier) {
      this.tableIdentifier = tableIdentifier;
      this.tableName = tableIdentifier.toString();
    }

    @Override
    protected void doRefresh() {
      ByteSequence key = byteSeq(tableKey(tableIdentifier));
      GetResponse res = unwrap(kv.get(key));
      var found = res.getCount() != 0;
      if (found) {
        var metadata =
            unmarshal(res.getKvs().getFirst().getValue().toString(StandardCharsets.UTF_8));
        var metadataLocation = metadata.get(METADATA_LOCATION_PROP);
        if (metadataLocation != null) {
          refreshFromMetadataLocation(metadataLocation);
          return;
        }
      }
      disableRefresh();
    }

    @Override
    protected void doCommit(TableMetadata base, TableMetadata metadata) {
      boolean newTable = base == null;
      CommitStatus commitStatus = CommitStatus.FAILURE;
      String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);
      try {
        ByteSequence key = byteSeq(tableKey(tableIdentifier));
        GetResponse res = unwrap(kv.get(key));
        long version = 0;
        var found = res.getCount() != 0;
        Map<String, String> data;
        if (found) {
          if (newTable) {
            throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
          }
          KeyValue entry = res.getKvs().getFirst();
          version = entry.getVersion();
          data = unmarshal(entry.getValue().toString(StandardCharsets.UTF_8));
          // TODO: check location?
          data.put(PREVIOUS_METADATA_LOCATION_PROP, base.metadataFileLocation());
          data.put(METADATA_LOCATION_PROP, newMetadataLocation);
        } else {
          if (!namespaceExists(tableIdentifier.namespace())) {
            throw new NoSuchNamespaceException(
                "Namespace does not exist: %s", tableIdentifier.namespace());
          }
          data =
              Map.of(
                  TABLE_TYPE_PROP, TABLE_TYPE_VALUE, METADATA_LOCATION_PROP, newMetadataLocation);
        }
        ByteSequence value = byteSeq(marshal(data));
        TxnResponse txRes =
            unwrapCommit(
                kvtx()
                    .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(version)))
                    .Then(Op.put(key, value, PutOption.DEFAULT))
                    .commit());
        if (!txRes.isSucceeded()) {
          throw new CommitFailedException("commit failed");
        }
        commitStatus = CommitStatus.SUCCESS;
      } catch (CommitStateUnknownException e) {
        commitStatus = CommitStatus.UNKNOWN;
        throw e;
      } finally {
        if (commitStatus == CommitStatus.FAILURE) {
          try {
            io.deleteFile(newMetadataLocation);
          } catch (RuntimeException e) {
            logger.error("Failed to cleanup metadata file at {}", newMetadataLocation, e);
          }
        }
      }
    }

    @Override
    protected String tableName() {
      return tableName;
    }

    @Override
    public FileIO io() {
      return io;
    }
  }
}
