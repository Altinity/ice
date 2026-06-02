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

import com.altinity.ice.cli.internal.util.UserInputParser;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.utils.Lazy;

public class AlterTable {

  private static final Logger logger = LoggerFactory.getLogger(AlterTable.class);

  private AlterTable() {}

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "op")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = AddColumn.class, name = "add_column"),
    @JsonSubTypes.Type(value = AlterColumn.class, name = "alter_column"),
    @JsonSubTypes.Type(value = RenameColumn.class, name = "rename_column"),
    @JsonSubTypes.Type(value = DropColumn.class, name = "drop_column"),
    @JsonSubTypes.Type(value = SetTblProperty.class, name = "set_tblproperty"),
    @JsonSubTypes.Type(value = RenameTo.class, name = "rename_to"),
    @JsonSubTypes.Type(value = DropPartitionField.class, name = "drop_partition_field"),
  })
  public abstract static class Update {}

  public static class AddColumn extends Update {
    private final String name;
    private final Type type;
    @Nullable private final String doc;
    @Nullable private final String after;
    @Nullable private final String before;
    private final boolean first;
    private final boolean required;
    @Nullable private final String initialDefault;

    public AddColumn(
        @JsonProperty(value = "name", required = true) String name,
        @JsonProperty(value = "type", required = true) String type,
        @JsonProperty("doc") @Nullable String doc,
        @JsonProperty("after") @Nullable String after,
        @JsonProperty("before") @Nullable String before,
        @JsonProperty("first") @Nullable Boolean first,
        @JsonProperty("required") @Nullable Boolean required,
        @JsonProperty("initial_default") @Nullable String initialDefault) {
      this.name = name;
      this.type = Types.fromPrimitiveString(type);
      this.doc = doc;
      this.after = after;
      this.before = before;
      this.first = first != null && first;
      this.required = required != null && required;
      this.initialDefault = initialDefault;
    }
  }

  public static class AlterColumn extends Update {
    private final String name;
    private final Type.PrimitiveType type;

    public AlterColumn(
        @JsonProperty(value = "name", required = true) String name,
        @JsonProperty(value = "type", required = true) String type) {
      this.name = name;
      this.type = Types.fromPrimitiveString(type);
    }
  }

  public static class RenameColumn extends Update {
    private final String name;
    private final String newName;

    public RenameColumn(
        @JsonProperty(value = "name", required = true) String name,
        @JsonProperty(value = "new_name", required = true) String newName) {
      this.name = name;
      this.newName = newName;
    }
  }

  public static class DropColumn extends Update {
    private final String name;

    public DropColumn(@JsonProperty(value = "name", required = true) String name) {
      this.name = name;
    }
  }

  public static class SetTblProperty extends Update {
    private final String key;
    private final String value;

    public SetTblProperty(
        @JsonProperty(value = "key", required = true) String key,
        @JsonProperty("value") String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class RenameTo extends Update {
    private final String newName;

    public RenameTo(@JsonProperty(value = "new_name", required = true) String newName) {
      this.newName = newName;
    }
  }

  public static class DropPartitionField extends Update {
    private final String name;

    public DropPartitionField(@JsonProperty(value = "name", required = true) String name) {
      this.name = name;
    }
  }

  public static void run(Catalog catalog, TableIdentifier tableId, List<Update> updates)
      throws IOException {
    if (updates.isEmpty()) {
      return;
    }

    Table table = catalog.loadTable(tableId);

    Transaction tx = table.newTransaction();
    Lazy<UpdateSchema> schemaUpdates = new Lazy<>(tx::updateSchema);
    Lazy<UpdateProperties> propertiesUpdates = new Lazy<>(tx::updateProperties);
    Lazy<UpdatePartitionSpec> partitionSpecUpdates = new Lazy<>(tx::updateSpec);
    RenameTo renameTo = null;
    for (Update update : updates) {
      switch (update) {
        case AddColumn up -> {
          // TODO: support nested columns
          UpdateSchema us = schemaUpdates.getValue();
          applyAddColumn(table, us, up);
          if (up.after != null) {
            us.moveAfter(up.name, up.after);
          } else if (up.before != null) {
            us.moveBefore(up.name, up.before);
          } else if (up.first) {
            us.moveFirst(up.name);
          }
        }
        case AlterColumn up -> {
          // TODO: support nested columns
          schemaUpdates.getValue().updateColumn(up.name, up.type);
        }
        case RenameColumn up -> {
          // TODO: support nested columns
          schemaUpdates.getValue().renameColumn(up.name, up.newName);
        }
        case DropColumn up -> {
          schemaUpdates.getValue().deleteColumn(up.name);
        }
        case SetTblProperty up -> {
          if (up.value != null) {
            propertiesUpdates.getValue().set(up.key, up.value);
          } else {
            propertiesUpdates.getValue().remove(up.key);
          }
        }
        case RenameTo up -> {
          renameTo = up;
        }
        case DropPartitionField up -> {
          partitionSpecUpdates.getValue().removeField(up.name);
        }
        default -> throw new UnsupportedOperationException();
      }
    }
    if (schemaUpdates.hasValue()) {
      schemaUpdates.getValue().commit();
    }
    if (propertiesUpdates.hasValue()) {
      propertiesUpdates.getValue().commit();
    }
    if (partitionSpecUpdates.hasValue()) {
      partitionSpecUpdates.getValue().commit();
    }
    tx.commitTransaction();
    if (renameTo != null) {
      catalog.renameTable(tableId, TableIdentifier.parse(renameTo.newName));
    }
    logger.info("Applied {} changes to table {}", updates.size(), tableId.toString());
  }

  private static void applyAddColumn(Table table, UpdateSchema us, AddColumn up) {
    if (up.required) {
      if (up.initialDefault != null) {
        Literal<?> defaultValue = UserInputParser.parseLiteral(up.type, up.initialDefault);
        if (up.doc != null) {
          us.addRequiredColumn(up.name, up.type, up.doc, defaultValue);
        } else {
          us.addRequiredColumn(up.name, up.type, defaultValue);
        }
      } else {
        if (tableHasData(table)) {
          throw new IllegalArgumentException(
              "Adding required column '"
                  + up.name
                  + "' without initial_default is not allowed when the table has existing data; "
                  + "provide initial_default");
        }
        // Empty table: Iceberg still rejects required adds without a default unless
        // incompatible changes are explicitly allowed (e.g. table has a snapshot but no files).
        us.allowIncompatibleChanges();
        if (up.doc != null) {
          us.addRequiredColumn(up.name, up.type, up.doc);
        } else {
          us.addRequiredColumn(up.name, up.type);
        }
      }
    } else {
      us.addColumn(up.name, up.type, up.doc);
    }
  }

  private static boolean tableHasData(Table table) {
    if (table.currentSnapshot() == null) {
      return false;
    }
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      return tasks.iterator().hasNext();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to scan table for existing data files", e);
    }
  }
}
