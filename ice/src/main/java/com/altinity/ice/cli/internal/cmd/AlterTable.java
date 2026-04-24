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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Literal;
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
    private final boolean nullable;
    @Nullable private final Literal<?> defaultValue;

    public AddColumn(
        @JsonProperty(value = "name", required = true) String name,
        @JsonProperty(value = "type", required = true) String type,
        @JsonProperty("doc") @Nullable String doc,
        @JsonProperty("nullable") @Nullable Boolean nullable,
        @JsonProperty("default") @Nullable Object defaultValue) {
      this.name = name;
      this.type = Types.fromPrimitiveString(type);
      this.doc = doc;
      this.nullable = nullable == null || nullable;
      this.defaultValue = coerceDefault(this.type, defaultValue);
    }
  }

  // Coerce a JSON literal (number / boolean / string) into an Iceberg Literal of the requested
  // type. For numeric, boolean and string types we build the Literal directly; for complex
  // primitives (date/time/timestamp/decimal/uuid/binary) we accept a string form and let
  // Iceberg parse it via Literal<CharSequence>.to(type).
  @Nullable
  private static Literal<?> coerceDefault(Type type, @Nullable Object jsonValue) {
    if (jsonValue == null) {
      return null;
    }
    try {
      switch (type.typeId()) {
        case BOOLEAN:
          if (jsonValue instanceof Boolean b) {
            return Literal.of(b);
          }
          return Literal.of(Boolean.parseBoolean(jsonValue.toString()));
        case INTEGER:
          return Literal.of(((Number) numberOrParse(jsonValue)).intValue());
        case LONG:
          return Literal.of(((Number) numberOrParse(jsonValue)).longValue());
        case FLOAT:
          return Literal.of(((Number) numberOrParse(jsonValue)).floatValue());
        case DOUBLE:
          return Literal.of(((Number) numberOrParse(jsonValue)).doubleValue());
        case STRING:
          return Literal.of(jsonValue.toString());
        case BINARY:
        case FIXED:
          return Literal.of(ByteBuffer.wrap(jsonValue.toString().getBytes(StandardCharsets.UTF_8)));
        case DECIMAL:
          return Literal.of(new BigDecimal(jsonValue.toString())).to(type);
        case UUID:
          return Literal.of(UUID.fromString(jsonValue.toString()));
        case DATE:
        case TIME:
        case TIMESTAMP:
        case TIMESTAMP_NANO:
          // Let Iceberg parse the canonical string form (e.g. "2025-01-01",
          // "2025-01-01T00:00:00", "2025-01-01T00:00:00+00:00").
          Literal<?> parsed = Literal.of(jsonValue.toString()).to(type);
          if (parsed == null) {
            throw new IllegalArgumentException(
                String.format("Cannot parse default value '%s' as %s", jsonValue, type));
          }
          return parsed;
        default:
          throw new IllegalArgumentException(
              String.format("Defaults not supported for type %s", type));
      }
    } catch (ClassCastException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Cannot coerce default value '%s' to type %s", jsonValue, type), e);
    }
  }

  // Accept either a JSON number (already a Number) or a string parseable as a number.
  private static Number numberOrParse(Object jsonValue) {
    if (jsonValue instanceof Number n) {
      return n;
    }
    String s = jsonValue.toString();
    // Use BigDecimal to accept any numeric form; downstream casts narrow to int/long/float/double.
    return new BigDecimal(s);
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
          if (up.nullable) {
            if (up.defaultValue == null) {
              us.addColumn(up.name, up.type, up.doc);
            } else {
              us.addColumn(up.name, up.type, up.doc, up.defaultValue);
            }
          } else {
            if (up.defaultValue == null) {
              us.addRequiredColumn(up.name, up.type, up.doc);
            } else {
              us.addRequiredColumn(up.name, up.type, up.doc, up.defaultValue);
            }
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
}
