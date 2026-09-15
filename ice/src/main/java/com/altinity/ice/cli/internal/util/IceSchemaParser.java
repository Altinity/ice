/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Parses a JSON table schema into an Iceberg {@link Schema}.
 *
 * <p>Example: {@code [{"name":"id","type":"long","required":true},{"name":"payload","type":
 * "unknown","doc":"not yet materialized"}]}
 *
 * <p>Unlike {@code --schema-from-parquet}, this supports types that Parquet schema inference cannot
 * represent (e.g. v3-only types such as {@code unknown}, {@code variant}, {@code geometry}).
 *
 * <p>Placeholder field IDs are assigned from a single counter shared across top-level and nested
 * fields so they are unique within the schema. Iceberg reassigns fresh IDs when the table is
 * created, so placeholder values are safe.
 */
public final class IceSchemaParser {

  private IceSchemaParser() {}

  public record IceField(
      @JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "type", required = true) String type,
      @JsonProperty("required") @Nullable Boolean required,
      @JsonProperty("doc") @Nullable String doc) {}

  public static Schema parse(String schemaJson) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    List<IceField> fields =
        mapper.readValue(
            schemaJson,
            mapper.getTypeFactory().constructCollectionType(List.class, IceField.class));
    return toSchema(fields);
  }

  public static Schema toSchema(List<IceField> fields) {
    if (fields == null || fields.isEmpty()) {
      throw new IllegalArgumentException("Schema must contain at least one field");
    }
    Set<String> names = new HashSet<>();
    List<Types.NestedField> columns = new ArrayList<>();
    AtomicInteger nextId = new AtomicInteger(0);
    for (IceField field : fields) {
      if (field.name() == null || field.name().isBlank()) {
        throw new IllegalArgumentException("Field name must not be empty: " + field);
      }
      if (field.type() == null || field.type().isBlank()) {
        throw new IllegalArgumentException("Field type must not be empty: " + field);
      }
      if (!names.add(field.name())) {
        throw new IllegalArgumentException("Duplicate field name: " + field.name());
      }
      int fieldId = nextId.incrementAndGet();
      Type type = IcebergTypeParser.parseType(field.type(), nextId);
      boolean required = field.required() != null && field.required();
      columns.add(
          required
              ? Types.NestedField.required(fieldId, field.name(), type, field.doc())
              : Types.NestedField.optional(fieldId, field.name(), type, field.doc()));
    }
    return new Schema(columns);
  }
}
