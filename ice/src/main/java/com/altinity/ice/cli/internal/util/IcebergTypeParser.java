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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Parses type strings including complex types ({@code list<string>}, {@code map<string,long>},
 * {@code struct<a:string,b:long>}) into Iceberg {@link Type} instances.
 *
 * <p>Placeholder field IDs are assigned during parsing. Iceberg's {@code UpdateSchema.addColumn}
 * reassigns fresh IDs from the table's {@code lastColumnId} on commit, so placeholder values are
 * safe.
 */
public final class IcebergTypeParser {

  private IcebergTypeParser() {}

  public static Type parseType(String typeString) {
    return parseType(typeString, new AtomicInteger(0));
  }

  private static Type parseType(String typeString, AtomicInteger nextId) {
    String s = typeString.strip();
    if (s.isEmpty()) {
      throw new IllegalArgumentException("Type string must not be empty");
    }

    String lower = s.toLowerCase();

    if (lower.startsWith("list<")) {
      return parseList(s, nextId);
    }
    if (lower.startsWith("map<")) {
      return parseMap(s, nextId);
    }
    if (lower.startsWith("struct<")) {
      return parseStruct(s, nextId);
    }

    return Types.fromPrimitiveString(s);
  }

  private static Types.ListType parseList(String s, AtomicInteger nextId) {
    String inner = unwrapAngleBrackets(s, "list");
    int elementId = nextId.incrementAndGet();
    Type elementType = parseType(inner, nextId);
    return Types.ListType.ofOptional(elementId, elementType);
  }

  private static Types.MapType parseMap(String s, AtomicInteger nextId) {
    String inner = unwrapAngleBrackets(s, "map");
    List<String> parts = splitAtTopLevelComma(inner);
    if (parts.size() != 2) {
      throw new IllegalArgumentException(
          "map type requires exactly 2 type parameters (key and value), got: " + s);
    }
    int keyId = nextId.incrementAndGet();
    int valueId = nextId.incrementAndGet();
    Type keyType = parseType(parts.get(0), nextId);
    Type valueType = parseType(parts.get(1), nextId);
    return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
  }

  private static Types.StructType parseStruct(String s, AtomicInteger nextId) {
    String inner = unwrapAngleBrackets(s, "struct");
    List<String> fieldDefs = splitAtTopLevelComma(inner);
    List<Types.NestedField> fields = new ArrayList<>();
    for (String fieldDef : fieldDefs) {
      String trimmed = fieldDef.strip();
      int colonIdx = findFieldNameSeparator(trimmed);
      if (colonIdx < 0) {
        throw new IllegalArgumentException(
            "struct field must be in the form 'name:type', got: " + trimmed);
      }
      String fieldName = trimmed.substring(0, colonIdx).strip();
      String fieldTypeStr = trimmed.substring(colonIdx + 1).strip();
      if (fieldName.isEmpty()) {
        throw new IllegalArgumentException("struct field name must not be empty in: " + trimmed);
      }
      if (fieldTypeStr.isEmpty()) {
        throw new IllegalArgumentException("struct field type must not be empty in: " + trimmed);
      }
      int fieldId = nextId.incrementAndGet();
      Type fieldType = parseType(fieldTypeStr, nextId);
      fields.add(Types.NestedField.optional(fieldId, fieldName, fieldType));
    }
    return Types.StructType.of(fields);
  }

  /** Finds the first colon that separates name from type, skipping colons inside nested types. */
  private static int findFieldNameSeparator(String s) {
    int depth = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '<' || c == '(') {
        depth++;
      } else if (c == '>' || c == ')') {
        depth--;
      } else if (c == ':' && depth == 0) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Strips the prefix and the surrounding angle brackets, e.g. "list<string>" -> "string".
   * Validates that brackets are present and balanced.
   */
  private static String unwrapAngleBrackets(String s, String prefix) {
    String trimmed = s.strip();
    int prefixLen = prefix.length();
    if (trimmed.length() < prefixLen + 2
        || trimmed.charAt(prefixLen) != '<'
        || trimmed.charAt(trimmed.length() - 1) != '>') {
      throw new IllegalArgumentException(
          "Expected " + prefix + "<...> syntax, got: " + trimmed);
    }
    String inner = trimmed.substring(prefixLen + 1, trimmed.length() - 1).strip();
    if (inner.isEmpty()) {
      throw new IllegalArgumentException(prefix + "<> must not be empty");
    }
    return inner;
  }

  /** Splits on commas that are not nested inside {@code <...>} or {@code (...)}. */
  private static List<String> splitAtTopLevelComma(String s) {
    List<String> parts = new ArrayList<>();
    int depth = 0;
    int start = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '<' || c == '(') {
        depth++;
      } else if (c == '>' || c == ')') {
        depth--;
      } else if (c == ',' && depth == 0) {
        parts.add(s.substring(start, i));
        start = i + 1;
      }
    }
    parts.add(s.substring(start));
    return parts;
  }
}
