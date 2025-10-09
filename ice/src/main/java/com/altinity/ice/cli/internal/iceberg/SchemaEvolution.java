/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class SchemaEvolution {

  /**
   * Checks if subsetSchema is a subset of supersetSchema. All required fields in supersetSchema
   * must be present in subsetSchema for the result to be true.
   */
  public static boolean isSubset(Schema subsetSchema, Schema supersetSchema) {
    // Check all required superset fields are present in subset
    for (Types.NestedField tField : supersetSchema.columns()) {
      Types.NestedField sField = subsetSchema.findField(tField.fieldId());
      if (sField == null) {
        if (tField.isOptional()) {
          continue;
        }
        return false;
      }
      if (tField.isRequired() && sField.isOptional()
          || !requiredFieldsPresent(tField.type(), sField.type())) {
        return false;
      }
    }
    // Check subset doesn't contain any fields not present in subset
    for (Types.NestedField sField : subsetSchema.columns()) {
      Types.NestedField tField = supersetSchema.findField(sField.fieldId());
      if (tField == null) {
        return false;
      }
      if (!strictSubset(sField.type(), tField.type())) {
        return false;
      }
    }
    return true;
  }

  public static boolean requiredFieldsPresent(Type scanType, Type targetType) {
    // Struct
    if (scanType.isStructType()) {
      if (!targetType.isStructType()) {
        return false;
      }
      Types.StructType sStruct = scanType.asStructType();
      Types.StructType tStruct = targetType.asStructType();
      for (Types.NestedField sField : sStruct.fields()) {
        Types.NestedField tField = tStruct.field(sField.fieldId());
        if (tField == null) {
          if (sField.isOptional()) {
            continue;
          }
          return false;
        }
        if (sField.isRequired() && tField.isOptional()
            || !requiredFieldsPresent(sField.type(), tField.type())) {
          return false;
        }
      }
      return true;
    }

    // List
    if (scanType.isListType()) {
      if (!targetType.isListType()) {
        return false;
      }
      Types.ListType sList = scanType.asListType();
      Types.ListType tList = targetType.asListType();
      return !(sList.isElementRequired() && tList.isElementOptional())
          && requiredFieldsPresent(sList.elementType(), tList.elementType());
    }

    // Map
    if (scanType.isMapType()) {
      if (!targetType.isMapType()) {
        return false;
      }
      Types.MapType sMap = scanType.asMapType();
      Types.MapType tMap = targetType.asMapType();
      return !(sMap.isValueRequired() && tMap.isValueOptional())
          && requiredFieldsPresent(sMap.keyType(), tMap.keyType())
          && requiredFieldsPresent(sMap.valueType(), tMap.valueType());
    }

    // we don't consider type here: it's checked in strictSubset
    return true;
  }

  private static boolean strictSubset(Type subsetType, Type supersetType) {
    if (subsetType.isPrimitiveType()) {
      return supersetType.isPrimitiveType()
          && checkPrimitiveTypeCompatible(subsetType, supersetType);
    }

    // Struct
    if (subsetType.isStructType()) {
      if (!supersetType.isStructType()) {
        return false;
      }
      Types.StructType sStruct = subsetType.asStructType();
      Types.StructType tStruct = supersetType.asStructType();
      for (Types.NestedField sField : sStruct.fields()) {
        Types.NestedField tField = tStruct.field(sField.fieldId());
        if (tField == null || !strictSubset(sField.type(), tField.type())) {
          return false;
        }
      }
      return true;
    }

    // List
    if (subsetType.isListType()) {
      if (!supersetType.isListType()) {
        return false;
      }
      Types.ListType sList = subsetType.asListType();
      Types.ListType tList = supersetType.asListType();
      return strictSubset(sList.elementType(), tList.elementType());
    }

    // Map
    if (subsetType.isMapType()) {
      if (!supersetType.isMapType()) {
        return false;
      }
      Types.MapType sMap = subsetType.asMapType();
      Types.MapType tMap = supersetType.asMapType();
      return strictSubset(sMap.keyType(), tMap.keyType())
          && strictSubset(sMap.valueType(), tMap.valueType());
    }

    throw new IllegalArgumentException("subtype isn't primitive, struct, list or map");
  }

  /**
   * Checks if primitive types are compatible allowing promotions:
   *
   * <pre>
   *   INT -> LONG
   *   FLOAT -> DOUBLE
   *   DECIMAL(P, S) -> DECIMAL(Px, S) if Px > P
   * </pre>
   *
   * @see <a href="https://iceberg.apache.org/spec/#schema-evolution">Iceberg / Table Spec / Schema
   *     Evolution</a>
   */
  private static boolean checkPrimitiveTypeCompatible(Type from, Type to) {
    if (from instanceof Types.IntegerType) {
      return (to instanceof Types.IntegerType) || (to instanceof Types.LongType);
    }
    if (from instanceof Types.FloatType) {
      return (to instanceof Types.FloatType) || (to instanceof Types.DoubleType);
    }
    if (from instanceof Types.DecimalType fromDec) {
      return to instanceof Types.DecimalType toDec && toDec.precision() >= fromDec.precision();
    }
    return from.equals(to);
  }
}
