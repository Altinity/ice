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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class IcebergTypeParserTest {

  @DataProvider(name = "validTypes")
  public Object[][] validTypes() {
    return new Object[][] {
      {"string", Type.TypeID.STRING},
      {"long", Type.TypeID.LONG},
      {"int", Type.TypeID.INTEGER},
      {"boolean", Type.TypeID.BOOLEAN},
      {"float", Type.TypeID.FLOAT},
      {"double", Type.TypeID.DOUBLE},
      {"date", Type.TypeID.DATE},
      {"decimal(10,2)", Type.TypeID.DECIMAL},
      {"list<string>", Type.TypeID.LIST},
      {"list<long>", Type.TypeID.LIST},
      {"LIST<STRING>", Type.TypeID.LIST},
      {"  list< string >  ", Type.TypeID.LIST},
      {"map<string,long>", Type.TypeID.MAP},
      {"map< string , long >", Type.TypeID.MAP},
      {"map<string,list<int>>", Type.TypeID.MAP},
      {"struct<a:string,b:long>", Type.TypeID.STRUCT},
      {"struct<name:string,tags:list<string>>", Type.TypeID.STRUCT},
      {"list<struct<id:long,name:string>>", Type.TypeID.LIST},
    };
  }

  @Test(dataProvider = "validTypes")
  public void testParseType(String input, Type.TypeID expectedTypeId) {
    Type type = IcebergTypeParser.parseType(input);
    assertThat(type.typeId()).isEqualTo(expectedTypeId);
  }

  @Test
  public void testListElementIsOptional() {
    Types.ListType listType = (Types.ListType) IcebergTypeParser.parseType("list<string>");
    assertThat(listType.isElementOptional()).isTrue();
  }

  @Test
  public void testDecimalPrecisionAndScale() {
    Types.DecimalType decimal = (Types.DecimalType) IcebergTypeParser.parseType("decimal(10,2)");
    assertThat(decimal.precision()).isEqualTo(10);
    assertThat(decimal.scale()).isEqualTo(2);
  }

  @Test
  public void testNestedStructFields() {
    Types.ListType list =
        (Types.ListType) IcebergTypeParser.parseType("list<struct<id:long,name:string>>");
    Types.StructType inner = (Types.StructType) list.elementType();
    assertThat(inner.fields()).hasSize(2);
    assertThat(inner.field("id").type().typeId()).isEqualTo(Type.TypeID.LONG);
    assertThat(inner.field("name").type().typeId()).isEqualTo(Type.TypeID.STRING);
  }

  @DataProvider(name = "invalidTypes")
  public Object[][] invalidTypes() {
    return new Object[][] {
      {"", "must not be empty"},
      {"list<>", "must not be empty"},
      {"map<string>", "exactly 2 type parameters"},
      {"struct<name>", "name:type"},
      {"foobar", "Cannot parse type string"},
    };
  }

  @Test(dataProvider = "invalidTypes")
  public void testParseTypeThrows(String input, String expectedMessage) {
    assertThatThrownBy(() -> IcebergTypeParser.parseType(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }
}
