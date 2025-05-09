/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

public class MainTest {

  @Test
  public void testSortOrderJsonParsing() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    // Test single sort order
    String singleSortJson =
        """
            {
                "column": "name",
                "desc": true,
                "nullFirst": true
            }
            """;

    Main.IceSortOrder singleSort = mapper.readValue(singleSortJson, Main.IceSortOrder.class);
    assertEquals(singleSort.column(), "name");
    assertTrue(singleSort.desc());
    assertTrue(singleSort.nullFirst());

    // Test array of sort orders
    String multipleSortJson =
        """
            [
                {
                    "column": "name",
                    "desc": true,
                    "nullFirst": true
                },
                {
                    "column": "age",
                    "desc": false,
                    "nullFirst": false
                }
            ]
            """;

    Main.IceSortOrder[] multipleSorts =
        mapper.readValue(multipleSortJson, Main.IceSortOrder[].class);
    assertEquals(multipleSorts.length, 2);

    // Verify first sort order
    assertEquals(multipleSorts[0].column(), "name");
    assertTrue(multipleSorts[0].desc());
    assertTrue(multipleSorts[0].nullFirst());

    // Verify second sort order
    assertEquals(multipleSorts[1].column(), "age");
    assertFalse(multipleSorts[1].desc());
    assertFalse(multipleSorts[1].nullFirst());
  }

  @Test
  public void testSortOrderJsonParsingWithMissingFields() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    // Test with missing nullFirst field (should default to false)
    String json =
        """
            {
                "column": "name",
                "desc": true
            }
            """;

    Main.IceSortOrder sort = mapper.readValue(json, Main.IceSortOrder.class);
    assertEquals(sort.column(), "name");
    assertTrue(sort.desc());
    assertFalse(sort.nullFirst());
  }

  @Test(expectedExceptions = Exception.class)
  public void testSortOrderJsonParsingWithInvalidJson() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    // Test with invalid JSON
    String invalidJson =
        """
            {
                "column": "name",
                "desc": "not-a-boolean"
            }
            """;

    mapper.readValue(invalidJson, Main.IceSortOrder.class);
  }
}
