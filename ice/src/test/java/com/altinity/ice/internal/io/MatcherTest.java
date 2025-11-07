/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.internal.io;

import static org.testng.Assert.*;

import java.util.Arrays;
import org.testng.annotations.Test;

public class MatcherTest {

  @Test
  public void testMatches() {
    record TestCase(String[] patterns, String[] input, String[] expectedOutput) {}
    for (var t :
        new TestCase[] {
          new TestCase(
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/*.parquet"
              },
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.not-quite-parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.parquet/not-quite-parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-1.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-2/part-2.parquet",
              },
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-1.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-2/part-2.parquet",
              }),
          new TestCase(
              new String[] {"*/data/*.parquet"},
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/metadata/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/data/date=2025-01-01/part-0.parquet",
              },
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/data/date=2025-01-01/part-0.parquet"
              }),
          new TestCase(
              new String[] {"*/data/*.parquet", "!*/data/external/*.parquet"},
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/data/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/data/external/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/foodata/external/part-0.parquet",
              },
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/data/date=2025-01-01/part-0.parquet"
              }),
          new TestCase(
              new String[] {"!*/data/external/*.parquet", "!*/data/external.bk/*.parquet"},
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/data/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/data/external/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/foodata/external/part-0.parquet",
              },
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/data/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/foodata/external/part-0.parquet"
              }),
        }) {
      var p = Matcher.from(t.patterns);
      assertEquals(Arrays.stream(t.input).filter(p).toArray(), t.expectedOutput);
    }
  }
}
