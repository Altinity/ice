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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.altinity.ice.cli.Main.PartitionFilter;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.testng.annotations.Test;

public class DeleteTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testToPredicateOperations() {
    assertThat(((UnboundPredicate<?>) Delete.toPredicate("f", 5, PartitionFilter.Op.EQUALS)).op())
        .isEqualTo(Expression.Operation.EQ);
    assertThat(
            ((UnboundPredicate<?>) Delete.toPredicate("f", 5, PartitionFilter.Op.LESS_THAN)).op())
        .isEqualTo(Expression.Operation.LT);
    assertThat(
            ((UnboundPredicate<?>) Delete.toPredicate("f", 5, PartitionFilter.Op.GREATER_THAN))
                .op())
        .isEqualTo(Expression.Operation.GT);
    assertThat(
            ((UnboundPredicate<?>)
                    Delete.toPredicate("f", 5, PartitionFilter.Op.LESS_THAN_OR_EQUAL))
                .op())
        .isEqualTo(Expression.Operation.LT_EQ);
    assertThat(
            ((UnboundPredicate<?>)
                    Delete.toPredicate("f", 5, PartitionFilter.Op.GREATER_THAN_OR_EQUAL))
                .op())
        .isEqualTo(Expression.Operation.GT_EQ);
  }

  @Test
  public void testOpDefaultsToEquals() {
    PartitionFilter pf = new PartitionFilter("f", List.of(1), null);
    assertThat(pf.op()).isEqualTo(PartitionFilter.Op.EQUALS);
  }

  @Test
  public void testDeserializeWithoutOpDefaultsToEquals() throws Exception {
    PartitionFilter pf =
        mapper.readValue("{\"name\": \"f\", \"values\": [1]}", PartitionFilter.class);
    assertThat(pf.name()).isEqualTo("f");
    assertThat(pf.op()).isEqualTo(PartitionFilter.Op.EQUALS);
  }

  @Test
  public void testDeserializeWithOp() throws Exception {
    PartitionFilter pf =
        mapper.readValue(
            "{\"name\": \"f\", \"op\": \"greater_than_or_equal\", \"values\": [1]}",
            PartitionFilter.class);
    assertThat(pf.op()).isEqualTo(PartitionFilter.Op.GREATER_THAN_OR_EQUAL);
  }

  @Test
  public void testDeserializeUnknownOpThrows() {
    assertThatThrownBy(
            () ->
                mapper.readValue(
                    "{\"name\": \"f\", \"op\": \"between\", \"values\": [1]}",
                    PartitionFilter.class))
        .hasMessageContaining("unknown partition filter op");
  }
}
