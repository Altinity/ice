/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.testsupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

public class StaticKeyManagementClientTest {

  private static final byte[] KEY = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);

  @Test
  public void testWrapUnwrapRoundTrip() {
    StaticKeyManagementClient kms = new StaticKeyManagementClient();
    ByteBuffer wrapped = kms.wrapKey(ByteBuffer.wrap(KEY), "key-a");

    assertThat(wrapped.remaining()).isGreaterThan(KEY.length);
    // a fresh instance must be able to unwrap (Spark and ice run in separate processes)
    ByteBuffer unwrapped = new StaticKeyManagementClient().unwrapKey(wrapped, "key-a");
    byte[] actual = new byte[unwrapped.remaining()];
    unwrapped.get(actual);
    assertThat(actual).isEqualTo(KEY);
  }

  @Test
  public void testUnwrapWithWrongKeyIdFails() {
    StaticKeyManagementClient kms = new StaticKeyManagementClient();
    ByteBuffer wrapped = kms.wrapKey(ByteBuffer.wrap(KEY), "key-a");

    assertThatThrownBy(() -> kms.unwrapKey(wrapped, "key-b")).isInstanceOf(RuntimeException.class);
  }

  @Test
  public void testTamperedWrappedKeyFails() {
    StaticKeyManagementClient kms = new StaticKeyManagementClient();
    ByteBuffer wrapped = kms.wrapKey(ByteBuffer.wrap(KEY), "key-a");
    byte[] bytes = new byte[wrapped.remaining()];
    wrapped.get(bytes);
    bytes[bytes.length - 1] ^= 1;

    assertThatThrownBy(() -> kms.unwrapKey(ByteBuffer.wrap(bytes), "key-a"))
        .isInstanceOf(RuntimeException.class);
  }
}
