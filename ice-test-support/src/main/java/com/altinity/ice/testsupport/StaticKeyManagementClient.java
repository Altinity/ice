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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import org.apache.iceberg.encryption.Ciphers;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.util.ByteBuffers;

/**
 * INSECURE {@link KeyManagementClient} for tests: the master key for a given key id is derived
 * deterministically from the key id itself, so anyone who knows the key id can unwrap keys.
 *
 * <p>Lets the ice CLI and Spark share "master keys" without a real KMS. Enable with catalog
 * property {@code encryption.kms-impl=com.altinity.ice.testsupport.StaticKeyManagementClient}.
 */
public class StaticKeyManagementClient implements KeyManagementClient {

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    Ciphers.AesGcmEncryptor encryptor = new Ciphers.AesGcmEncryptor(masterKey(wrappingKeyId));
    return ByteBuffer.wrap(encryptor.encrypt(ByteBuffers.toByteArray(key), aad(wrappingKeyId)));
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    Ciphers.AesGcmDecryptor decryptor = new Ciphers.AesGcmDecryptor(masterKey(wrappingKeyId));
    return ByteBuffer.wrap(
        decryptor.decrypt(ByteBuffers.toByteArray(wrappedKey), aad(wrappingKeyId)));
  }

  @Override
  public void initialize(Map<String, String> properties) {}

  static byte[] masterKey(String keyId) {
    try {
      return MessageDigest.getInstance("SHA-256")
          .digest(("ice-test-kms:" + keyId).getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static byte[] aad(String keyId) {
    return keyId.getBytes(StandardCharsets.UTF_8);
  }
}
