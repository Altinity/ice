/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.test;

import java.io.IOException;
import java.io.InputStream;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.InputFile;

public final class Resource {

  private Resource() {}

  public static InputFile asInputFile(String resourceName) throws IOException {
    try (InputStream is = Resource.class.getClassLoader().getResourceAsStream(resourceName)) {
      if (is == null) {
        return null;
      }
      byte[] bytes = is.readAllBytes();
      return new InMemoryInputFile(bytes);
    }
  }
}
