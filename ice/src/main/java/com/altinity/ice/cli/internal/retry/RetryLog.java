/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.retry;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class RetryLog implements Closeable {

  private final String f;
  private final Writer w;

  public RetryLog(String f) throws IOException {
    this.f = f;
    this.w = new BufferedWriter(new FileWriter(f + "~"));
  }

  // This method is expected to be thread-safe.
  public void add(String fileToRetry) throws IOException {
    //noinspection StringConcatenationInsideStringBufferAppend
    w.append(fileToRetry + "\n");
  }

  public void commit() throws IOException {
    w.close();
    Files.move(Paths.get(f + "~"), Paths.get(f), StandardCopyOption.REPLACE_EXISTING);
  }

  @Override
  public void close() throws IOException {
    w.close();
  }
}
