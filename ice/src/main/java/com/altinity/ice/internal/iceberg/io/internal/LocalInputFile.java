/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.altinity.ice.internal.iceberg.io.internal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

/** Extracted from {@link org.apache.iceberg.Files}. */
public class LocalInputFile implements InputFile {

  protected final File file;

  public LocalInputFile(File file) {
    this.file = file;
  }

  @Override
  public long getLength() {
    return file.length();
  }

  @Override
  public SeekableInputStream newStream() {
    try {
      return new SeekableFileInputStream(new RandomAccessFile(file, "r"));
    } catch (FileNotFoundException e) {
      throw new NotFoundException(e, "Failed to read file: %s", file);
    }
  }

  @Override
  public String location() {
    return file.toString();
  }

  @Override
  public boolean exists() {
    return file.exists();
  }

  @Override
  public String toString() {
    return location();
  }
}
