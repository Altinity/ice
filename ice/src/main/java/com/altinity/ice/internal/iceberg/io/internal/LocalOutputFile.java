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
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

/** Extracted from {@link org.apache.iceberg.Files}. */
public class LocalOutputFile implements OutputFile {

  protected final File file;

  public LocalOutputFile(File file) {
    this.file = file;
  }

  @Override
  public PositionOutputStream create() {
    if (file.exists()) {
      throw new AlreadyExistsException("File already exists: %s", file);
    }

    if (!file.getParentFile().isDirectory() && !file.getParentFile().mkdirs()) {
      throw new RuntimeIOException(
          "Failed to create the file's directory at %s.", file.getParentFile().getAbsolutePath());
    }

    try {
      return new PositionFileOutputStream(file, new RandomAccessFile(file, "rw"));
    } catch (FileNotFoundException e) {
      throw new NotFoundException(e, "Failed to create file: %s", file);
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    if (file.exists()) {
      if (!file.delete()) {
        throw new RuntimeIOException("Failed to delete: %s", file);
      }
    }
    return create();
  }

  @Override
  public String location() {
    return file.toString();
  }

  @Override
  public InputFile toInputFile() {
    return new LocalInputFile(file);
  }

  @Override
  public String toString() {
    return location();
  }
}
