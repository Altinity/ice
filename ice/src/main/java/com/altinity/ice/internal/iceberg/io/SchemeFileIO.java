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
package com.altinity.ice.internal.iceberg.io;

import com.altinity.ice.internal.strings.Strings;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileIO implementation that uses location scheme to choose the correct FileIO implementation.
 * Delegate FileIO implementations must implement the {@link DelegateFileIO} mixin interface,
 * otherwise initialization will fail.
 *
 * <p>This class is a modified version of {@link org.apache.iceberg.io.ResolvingFileIO}.
 */
public class SchemeFileIO implements DelegateFileIO {

  private static final Logger logger = LoggerFactory.getLogger(SchemeFileIO.class);

  private static final int BATCH_SIZE = 100_000;

  private static final String FILE_IO_IMPL = LocalFileIO.class.getName();
  private static final String S3_FILE_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";
  private static final String GCS_FILE_IO_IMPL = "org.apache.iceberg.gcp.gcs.GCSFileIO";
  private static final String ADLS_FILE_IO_IMPL = "org.apache.iceberg.azure.adlsv2.ADLSFileIO";

  private static final Map<String, String> SCHEME_TO_FILE_IO =
      ImmutableMap.of(
          "file", FILE_IO_IMPL,
          "s3", S3_FILE_IO_IMPL,
          "s3a", S3_FILE_IO_IMPL,
          "s3n", S3_FILE_IO_IMPL,
          "gs", GCS_FILE_IO_IMPL,
          "abfs", ADLS_FILE_IO_IMPL,
          "abfss", ADLS_FILE_IO_IMPL,
          "wasb", ADLS_FILE_IO_IMPL,
          "wasbs", ADLS_FILE_IO_IMPL);

  private final Map<String, DelegateFileIO> ioMap = Maps.newConcurrentMap();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final StackTraceElement[] createStack = Thread.currentThread().getStackTrace();

  private Map<String, String> properties = Map.of();

  @Override
  public InputFile newInputFile(String location) {
    return io(location).newInputFile(location);
  }

  @Override
  public InputFile newInputFile(String location, long length) {
    return io(location).newInputFile(location, length);
  }

  @Override
  public OutputFile newOutputFile(String location) {
    return io(location).newOutputFile(location);
  }

  @Override
  public void deleteFile(String location) {
    io(location).deleteFile(location);
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    Iterators.partition(pathsToDelete.iterator(), BATCH_SIZE)
        .forEachRemaining(
            partition -> {
              Map<DelegateFileIO, List<String>> pathByFileIO =
                  partition.stream().collect(Collectors.groupingBy(this::io));
              for (Map.Entry<DelegateFileIO, List<String>> entries : pathByFileIO.entrySet()) {
                DelegateFileIO io = entries.getKey();
                List<String> filePaths = entries.getValue();
                io.deleteFiles(filePaths);
              }
            });
  }

  @Override
  public Map<String, String> properties() {
    return properties; // properties is immutable
  }

  @Override
  public void initialize(Map<String, String> newProperties) {
    if (closed.get()) {
      throw new IllegalStateException();
    }
    this.properties = ImmutableMap.copyOf(newProperties);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      for (var io : ioMap.entrySet()) {
        try {
          io.getValue().close();
        } catch (Exception e) {
          logger.warn("Failed to close {} FileIO", io.getKey(), e);
        }
      }
      ioMap.clear();
    }
  }

  private DelegateFileIO io(String location) {
    String s = scheme(location);
    String impl = SCHEME_TO_FILE_IO.get(s);
    Preconditions.checkNotNull(
        impl, String.format("unsupported scheme \"%s\": \"%s\"", s, location));
    DelegateFileIO io = ioMap.get(impl);
    if (io != null) {
      return io;
    }
    return ioMap.computeIfAbsent(
        impl,
        key -> {
          Map<String, String> props = Maps.newHashMap(properties);
          props.put("init-creation-stacktrace", "false"); // ResolvingFileIO.createStack

          // s3.endpoint, client.region, ... defaults
          for (Map.Entry<String, String> e : new HashMap<>(props).entrySet()) {
            if (e.getKey().startsWith("ice.io.default.")) {
              String k = Strings.removePrefix(e.getKey(), "ice.io.default.");
              if (!props.containsKey(k)) {
                props.put(k, e.getValue());
              }
            }
          }

          logger.debug(
              "Iceberg configuration for {}/{}: {}",
              impl,
              key,
              props.entrySet().stream()
                  .map(
                      e ->
                          !e.getKey().contains("key") && !e.getKey().contains("authorization")
                              ? e.getKey() + "=" + e.getValue()
                              : e.getKey())
                  .sorted()
                  .collect(Collectors.joining(", ")));

          FileIO fileIO = CatalogUtil.loadFileIO(key, props, null);
          Preconditions.checkState(
              fileIO instanceof DelegateFileIO,
              "FileIO does not implement DelegateFileIO: " + fileIO.getClass().getName());
          return (DelegateFileIO) fileIO;
        });
  }

  private static String scheme(String location) {
    int colonPos = location.indexOf(":");
    if (colonPos > 0) {
      return location.substring(0, colonPos);
    }
    return null;
  }

  @SuppressWarnings({"removal"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed.get()) {
      close();
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      logger.warn("Unclosed ResolvingFileIO instance created by:\n\t{}", trace);
    }
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    return io(prefix).listPrefix(prefix);
  }

  @Override
  public void deletePrefix(String prefix) {
    io(prefix).deletePrefix(prefix);
  }
}
