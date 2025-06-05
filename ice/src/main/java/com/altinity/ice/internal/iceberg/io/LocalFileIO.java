/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.internal.iceberg.io;

import com.altinity.ice.internal.strings.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class LocalFileIO implements DelegateFileIO {

  // current workdir by default
  public static final String LOCALFILEIO_PROP_BASEDIR = "localfileio.basedir";
  public static final String LOCALFILEIO_PROP_WAREHOUSE =
      "localfileio.warehouse"; // e.g. file://path/to/warehouse/root/relative/to/localfileio.basedir
  private Path basePath;
  private String locationPrefix;

  @Override
  public void initialize(Map<String, String> properties) {
    String warehouse =
        properties.getOrDefault(
            LOCALFILEIO_PROP_WAREHOUSE,
            properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "file://"));
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(warehouse), "\"%s\" required", LOCALFILEIO_PROP_WAREHOUSE);
    Preconditions.checkArgument(
        warehouse.startsWith("file://"),
        "\"%s\" must start with file://",
        LOCALFILEIO_PROP_WAREHOUSE);
    String baseDir = properties.getOrDefault(LOCALFILEIO_PROP_BASEDIR, "");
    if (baseDir.isEmpty()) {
      baseDir = System.getProperty("user.dir");
    }
    if (!baseDir.isEmpty()) {
      baseDir = Strings.removeSuffix(baseDir, "/") + "/";
    }
    String base = baseDir + Strings.removeSuffix(Strings.removePrefix(warehouse, "file://"), "/");
    if (!Files.isDirectory(Paths.get(base))) {
      throw new IllegalArgumentException(
          String.format("\"%s\" must point to an existing directory", LOCALFILEIO_PROP_WAREHOUSE));
    }
    Path basePath;
    try {
      basePath = Paths.get(base).toRealPath();
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
    // TODO: do no allow dir that contain subfolders other than data/metadata (unless force flag is
    // used)
    if ("/".equals(basePath.toString())) {
      throw new IllegalArgumentException(
          String.format("\"%s\" cannot point to /", LOCALFILEIO_PROP_WAREHOUSE));
    }
    this.basePath = basePath;
    var x = Strings.removeSuffix(Strings.removePrefix(warehouse, "file://"), "/");
    this.locationPrefix = "file://" + (!x.isEmpty() ? x + "/" : "");
  }

  private Path resolve(String userPath) {
    String relativeToBase = Strings.removePrefix(userPath, this.locationPrefix);
    Path resolved = basePath.resolve(relativeToBase).normalize().toAbsolutePath();
    if (!resolved.startsWith(basePath)) {
      throw new SecurityException(String.format("Access outside \"%s\" is not allowed", resolved));
    }
    return resolved;
  }

  @Override
  public InputFile newInputFile(String path) {
    return new LocalInputFile(resolve(path).toFile());
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new LocalOutputFile(resolve(path).toFile());
  }

  @Override
  public void deleteFile(String path) {
    if (!resolve(path).toFile().delete()) {
      throw new RuntimeIOException("Failed to delete file: " + path);
    }
  }

  private String location(Path path) {
    return locationPrefix + basePath.relativize(path);
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    Path prefixPath = resolve(prefix);
    File prefixFile = prefixPath.toFile();
    if (!prefixFile.exists()) {
      return new ArrayList<>();
    }
    if (prefixFile.isFile()) {
      List<FileInfo> result = new ArrayList<>();
      result.add(
          new FileInfo(location(prefixPath), prefixFile.length(), prefixFile.lastModified()));
      return result;
    }
    try (Stream<Path> paths = Files.walk(prefixPath)) {
      return paths
          .filter(path -> path.toFile().isFile())
          .map(
              path -> {
                File file = path.toFile();
                return new FileInfo(location(path), file.length(), file.lastModified());
              })
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to list prefix: %s", prefix);
    }
  }

  @Override
  public void deletePrefix(String prefix) {
    Path prefixPath = resolve(prefix);
    File prefixFile = prefixPath.toFile();
    if (!prefixFile.exists()) {
      return;
    }
    if (prefixFile.isFile()) {
      deleteFile(location(prefixPath));
      return;
    }
    try (Stream<Path> paths = Files.walk(prefixPath)) {
      paths
          .sorted(
              Comparator.reverseOrder()) // sort in reverse order to delete files before directories
          .map(Path::toFile)
          .forEach(file -> deleteFile(location(file.toPath())));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete prefix: %s", prefix);
    }
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    int failedCount = 0;
    for (String path : pathsToDelete) {
      try {
        deleteFile(path); // deleteFile calls resolve() internally
      } catch (Exception e) {
        // TODO: log (otherwise e is lost)
        failedCount++;
      }
    }
    if (failedCount != 0) {
      throw new BulkDeletionFailureException(failedCount);
    }
  }

  private class LocalOutputFile
      extends com.altinity.ice.internal.iceberg.io.internal.LocalOutputFile {

    protected LocalOutputFile(File file) {
      super(file);
    }

    @Override
    public String location() {
      return LocalFileIO.this.location(file.toPath());
    }

    @Override
    public InputFile toInputFile() {
      return new LocalInputFile(file);
    }
  }

  private class LocalInputFile
      extends com.altinity.ice.internal.iceberg.io.internal.LocalInputFile {

    private LocalInputFile(File file) {
      super(file);
    }

    @Override
    public String location() {
      return LocalFileIO.this.location(file.toPath());
    }
  }
}
