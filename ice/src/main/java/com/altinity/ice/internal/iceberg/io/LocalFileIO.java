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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class LocalFileIO implements DelegateFileIO {

  public static final String LOCALFILEIO_PROP_WAREHOUSE = "localfileio.warehouse";

  public static final String LOCALFILEIO_PROP_BASEDIR = "localfileio.basedir";
  public static final String LOCALFILEIO_PROP_ALLOWACCESS = "localfileio.allowaccess";

  private String workDir;
  private Path warehousePath;
  private String locationPrefix;
  private List<Path> allowAccess = List.of(); // TODO: replace with trie

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
    this.workDir = resolveWorkdir(warehouse, properties.get(LOCALFILEIO_PROP_BASEDIR));
    Path warehousePath = resolveWarehousePath(warehouse, workDir);
    if (!Files.isDirectory(warehousePath)) {
      throw new IllegalArgumentException(
          String.format("\"%s\" must point to an existing directory", warehousePath));
    }
    try {
      this.warehousePath = warehousePath.toRealPath();
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
    var x = Strings.removeSuffix(Strings.removePrefix(warehouse, "file://"), "/");
    this.locationPrefix = "file://" + (!x.isEmpty() ? x + "/" : "");
    this.allowAccess =
        Arrays.stream(properties.getOrDefault(LOCALFILEIO_PROP_ALLOWACCESS, "").split(","))
            .map(s -> Strings.removePrefix(s.trim(), "file://"))
            .filter(s -> !s.isEmpty())
            .map(Paths::get)
            .toList();
  }

  /**
   * resolveWorkdir returns workdir based on whether warehouse is absolute or relative, e.g. <code>
   * resolveWorkdir("/foo/bar", null) -> "/"
   * resolveWorkdir("foo/bar", "/") -> "/"
   * resolveWorkdir("foo/bar", "/baz") -> "/baz"
   * resolveWorkdir("foo/bar", null) -> "$PWD"
   * </code>
   */
  public static String resolveWorkdir(String fileWarehouse, @Nullable String warehouseBaseDir) {
    String baseDir = Objects.requireNonNullElse(warehouseBaseDir, "");
    if (baseDir.isEmpty()) {
      baseDir =
          Strings.removePrefix(fileWarehouse, "file://").startsWith("/")
              ? "/"
              : System.getProperty("user.dir") /* workdir */;
    }
    return baseDir;
  }

  /**
   * resolveBasePath returns warehouse's absolute path, e.g. <code>
   * resolveBasePath("/foo/bar", "/") -> "/foo/bar"
   * resolveBasePath("foo/bar", "/") -> "/foo/bar"
   * resolveBasePath("foo/bar", "/baz") -> "/baz/foo/bar"
   * resolveBasePath("foo/bar", "$PWD") -> "$PWD/foo/bar"
   * </code>
   */
  public static Path resolveWarehousePath(String fileWarehouse, String workDir) {
    Path basePath =
        Paths.get(
                Strings.removeSuffix(workDir, "/")
                    + "/"
                    + Strings.removeSuffix(Strings.removePrefix(fileWarehouse, "file://"), "/"))
            .toAbsolutePath();
    // TODO: do no allow dir that contain subfolders other than data/metadata
    //       (unless force flag is used)
    if ("/".equals(basePath.toString())) {
      throw new IllegalArgumentException(
          String.format("\"%s\" cannot point to /", LOCALFILEIO_PROP_WAREHOUSE));
    }
    return basePath;
  }

  // TODO: refactor (resolve + all resolve* above); this feels way more complicated that it needs to
  // be
  private Path resolve(String userPath) {
    String relativeToBase = Strings.removePrefix(userPath, this.locationPrefix);
    if (relativeToBase.startsWith("file://")) {
      // userPath is not relative to locationPrefix (expected when --force-no-copy is used)
      // check if it's explicitly whitelisted by the user
      relativeToBase = Strings.removePrefix(relativeToBase, "file://");
      if (allowAccess.isEmpty()) {
        throw new SecurityException(
            String.format(
                "Access outside \"%s\" is not allowed: \"%s\" (add `localFileIOAllowAccess: [\"/dir\"]` to .ice.yaml to whitelist)",
                warehousePath, relativeToBase));
      }
      Path resolved;
      if (relativeToBase.startsWith("/")) {
        resolved = Paths.get(relativeToBase).toAbsolutePath();
      } else {
        resolved = Paths.get(workDir).resolve(relativeToBase).toAbsolutePath();
      }
      if (allowAccess.stream().noneMatch(resolved::startsWith)) {
        throw new SecurityException(
            String.format(
                "Access outside \"%s\" (plus \"%s\") is not allowed: \"%s\"",
                warehousePath,
                String.join("\", \"", allowAccess.stream().map(Path::toString).toList()),
                relativeToBase));
      }
      return resolved;
    }
    Path resolved = warehousePath.resolve(relativeToBase).normalize().toAbsolutePath();
    if (!resolved.startsWith(warehousePath)) {
      throw new SecurityException(
          String.format("Access outside \"%s\" is not allowed: \"%s\"", warehousePath, resolved));
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
    return locationPrefix + warehousePath.relativize(path);
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
