/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.iceberg.io;

import com.altinity.ice.cli.internal.config.Config;
import com.altinity.ice.internal.strings.Strings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.Lazy;

// TODO: refactor: this entire class is a train wreck
public final class Input {

  private static final Logger logger = LoggerFactory.getLogger(Input.class);

  private Input() {}

  private static final Lazy<Path> tempHTTPCacheDir =
      new Lazy<>(
          () -> {
            Path dir;
            try {
              dir = Files.createTempDirectory("ice-http-cache-");
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            Runtime.getRuntime()
                .addShutdownHook(
                    new Thread(
                        () -> {
                          boolean ok = true;
                          File dirFile = dir.toFile();
                          for (File file : dirFile.listFiles()) {
                            ok = file.delete() && ok;
                          }
                          ok = dirFile.delete() && ok;
                          if (!ok) {
                            logger.warn("Failed to clean up http cache dir " + dir);
                          }
                        }));
            return dir;
          });

  public static FileIO newIO(String path, Table table, Lazy<S3Client> s3ClientLazy) {
    FileIO io = null;
    if (path.startsWith("s3://") && (table == null || !path.startsWith(table.location()))) {
      io = new S3FileIO(s3ClientLazy::getValue);
    }
    return io;
  }

  // TODO: turn into FileIO?
  // TODO: list() that supports wildcards in case of file:// and s3://
  // TODO: clear cached files on process exit
  // FIXME: method named "get" with side effects... classic
  public static org.apache.iceberg.io.InputFile newFile(String path, RESTCatalog catalog, FileIO io)
      throws IOException {
    return switch (path) {
      case String s when (s.startsWith("http:") || s.startsWith("https:")) -> {
        // FIXME: use dedicated client
        String name = DigestUtils.sha256Hex(s);
        // FIXME: we don't really need to cache it if response contain Content-Length
        String httpCachePath = catalog.properties().get(Config.OPTION_HTTP_CACHE);
        if (Strings.isNullOrEmpty(httpCachePath)) {
          httpCachePath = tempHTTPCacheDir.getValue().toString();
        }
        Path dst = Paths.get(httpCachePath, name);
        if (!Files.exists(dst)) {
          createParentDirs(dst.toFile());
          String tempName = name + "~";
          Path tmp = Paths.get(httpCachePath, tempName);
          // Clean up any existing temp file from previous interrupted runs
          if (Files.exists(tmp)) {
            Files.delete(tmp);
          }
          try (InputStream in = URI.create(s).toURL().openStream()) {
            Files.copy(in, tmp);
          }
          Files.move(tmp, dst);
        }
        yield org.apache.iceberg.Files.localInput(dst.toFile());
      }
      case String s when s.startsWith("s3:") -> {
        // TODO: remove
        if (io == null) {
          throw new UnsupportedOperationException(
              "s3:// is not not yet supported in this code path");
        }
        yield io.newInputFile(path);
      } // org.apache.iceberg.aws.s3.S3InputFile
      default -> {
        var p = path;
        if (p.startsWith("file://")) {
          p = p.replaceFirst("file://", "");
        }
        yield org.apache.iceberg.Files.localInput(p);
      }
    };
  }

  // TODO: just use guava
  private static void createParentDirs(File file) throws IOException {
    File parent = file.getCanonicalFile().getParentFile();
    if (parent != null) {
      //noinspection ResultOfMethodCallIgnored
      parent.mkdirs();
      if (!parent.isDirectory()) {
        throw new IOException("Unable to create parent directories of " + file);
      }
    }
  }
}
