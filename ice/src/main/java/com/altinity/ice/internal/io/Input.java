package com.altinity.ice.internal.io;

import com.altinity.ice.internal.crypto.Hash;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.Lazy;

// TODO: refactor: this entire class is a trainwreck
public final class Input {

  private Input() {}

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
        String name = Hash.sha256(s);
        // FIXME: we don't really need to cache it if response contain Content-Length
        String httpCachePath = catalog.properties().get("ice.http.cache");
        if (httpCachePath == null || httpCachePath.isEmpty()) {
          throw new IllegalArgumentException(
              "ice.http.cache must currently be set when ingesting from https?://");
        }
        Path dst = Paths.get(httpCachePath, name);
        if (!Files.exists(dst)) {
          createParentDirs(dst.toFile());
          String tempName = name + "~";
          Path tmp = Paths.get(httpCachePath, tempName);
          try (InputStream in = URI.create(s).toURL().openStream()) {
            // FIXME: race with another copy
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
