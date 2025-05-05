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

  public void add(String fileToRetry) throws IOException {
    w.append(fileToRetry).write("\n");
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
