/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.cmd;

import com.github.shyiko.skedule.Schedule;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import software.amazon.awssdk.services.sqs.model.Message;

/**
 * Files (and the messages that carried them) accumulated since the last Iceberg commit.
 *
 * <p>Keyed by message id so that a message redelivered before the commit does not end up twice in
 * the delete request, which SQS rejects for having duplicate entry ids.
 */
public final class InsertWatchBuffer {

  /**
   * Thresholds controlling how much data is accumulated before it is committed. Committing on every
   * poll produces one Iceberg snapshot (and at least one manifest) per poll, which makes readers
   * spend most of their time resolving metadata; accumulating first trades write latency for a
   * proportionally smaller metadata tree.
   */
  public record BatchOptions(@Nullable String commitSchedule, int maxFiles, long maxBytes) {

    public static final BatchOptions NONE = new BatchOptions(null, 0, 0);

    public BatchOptions {
      if (maxFiles < 0) {
        throw new IllegalArgumentException("--watch-max-files must be non-negative");
      }
      if (maxBytes < 0) {
        throw new IllegalArgumentException("--watch-max-bytes must be non-negative");
      }
      if (commitSchedule != null) {
        // Fail fast on a bad expression at startup rather than on the first poll.
        Schedule.parse(commitSchedule);
      }
    }

    public boolean enabled() {
      return commitSchedule != null || maxFiles > 0 || maxBytes > 0;
    }
  }

  /**
   * Result of matching a poll batch against the input patterns: the files to insert (mapped to the
   * size reported by the S3 event), the messages that carried them, and the messages that can be
   * acknowledged immediately because nothing in them matched.
   */
  record FilterResult(
      Map<String, Long> files, List<Message> messages, List<Message> unmatchedMessages) {}

  private final Map<String, Long> files = new LinkedHashMap<>();
  private final Map<String, Message> messages = new LinkedHashMap<>();
  private long bytes;
  private Instant startedAt;

  void add(FilterResult r) {
    for (var e : r.files().entrySet()) {
      if (files.putIfAbsent(e.getKey(), e.getValue()) == null) {
        bytes += e.getValue();
      }
    }
    for (Message m : r.messages()) {
      messages.put(m.messageId(), m);
    }
    if (startedAt == null && !files.isEmpty()) {
      startedAt = Instant.now();
    }
  }

  boolean isEmpty() {
    return files.isEmpty();
  }

  int fileCount() {
    return files.size();
  }

  long bytes() {
    return bytes;
  }

  Duration age() {
    return startedAt == null ? Duration.ZERO : Duration.between(startedAt, Instant.now());
  }

  String[] fileArray() {
    return files.keySet().toArray(String[]::new);
  }

  List<Message> messageList() {
    return new ArrayList<>(messages.values());
  }

  void clear() {
    files.clear();
    messages.clear();
    bytes = 0;
    startedAt = null;
  }

  /** Returns the threshold that was reached, or null if the buffer should keep filling up. */
  @Nullable
  String flushTrigger(BatchOptions o, boolean scheduleDue) {
    if (files.isEmpty()) {
      return null;
    }
    if (!o.enabled()) {
      return "immediate";
    }
    if (o.maxFiles() > 0 && files.size() >= o.maxFiles()) {
      return "max_files";
    }
    if (o.maxBytes() > 0 && bytes >= o.maxBytes()) {
      return "max_bytes";
    }
    if (scheduleDue) {
      return "schedule";
    }
    return null;
  }

  static String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    }
    String[] units = {"KiB", "MiB", "GiB", "TiB", "PiB"};
    double v = bytes;
    int i = -1;
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i++;
    }
    return String.format(Locale.ENGLISH, "%.1f %s", v, units[i]);
  }
}
