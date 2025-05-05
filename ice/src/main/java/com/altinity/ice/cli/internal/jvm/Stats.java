/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.jvm;

import com.sun.management.UnixOperatingSystemMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.util.List;

public record Stats(MemoryUsage heap, MemoryUsage nonHeap, long fds, long runTime, long gcTime) {

  private static final long startTime = System.currentTimeMillis();

  public static Stats gather() {
    MemoryMXBean m = ManagementFactory.getMemoryMXBean();
    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    long openFDs =
        os instanceof UnixOperatingSystemMXBean
            ? ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount()
            : -1;
    long gcTime = getTotalGCTimeMillis(ManagementFactory.getGarbageCollectorMXBeans());
    return new Stats(
        m.getHeapMemoryUsage(),
        m.getNonHeapMemoryUsage(),
        openFDs,
        (System.currentTimeMillis() - startTime),
        gcTime);
  }

  private String toString(MemoryUsage s) {
    return "init="
        + (s.getInit() >> 10 >> 10)
        + "M,"
        + "used="
        + (s.getUsed() >> 10 >> 10)
        + "M,"
        + "committed="
        + (s.getCommitted() >> 10 >> 10)
        + "M"
        + (s.getMax() > 0
            ? ",free="
                + ((s.getMax() - s.getUsed()) >> 10 >> 10)
                + "M,"
                + "max="
                + (s.getMax() >> 10 >> 10)
                + "M"
            : "");
  }

  private static long getTotalGCTimeMillis(List<GarbageCollectorMXBean> beans) {
    long r = 0;
    for (GarbageCollectorMXBean b : beans) {
      long ms = b.getCollectionTime();
      if (ms != -1) {
        r += ms;
      }
    }
    return r;
  }

  @Override
  public String toString() {
    double gcPressure = Math.min((1.0 * gcTime / runTime) * 100, 100);
    return String.format(
        "heap: %s, gc pressure: %.2f%%(%dms), off-heap: %s, open fds: %d",
        toString(heap), gcPressure, gcTime, toString(nonHeap), fds);
  }
}
