/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.internal.logback;

import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Emits the JVM process id for use in log patterns as {@code %processId}. Logback does not ship
 * this conversion word in {@code logback-classic} 1.5.x; we register it from {@link
 * ColorAwarePatternLayout}.
 */
public class ProcessIdClassicConverter extends ClassicConverter {

  private static final String PID = String.valueOf(ProcessHandle.current().pid());

  @Override
  public String convert(ILoggingEvent event) {
    return PID;
  }
}
