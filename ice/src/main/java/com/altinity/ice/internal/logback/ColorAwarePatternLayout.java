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

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.core.pattern.CompositeConverter;
import picocli.CommandLine;

/**
 * A Logback Pattern Layout that uses Picocli ANSI color heuristic to apply ANSI color only on the
 * terminals which support it. Code was taken from <a
 * href="https://stackoverflow.com/a/68633212">stackoverflow.com/a/68633212</a>.
 */
public class ColorAwarePatternLayout extends PatternLayout {

  static {
    if (!CommandLine.Help.Ansi.AUTO.enabled()) { // Usage of Picocli heuristic
      DEFAULT_CONVERTER_MAP.put("black", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("red", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("green", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("yellow", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("blue", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("magenta", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("cyan", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("white", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("gray", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("boldRed", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("boldGreen", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("boldYellow", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("boldBlue", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("boldMagenta", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("boldCyan", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("boldWhite", NoColorConverter.class.getName());
      DEFAULT_CONVERTER_MAP.put("highlight", NoColorConverter.class.getName());
    }
  }

  public static class NoColorConverter<E> extends CompositeConverter<E> {
    @Override
    protected String transform(E event, String in) {
      return in;
    }
  }
}
