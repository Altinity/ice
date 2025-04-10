package com.altinity.ice.rest.catalog.internal.log;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.core.pattern.CompositeConverter;
import picocli.CommandLine;

/**
 * A Logback Pattern Layout that uses Picocli ANSI color heuristic to apply ANSI color only on the
 * terminal which supports it.
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
