package com.altinity.ice.internal.strings;

public final class Strings {

  private Strings() {}

  // TODO: remove; this shouldn't be needed; also available from guava
  public static boolean isNullOrEmpty(String v) {
    return v == null || v.isEmpty();
  }

  public static String orDefault(String v, String def) {
    return v != null && !v.isEmpty() ? v : def;
  }

  public static String removePrefix(String text, String prefix) {
    return replacePrefix(text, prefix, "");
  }

  public static String replacePrefix(String text, String from, String to) {
    if (text.startsWith(from)) {
      return to + text.substring(from.length());
    }
    return text;
  }

  public static String removeSuffix(String text, String suffix) {
    if (text.endsWith(suffix)) {
      return removeSuffix(text, text.substring(0, text.length() - suffix.length()));
    }
    return text;
  }
}
