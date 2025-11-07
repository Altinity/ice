/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.internal.io;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;

public interface Matcher extends Predicate<String> {

  @Override
  boolean test(String key);

  static Matcher from(String... paths) {
    if (paths.length == 1) {
      return from(paths[0]);
    }
    if (paths.length == 0) {
      return none();
    }
    Map<Boolean, List<String>> notPartition =
        Arrays.stream(paths).collect(Collectors.partitioningBy(s -> s.startsWith("!")));
    List<String> or = notPartition.get(false);
    List<String> not = notPartition.get(true);
    if (not.isEmpty()) {
      return new Or(or.stream().map(Matcher::from).toArray(Matcher[]::new));
    }
    if (or.isEmpty()) {
      return new And(not.stream().map(Matcher::from).toArray(Matcher[]::new));
    }
    return new And(
        new Or(or.stream().map(Matcher::from).toArray(Matcher[]::new)),
        new And(not.stream().map(Matcher::from).toArray(Matcher[]::new)));
  }

  static Matcher from(String path) {
    if (path.startsWith("!")) {
      return new Not(from(path.substring(1)));
    }
    if (path.equals("*")) {
      return any();
    }
    int wildcardIndex = path.indexOf('*');
    if (wildcardIndex == -1) {
      return new Exact(path);
    }
    return new Wildcard(path.substring(0, wildcardIndex), path.substring(wildcardIndex));
  }

  static Matcher any() {
    return key -> true;
  }

  static Matcher none() {
    return key -> false;
  }

  record Or(Matcher... matchers) implements Matcher {

    @Override
    public boolean test(String key) {
      for (Matcher matcher : matchers) {
        if (matcher.test(key)) {
          return true;
        }
      }
      return false;
    }
  }

  record And(Matcher... matchers) implements Matcher {

    @Override
    public boolean test(String key) {
      for (Matcher matcher : matchers) {
        if (!matcher.test(key)) {
          return false;
        }
      }
      return true;
    }
  }

  record Not(Matcher matcher) implements Matcher {

    @Override
    public boolean test(String key) {
      return !matcher.test(key);
    }
  }

  record Exact(String path) implements Matcher {

    @Override
    public boolean test(String key) {
      return path.equals(key);
    }
  }

  record Wildcard(String prefix, String keyPattern) implements Matcher {

    public boolean test(String key) {
      if (!key.startsWith(prefix)) {
        return false;
      }
      return FilenameUtils.wildcardMatch(key.substring(prefix.length()), keyPattern);
    }
  }
}
