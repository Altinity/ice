/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.internal.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

public final class Config {

  private Config() {}

  public static <T> T load(String configFile, boolean configFileRequired, TypeReference<T> type)
      throws IOException {
    String v = "{}";
    try {
      v = Files.readString(Path.of(configFile));
    } catch (NoSuchFileException e) {
      if (configFileRequired) {
        throw e;
      }
    }
    ObjectMapper om = new ObjectMapper(new YAMLFactory());
    om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    // TODO: support env var interpolation
    try {
      return om.readValue(v, type);
    } catch (UnrecognizedPropertyException e) {
      throw new InvalidConfigException(e.getMessage());
    }
  }
}
